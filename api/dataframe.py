import ujson as json
from flask import request
from flask_restful import Resource
from bson import ObjectId
import uuid

from api.exceptions import JsonRequiredException, DataFrameNotFoundException, BadRequestException
import util as util
from proto.framespace import framespace_pb2 as fs
from google.protobuf import json_format
import pandas as pd

class DataFrame(Resource):
  """
  The DataFrame endpoint is designed to aggregate vectors across multiple keyspaces.
  The API for this v1.0 endpoint is under development, but the proposed solution is 
  a request object like the following:
  
  message BuildDataFrameRequest {
    repeated Dimension new_major = 1;
    repeated Dimension new_minor = 2;
    int32 page_size = 3;
  }

  which returns a single instance of the following dataframe object

  message DataFrame {
    string id = 1;
    Dimension major = 2;
    Dimension minor = 3;
    repeated Unit units = 4;
    map<string, string> metadata = 5;
    map<string, google.protobuf.Struct> contents = 6;
  }
  """

  def __init__(self, db):
    self.db = db

  def get(self):
    """
    GET /dataframe
    """
    pass

  def post(self):
    """
    POST
    Request to build a dataframe based off a combination of
    keyspaces, keys, and units. 
    Speed up by by-passing proto message creation in response
    """
    if request.json is None:
      raise JsonRequiredException()

    return self.buildDataFrame(json.dumps(request.json))


  def buildDataFrame(self, request, transpose=False):

    jreq = util.fromJson(request, fs.BuildDataFrameRequest)
    print json_format._MessageToJsonObject(jreq, True)

    major_keyspaces = [keyset.keyspace_id for keyset in jreq.major]
    minor_keyspaces = [keyset.keyspace_id for keyset in jreq.minor]

    unit_ids = [unit.id for unit in jreq.units]

    filters = {}
    major_keys = []
    minor_keys = []

    # address errors related to missing values
    filters.setdefault('$and',[])
    if len(major_keyspaces) > 0:
      filters['$and'].append({'majks': util.getMongoFieldFilter(major_keyspaces, ObjectId)})
      major_keys = self.setDimensionFilters(jreq.major, [key for key in keyset.keys for keyset in jreq.major])
    
    if len(minor_keyspaces) > 0:
      filters['$and'].append({'minks': util.getMongoFieldFilter(minor_keyspaces, ObjectId)})
      minor_keys = [key for key in keyset.keys for keyset in jreq.minor]
    
    if len(unit_ids) > 0:
      filters['$and'].append({'units': util.getMongoFieldFilter(unit_ids, ObjectId)})
    
    if len(minor_keys) > 0:
      filters['$and'].append({'key': {"$in": minor_keys}})

    print filters

    agg_set = [{"$match": filters}]

    if len(major_keys) > 0:
      agg_set.append({"$project": major_keys})

    agg_set.append({"$group": {"_id": "$key", "contents": {"$push": "$contents"}}})
    
    if jreq.page_size != 0:
      agg_set.append({"$limit": jreq.page_size})

    vectors = self.db.vector.aggregate(agg_set)
    vectors.batch_size(1000000)

    if vectors is None:
      raise ObjectNotFoundException()

    contents = {vector['_id']:mergeDicts(vector['contents']) for vector in vectors}

    # explore options for metadata
    dataframe = {"id": str(uuid.uuid4()), 
                 "contents": contents, 
                 "metadata": {"keyspaces": major_keyspaces + minor_keyspaces, 'units': unit_ids}}

    return util.buildResponse(dataframe)

  def buildDimensionFilters(self, keys):
    """
    Function that sets filters on desired keys
    """
    kmaj_keys = {}
    if len(keys) > 0:
      kmaj_keys = {"contents."+str(k):1 for k in keys}
      kmaj_keys['key'] = 1
    return kmaj_keys

  def setDimensionFilters(self, dimension_set, keys):
    """
    Handles the case where one dimension keys list is null, meaning return all keys
    and another dimensions keys list has items in it
    """
    major_keys = {}
    if len(keys) > 0:
      major_keys = self.buildDimensionFilters(keys)
      for dim in dimension_set:
        if len(dim.keys) == 0 and len(major_keys) > 0:
          ks = self.db.keyspace.find_one({'_id': ObjectId(dim.keyspace_id)})
          major_keys.update(self.buildDimensionFilters(ks['keys']))
    return major_keys


def mergeDicts(diction):
  t = diction[0]
  for i in diction[1:]:
    t.update(i)
  return t


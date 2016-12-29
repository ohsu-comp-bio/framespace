import ujson as json
from flask import request
from flask_restful import Resource
from bson import ObjectId

from api.exceptions import JsonRequiredException, DataFrameNotFoundException, BadRequestException
import util as util
from proto.framespace import framespace_pb2 as fs
from google.protobuf import json_format
import pandas as pd
import uuid

class Aggregate(Resource):
  """
  """

  def __init__(self, db):
    self.db = db

  def get(self):
    """
    GET /dataframe/<dataframe_id>
    """
    pass

  def post(self):
    """
    POST {"dataframeId": ID}
    Returns a dataframe or a subset of a dataframe. 
    Unsupported: Transpose via passing dimensions. 
    Speed up by by-passing proto message creation in response
    """
    if request.json is None:
      raise JsonRequiredException()

    return self.buildDataFrame(json.dumps(request.json))


  def buildDataFrame(self, request, transpose=False):

    jreq = util.fromJson(request, fs.SliceDataFrameRequest)
    print json_format._MessageToJsonObject(jreq, True)

    major_keyspaces = [x.keyspace_id for x in jreq.new_major]
    major_keys = self.setDimensionFilters(jreq.new_major, [y for y in x.keys for x in jreq.new_major])

    minor_keyspaces = [x.keyspace_id for x in jreq.new_minor]
    minor_keys = [y for y in x.keys for x in jreq.new_minor]

    filters = {}
    filters.setdefault('$and',[]).append({'majks': util.getMongoFieldFilter([x.keyspace_id for x in jreq.new_major], ObjectId)})
    filters['$and'].append({'minks': util.getMongoFieldFilter([x.keyspace_id for x in jreq.new_minor], ObjectId)})
    
    if len(minor_keys) > 0:
      filters['$and'].append({'key': {"$in": minor_keys}})

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
    dataframe = {"id": str(uuid.uuid4()), "contents": contents}

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

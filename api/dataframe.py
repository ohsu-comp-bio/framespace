import ujson as json
from flask import request
from flask_restful import Resource
from bson import ObjectId

from api.exceptions import JsonRequiredException, DataFrameNotFoundException, BadRequestException
import util as util
from proto.framespace import framespace_pb2 as fs
from google.protobuf import json_format
import pandas as pd

class DataFrame(Resource):
  """
  API Resource that describes a dataframe slice.

  message SliceDataFrameRequest {
    string dataframe_id = 1;
    Dimension new_major = 2;
    Dimension new_minor = 3;
    int32 page_start = 4;
    int32 page_end = 5;
  }

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

  def get(self, dataframe_id):
    """
    GET /dataframe/<dataframe_id>
    """
    transpose = bool(request.args.get('transpose', False))
    request_args = self.translateGetArgs(request, dataframe_id)
    return self.sliceDataFrame(json.dumps(request_args), transpose=transpose)


  def post(self):
    """
    POST {"dataframeId": ID}
    Returns a dataframe or a subset of a dataframe. 
    Unsupported: Transpose via passing dimensions. 
    Speed up by by-passing proto message creation in response
    """
    if request.json is None:
      raise JsonRequiredException()

    return self.sliceDataFrame(json.dumps(request.json))


  def sliceDataFrame(self, request, transpose=False):

    # inits
    vec_filters = {}

    # validate request
    jreq = util.fromJson(request, fs.SliceDataFrameRequest)

    if not jreq.dataframe_id:
      raise BadRequestException("dataframeId is required for sliceDataFrame")

    # first request to get dataframe
    result = self.db.dataframe.find_one({"_id": ObjectId(str(jreq.dataframe_id))})

    if result is None:
      raise DataFrameNotFoundException(jreq.dataframe_id)

    # prep vector query
    vc = result.get('contents', [])
    
    # save page end for later check
    page_end = int(jreq.page_end)
    # if page start is outside of dataframe length, return empty
    if jreq.page_start > len(vc):
      dataframe = {"id": str(result["_id"]), \
                 "major": {"keyspaceId": str(result['major']), "keys": []}, \
                 "minor": {"keyspaceId": str(result['minor']), "keys": []}, \
                 "contents": []}
      return util.buildResponse(dataframe)
     
    elif jreq.page_end > len(vc) or len(jreq.new_minor.keys) > 0 or jreq.page_end == 0:
      jreq.page_end = len(vc)

    # construct vector filters
    vec_filters["_id"] = {"$in": vc[jreq.page_start:jreq.page_end]}

    if not transpose:
      kmaj_keys = self.setDimensionFilters(jreq.new_major.keys, jreq.new_minor.keys, vec_filters)
    else:
      kmaj_keys = self.setDimensionFilters(jreq.new_minor.keys, jreq.new_major.keys, vec_filters)

    # seconrd query to backend to get contents
    vectors = self.db.vector.find(vec_filters, kmaj_keys)
    vectors.batch_size(1000000)
    # construct response

    contents = {vector["key"]:vector["contents"] for vector in vectors}
    if transpose:
      # this is the overhead
      d = pd.DataFrame.from_dict(contents, orient="index")
      contents = d.to_dict()

    # avoid invalid keys passing through to keys
    # explore impacts on response time
    kmaj_keys = []
    if len(jreq.new_major.keys) > 0:
      kmaj_keys = contents[contents.keys()[0]].keys()
    # return keys in dimension, 
    # if the whole dimension is not returned
    kmin_keys = []
    # if len(jreq.new_minor.keys) > 0 or page_end < len(vc):
    if len(jreq.new_minor.keys) > 0 or page_end == 0:
      kmin_keys = contents.keys()

    dataframe = {"id": str(result["_id"]), \
                 "major": {"keyspaceId": str(result['major']), "keys": kmaj_keys}, \
                 "minor": {"keyspaceId": str(result['minor']), "keys": kmin_keys}, \
                 "contents": contents}

    return util.buildResponse(dataframe)
 

  def setDimensionFilters(self, major_keys, minor_keys, vec_filters):
    kmaj_keys = None
    if len(major_keys) > 0:
      kmaj_keys = {"contents."+str(k):1 for k in major_keys}
      kmaj_keys['key'] = 1

    if len(minor_keys) > 0:
      vec_filters['key'] = {"$in": map(str, minor_keys)}

    return kmaj_keys

  def translateGetArgs(self, request, dataframe_id):
    """
    Handles json building for get args, for json <-> pb support
    """
    d = {'dataframeId': dataframe_id}
    for arg in request.args:
      if arg[:4] == 'page':
        d[str(arg)] = int(request.args[arg][0])
      if arg[:3] == 'new':
        d[str(arg)] = {'keys': request.args[arg].split(',')}
    return d


class Transpose(DataFrame):
  """
  Class for Translating DataFrame Resource
  """

  def __init__(self, db):
    self.db = db

  def get(self, dataframe_id):
    request_args = self.translateGetArgs(request, dataframe_id)
    return self.sliceDataFrame(json.dumps(request_args), transpose=True)

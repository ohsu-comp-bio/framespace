from flask import request, jsonify
from flask_restful import Resource
import json
from bson import ObjectId

import util as util
from proto.framespace import framespace_pb2 as fs
from google.protobuf import json_format

class DataFrames(Resource):
  """
  API Resource that describes a single keyspace.

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
    GET /dataframes
    """
    return self.searchDataFrames(json.dumps(dict(request.args)), from_get=True)


  def post(self):
    """
    POST {"keyspaceIds": [ID1, ID2]}
    Search for dataframes in FrameSpace registered to a given keyspace. 
    Contents omitted in return for clarity.
    """
    if not request.json:
      return "Bad content type, must be application/json\n"

    check_ks = request.json.get('keyspaceIds', None)
    if check_ks is None or (len(check_ks) == 1 and check_ks[0] == unicode('mask-keys')):
      return 'keyspaceIds required when searching dataframes.\n'

    return self.searchDataFrames(json.dumps(request.json))

  def searchDataFrames(self, request, from_get=False):
    """
    DataFrame Search Function
    """
    try:

      # get proto, validates
      jreq = util.fromJson(request, fs.SearchDataFramesRequest)
      print json_format._MessageToJsonObject(jreq, True)

      # handle masks, ommitting contents from endpoint
      mask_contents = {"contents": 0}
      mask_keys = util.setMask(jreq.keyspace_ids, unicode('mask-keys'), 'keys')

      # handle filters
      filters = {}
      if len(jreq.dataframe_ids) > 0:
        filters['_id'] = util.getMongoFieldFilter(jreq.dataframe_ids, ObjectId, from_get=from_get)

      if len(jreq.keyspace_ids) > 0:
        filters['$or'] = [{'major': util.getMongoFieldFilter(jreq.keyspace_ids, ObjectId, from_get=from_get)}]
        filters['$or'].append({'minor': util.getMongoFieldFilter(jreq.keyspace_ids, ObjectId, from_get=from_get)})

      # query backend
      if len(filters) > 0:
        result = self.db.dataframe.find(filters, mask_contents)
      else:
        result = self.db.dataframe.find({}, mask_contents)

      # make proto
      _protoresp = fs.SearchDataFramesResponse(dataframes=[])
      for r in result:
        kmaj_name, kmaj_keys = util.getKeySpaceInfo(self.db, r['major'], mask_keys)
        kmin_name, kmin_keys = util.getKeySpaceInfo(self.db, r['minor'], mask_keys)

        dataframe = fs.DataFrame(id=str(r['_id']), \
          major=fs.Dimension(keyspace_id=str(r['major']), keys=kmaj_keys), \
          minor=fs.Dimension(keyspace_id=str(r['minor']), keys=kmin_keys), \
          units=[], contents=[])

        for unit in r['units']:
          _unit = self.db.units.find_one({'_id':unit})
          dataframe.units.extend([fs.Unit(name=_unit['name'], \
                                          description=_unit['description'], \
                                          id=str(_unit['_id']))])

        _protoresp.dataframes.extend([dataframe])

      return util.toFlaskJson(_protoresp)

    except Exception as e:
      return str(e)
from flask import request, jsonify
from flask_restful import Resource, abort
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
    return self.searchDataFrames(dict(request.args), from_get=True)


  def post(self):
    """
    POST {"keyspaceIds": [ID1, ID2]}
    Search for dataframes in FrameSpace registered to a given keyspace. 
    Contents omitted in return for clarity.
    """
    return self.searchDataFrames(request.json)

  def searchDataFrames(self, request, from_get=False):
    """
    DataFrame Search Function
    """
    # enforce project level access control by enforcing restrictions on keyspaces
    check_ks = request.get('keyspaceIds', None)
    if check_ks is None or (len(check_ks) == 1 and check_ks[0] == unicode('mask-keys')):
      return jsonify({500: 'keyspaceIds required when searching dataframes'})

    try:

      # get proto, validates
      jreq = util.fromJson(json.dumps(request), fs.SearchDataFramesRequest)

      # handle masks, ommitting contents from endpoint
      mask_contents = {"contents": 0}
      mask_keys = None

      # handle filters
      filters = {}
      if len(jreq.dataframe_ids) > 0:
        filters['_id'] = util.getMongoFieldFilter(jreq.dataframe_ids, ObjectId, from_get=from_get)

      if len(jreq.keyspace_ids) > 0:
        # grab keys mask
        keyspace_ids = jreq.keyspace_ids
        if from_get:
          keyspace_ids = jreq.keyspace_ids[0].split(',')

        mask_keys = util.setMask(keyspace_ids, unicode('mask-keys'), 'keys')

        # process keyspace ids
        if len(keyspace_ids) > 0:
          filters['$or'] = [{'major': util.getMongoFieldFilter(keyspace_ids, ObjectId, from_get=from_get)}]
          filters['$or'].append({'minor': util.getMongoFieldFilter(keyspace_ids, ObjectId, from_get=from_get)})

      # query backend
      result = self.db.dataframe.find(filters, mask_contents)
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
      return jsonify({500: str(e)})
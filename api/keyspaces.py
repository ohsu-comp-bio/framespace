from flask import request, jsonify
from flask_restful import Resource
import json
from bson import ObjectId

import util as util
from proto.framespace import framespace_pb2 as fs

# /keyspaces/<axis_name>/<keyspace_name>

class KeySpace(Resource):
  """
  API Resource that describes a single keyspace.

  message KeySpace {
    string id = 1;
    string name = 2;
    string axis_name = 3;
    repeated string keys = 4;
    map<string, string> metadata = 5;
  }
  """

  def __init__(self, db):
    self.db = db

  def get(self, axis_name, name=None):
      """
      GET /keyspaces/<axis_name>/<name>
      """
      try:

        # alter this, keyspace name isn't guaranteed unique
        result = self.db.keyspace.find_one({"axis_name": axis_name, "name": name})

        if result:
          _keyspace = fs.KeySpace(name=result['name'], \
            axis_name=result['axis_name'], \
            id=str(result['_id']), \
            keys=result['keys'])

          return util.toFlaskJson(_keyspace)
        else:
          return jsonify({})

      except Exception as e:
        return str(e)


class KeySpaces(Resource):
  """
  API Resource that describes keyspace >= 1.

  message KeySpace {
    string id = 1;
    string name = 2;
    string axis_name = 3;
    repeated string keys = 4;
    map<string, string> metadata = 5;
  }
  """

  def __init__(self, db):
    self.db = db


  def get(self):
    """
    GET /keyspaces?axisNames=...&keySpaceIds=...&names=...&keys=...
    """

    try:

      # consolidate filters
      filters = {}
      mask = None

      if request.args.get('axisNames', None):
        filters['axis_name'] = util.getMongoFieldFilter(request.args.get('axisNames').split(','), str)

      if request.args.get('names', None):
        filters['name'] = util.getMongoFieldFilter(request.args.get('names').split(','), str)
      
      if request.args.get('keySpaceIds', None):
        filters['_id'] = util.getMongoFieldFilter(request.args.get('keySpaceIds').split(','), ObjectId)
      
      # add check here for case where name and id are both $or

      if request.args.get('keys', None):
        keys = request.args.get('keys').split(',')
        mask = util.setMask(keys, unicode('mask'), "keys")
        if len(keys) > 0:
          filters['keys'] = util.getMongoFieldFilter(keys, str)

      print filters
      print mask
      print keys
      result = self.db.keyspace.find(filters, mask)
      print result.count()

      # make proto
      _protoresp = fs.SearchKeySpacesResponse()
      for r in result:
        if r.get('keys', None) is None:
          r['keys'] = []
        _protoresp.keyspaces.add(name=r['name'], axis_name=r['axis_name'], id=str(r['_id']), keys=r['keys'])

      return util.toFlaskJson(_protoresp)

    except Exception as e:
      return "".join([str(e), "\n"])

  def post(self):
    """
    POST /keyspaces/search
    {"names": ["name1"], "axis_names": ["axis_name1"], "keyspace_ids": ["57118a40b5262889d1de9c94"], "keys": ["key1"] }
    """
    # validate request
    if not request.json:
      return "Bad content type, must be application/json."

    try:

      # get proto, validates
      jreq = util.fromJson(json.dumps(request.json), fs.SearchKeySpacesRequest)

      # handle masks
      mask = util.setMask(jreq.keys, unicode('mask'), "keys")

      # consolidate filters
      filters = {}

      if len(jreq.axis_names) > 0:
        filters['axis_name'] = util.getMongoFieldFilter(jreq.axis_names, str)
      else:
        return 'Axis name required for keyspace search.'

      if len(jreq.names) > 0:
        filters['name'] = util.getMongoFieldFilter(jreq.names, str)
      
      if len(jreq.keyspace_ids) > 0:
        filters['_id'] = util.getMongoFieldFilter(jreq.keyspace_ids, ObjectId)
      
      if len(jreq.keys) > 0:
        filters['keys'] = util.getMongoFieldFilter(jreq.keys, str)

      result = self.db.keyspace.find(filters, mask)

      # make proto
      _protoresp = fs.SearchKeySpacesResponse()
      for r in result:
        if r.get('keys', None) is None:
          r['keys'] = []
        _protoresp.keyspaces.add(name=r['name'], axis_name=r['axis_name'], id=str(r['_id']), keys=r['keys'])

      return util.toFlaskJson(_protoresp)

    except Exception as e:
      return "".join([str(e), "\n"])
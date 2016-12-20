from flask import request, jsonify
from flask_restful import Resource
import json
from bson import ObjectId

from api.exceptions import JsonRequiredException, KeySpaceNotFoundException
import util
from proto.framespace import framespace_pb2 as fs


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

  def get(self, keyspace_id):
      """
      GET /keyspaces/<keyspace_id>
      """
      mask = None
      if bool(request.args.get('mask', False)):
        mask = {'keys': 0}

      result = self.db.keyspace.find_one({"_id": ObjectId(keyspace_id)}, mask)

      if result:
        _keyspace = fs.KeySpace(name=result['name'], \
          axis_name=result['axis_name'], \
          id=str(result['_id']))
        if mask is None:
          _keyspace.keys.extend(result['keys'])

        return util.toFlaskJson(_keyspace)
      else:
        raise KeySpaceNotFoundException(keyspace_id)


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
    # handle keys args
    return self.searchKeySpaces(dict(request.args))

  def post(self):
    """
    POST /keyspaces/search
    {"names": ["name1"], "axis_names": ["axis_name1"], "keyspace_ids": ["57118a40b5262889d1de9c94"], "keys": ["key1"] }
    """

    # Request must have content type of application/json
    if request.json is None:
      raise JsonRequiredException()

    return self.searchKeySpaces(request.json)


  def searchKeySpaces(self, request):

    # get proto, validates
    jreq = util.fromJson(json.dumps(request), fs.SearchKeySpacesRequest)

    # handle masks
    mask = util.setMask(jreq.keys, unicode('mask'), "keys")

    # consolidate filters
    filters = {}

    if len(jreq.axis_names) > 0:
      filters['axis_name'] = util.getMongoFieldFilter(jreq.axis_names, str)

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

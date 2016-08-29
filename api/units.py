from flask import request, jsonify
from flask_restful import Resource
import json
from bson import ObjectId

import util as util
from proto.framespace import framespace_pb2 as fs

class Units(Resource):
  """
  API Resource that describes a unit.

  message Unit {
    string id = 1;
    string name = 2;
    string description = 3;
  } 
  """

  def __init__(self, db):
    self.db = db


  def get(self):
    """
    GET /units?id={id}&name={name}
    """
    filt = {}
    for arg in request.args:
      value = request.args.get(arg, None)
      if value:
        # handle args for _id field
        k, v = util.handle_arg(arg, value)
        filt[k] = v

    try:
      result = self.db.units.find_one(filt)
      # make proto
      if result:
        _unit = fs.Unit(id=str(result['_id']), name=result['name'], description=result['description'])
        return util.toFlaskJson(_unit)
      else:
        return jsonify({})

    except Exception as e:
      return "".join([str(e), "\n"])


  def post(self):
    """
    POST /units/search
    { "names": ["gene-exp"] }
    """

    # validate request
    req = util.getRequest(request)
    
    try:

      # get proto, validates
      jreq = util.fromJson(json.dumps(req), fs.SearchUnitsRequest)

      filters = {}
      # query backend
      if len(jreq.names) > 0:
        filters['name'] = util.getMongoFieldFilter(jreq.names, str)

      if len(jreq.ids) > 0:
        filters['_id'] = util.getMongoFieldFilter(jreq.ids, ObjectId)

      if len(filters) > 0:
        result = self.db.units.find(filters)
      else:
        result = self.db.units.find()

      # make proto
      _protoresp = fs.SearchUnitsResponse()
      for r in result:
        _protoresp.units.add(name=r['name'], description=r['description'], id=str(r['_id']))

      return util.toFlaskJson(_protoresp)

    except Exception as e:
      return "".join([str(e), "\n"])
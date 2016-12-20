from flask import request, jsonify
from flask_restful import Resource
import json
from bson import ObjectId

import util as util
from proto.framespace import framespace_pb2 as fs
from api.exceptions import UnitNotFoundException

class Unit(Resource):
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


  def get(self, name):
    """
    GET /units/name
    """

    result = self.db.units.find_one({"name": str(name)})
    # make proto
    if result:
      _unit = fs.Unit(id=str(result['_id']), name=result['name'], description=result['description'])
    else:
      raise UnitNotFoundException(name)

    return util.toFlaskJson(_unit)


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

    return self.searchUnits(dict(request.args), from_get=True)


  def post(self):
    """
    POST /units/search
    { "names": ["gene-exp"] }
    """

    # validate request
    req = util.getRequest(request)
    return self.searchUnits(req)
    

  def searchUnits(self, request, from_get=False):
    # get proto, validates
    jreq = util.fromJson(json.dumps(request), fs.SearchUnitsRequest)

    filters = {}
    # query backend
    if len(jreq.names) > 0:
      filters['name'] = util.getMongoFieldFilter(jreq.names, str, from_get=from_get)

    if len(jreq.ids) > 0:
      filters['_id'] = util.getMongoFieldFilter(jreq.ids, ObjectId, from_get=from_get)

    if len(filters) > 0:
      result = self.db.units.find(filters)
    else:
      result = self.db.units.find()

    # make proto
    _protoresp = fs.SearchUnitsResponse()
    for r in result:
      _protoresp.units.add(name=r['name'], description=r['description'], id=str(r['_id']))

    return util.toFlaskJson(_protoresp)

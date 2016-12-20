from flask import request, jsonify
from flask_restful import Resource, abort
import json
from bson import ObjectId

import util as util
from proto.framespace import framespace_pb2 as fs
from api.exceptions import AxisNotFoundException

class Axis(Resource):
  """
  API Resource that describes a single Axis.

  message Axis {
    string name = 1;
    string description = 2;
  } 
  """

  def __init__(self, db):
    self.db = db

  def get(self, name):
    """
    GET /axes/name
    Return axis with a given name
    """
    result = self.db.axis.find_one({"name": str(name)})
    if result is not None:
      _axis = fs.Axis()
      _axis = fs.Axis(name=result['name'], description=result['description'])
    else:
      raise AxisNotFoundException(name)

    return util.toFlaskJson(_axis)


class Axes(Resource):
  """
  API Resource that describes Axis >= 1.

  message Axis {
    string name = 1;
    string description = 2;
  } 
  """

  def __init__(self, db):
    self.db = db


  def get(self):
    """
    GET /axes
    Return all axes
    """

    return self.axesSearch(dict(request.args), from_get=True)

  def post(self):
    """
    POST /axes/search
    { "names" : ["gene"] }
    """

    # validate request
    req = util.getRequest(request)
    return self.axesSearch(req)

  def axesSearch(self, request, from_get=False):
    # get proto, validates
    jreq = util.fromJson(json.dumps(request), fs.SearchAxesRequest)

    # query backend
    if len(jreq.names) > 0:
      result = self.db.axis.find({"name": util.getMongoFieldFilter(jreq.names, str, from_get=from_get)})
    else:
      result = self.db.axis.find()

    # make proto
    _protoresp = fs.SearchAxesResponse()
    for r in result:
      _protoresp.axes.add(name=r['name'], description=r['description'])

    return util.toFlaskJson(_protoresp)

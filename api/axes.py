from flask import request, jsonify
from flask_restful import Resource, abort
import json
from bson import ObjectId

import util as util
from proto.framespace import framespace_pb2 as fs
# from util.ccc_auth import validateRulesEngine
from functools import wraps

def validate(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
      if not util.ccc_auth.validateRulesEngine(request):
        return abort(401)
      return func(*args, **kwargs)
    return wrapper

class Axis(Resource):
  """
  API Resource that describes a single Axis.

  message Axis {
    string name = 1;
    string description = 2;
  } 
  """
  # decorators = [checkuserfun('test')]

  def __init__(self, db):
    self.db = db

  @validate
  def get(self, name):
    """
    GET /axes/name
    Return axis with a given name
    """
    try:
      result = self.db.axis.find_one({"name": str(name)})

      _axis = fs.Axis()
      _axis = fs.Axis(name=result['name'], description=result['description'])
        
      return util.toFlaskJson(_axis)

    except Exception as e:
      return "".join([str(e), "\n"])


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
    try:
      if request.args.get('names', None):
        result = self.db.axis.find({"name": util.getMongoFieldFilter(request.args.get('names').split(','), str)})
      else:
        result = self.db.axis.find()

      # make proto
      _protoresp = fs.SearchAxesResponse()
      for r in result:
        _protoresp.axes.add(name=r['name'], description=r['description'])

      return util.toFlaskJson(_protoresp)

    except Exception as e:
      return "".join([str(e), "\n"])

  def post(self):
    """
    POST /axes/search
    { "names" : ["gene"] }
    """

    # validate request
    req = util.getRequest(request)

    try:
      # get proto, validates
      jreq = util.fromJson(json.dumps(req), fs.SearchAxesRequest)

      # query backend
      if len(jreq.names) > 0:
        result = self.db.axis.find({"name": util.getMongoFieldFilter(jreq.names, str)})
      else:
        result = self.db.axis.find()

      # make proto
      _protoresp = fs.SearchAxesResponse()
      for r in result:
        _protoresp.axes.add(name=r['name'], description=r['description'])

      return util.toFlaskJson(_protoresp)

    except Exception as e:
      return "".join([str(e), "\n"])

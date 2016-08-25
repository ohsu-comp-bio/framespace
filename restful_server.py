from flask import Flask, request, jsonify
from flask_restful import Resource, Api
import os, json
import util as util

from pymongo import MongoClient
from bson import ObjectId

from proto.framespace import framespace_pb2 as fs

# app = Flask(__name__)
# name passed to flask app will bind to db,
app = Flask('framespace')
api = Api(app)
# if not docker, then run locally
try:
  mongo = MongoClient(os.environ['DB_PORT_27017_TCP_ADDR'], 27017)
except:
  mongo = MongoClient()

db = mongo['framespace']

def handle_arg(key, value):
  # handle ids
  if key == 'id':
    key = '_'+key
    return key, ObjectId(value)
  else:
    return key, value

class Units(Resource):
  def get(self):
    filt = {}
    for arg in request.args:
      value = request.args.get(arg, None)
      if value:
        k, v = handle_arg(arg, value)
        filt[k] = v

    try:
      result = db.units.find_one(filt)
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
        result = db.units.find(filters)
      else:
        result = db.units.find()

      # make proto
      _protoresp = fs.SearchUnitsResponse()
      for r in result:
        _protoresp.units.add(name=r['name'], description=r['description'], id=str(r['_id']))

      return util.toFlaskJson(_protoresp)

    except Exception as e:
      return "".join([str(e), "\n"])

api.add_resource(Units, '/units')
api.add_resource(Units, '/units/search')

if __name__ == '__main__':
    app.run(debug=True)
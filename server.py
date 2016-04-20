from flask import Flask, request, jsonify
from flask.ext.pymongo import PyMongo
from bson import ObjectId
import json

from proto.framespace import framespace_pb2 as models
from proto.framespace import framespace_service_pb2 as services

import google.protobuf.json_format as json_format

# name passed to flask app will bind to db
app = Flask('framespace')
# connects to the mongodb server running on port 27017
mongo = PyMongo(app)

@app.route('/v1/frame/test')


@app.route('/v1/frame/axes/search', methods = ['POST'])
def searchAxes():
  """
  POST /axes/search
  { "names" : ["gene"] }
  """

  # validate request
  if not request.json:
    return "Bad content type, must be application/json\n"

  if request.json.get('names', None) is None:
    return "Names field required for searching axes\n"

  try:

    # get proto, validates
    jreq = fromJson(json.dumps(request.json), services.SearchAxesRequest)

    # query backend
    names = map(str, jreq.names)
    if len(names) > 0:
      result = mongo.db.axis.find({"name": {"$in": names}})
    else:
      result = mongo.db.axis.find()

    # make proto
    _protoresp = services.SearchAxesResponse()
    for r in result:
      _protoresp.axes.add(name=r['name'], description=r['description'])

    # jsonify proto to send
    return toFlaskJson(_protoresp)

  except Exception:
    return "Invalid SearchAxesRequest\n"


@app.route('/v1/frame/units/search', methods = ['POST'])
def searchUnits():
  """
  POST /units/search
  { "names": ["gene-exp"] }
  """

    # validate request
  if not request.json:
    return "Bad content type, must be application/json\n"

  if request.json.get('names', None) is None:
    return "Names field required for searching units\n"

  try:

    # get proto, validates
    jreq = fromJson(json.dumps(request.json), services.SearchUnitsRequest)

    # query backend
    names = map(str, jreq.names)
    if len(names) > 0:
      result = mongo.db.units.find({"name": {"$in": names}})
    else:
      result = mongo.db.units.find()

    # make proto
    _protoresp = services.SearchUnitsResponse()
    for r in result:
      _protoresp.units.add(name=r['name'], description=r['description'])

    # jsonify proto to send
    return toFlaskJson(_protoresp)

  except Exception:
    return "Invalid SearchUnitsRequest\n"


@app.route('/v1/frame/keyspaces/search', methods = ['POST'])
def searchKeySpaces():
  """
  POST /keyspaces/search
  {"names": ["name1"], "axis_names": ["axis_name1"], "keyspace_ids": ["57118a40b5262889d1de9c94"], "keys": ["key1"] }
  """
  # validate request
  if not request.json:
    return "Bad content type, must be application/json\n"

  # add required check

  try:

    print 'getting request', request.json

    # get proto, validates
    jreq = fromJson(json.dumps(request.json), services.SearchKeySpacesRequest)

    # consolidate filters
    filters = {}
    mask = None
    if len(jreq.names) > 0:
      filters['name'] = getMongoFieldFilter(jreq.names, str)
    
    if len(jreq.keyspace_ids) > 0:
      filters['_id'] = getMongoFieldFilter(jreq.keyspace_ids, ObjectId)

    if len(jreq.axis_names) > 0:
      filters['axis_name'] = getMongoFieldFilter(jreq.axis_names, str)
    
    # explore here key return options
    # asterix, return all
    # keywords to return some
    if len(jreq.keys) > 0:
      # mask keyword omits keys from being returned
      if jreq.keys[0] == unicode('mask'):
        mask = {'keys': 0}
      else:
        filters['keys'] = getMongoFieldFilter(jreq.keys, str)

    if len(filters) > 0:
      result = mongo.db.keyspace.find(filters, mask)
    else:
      result = mongo.db.axis.find()

    print filters
    print mask
    # make proto
    _protoresp = services.SearchKeySpacesResponse()
    for r in result:
      if r.get('keys', None) is None:
        r['keys'] = []
      _protoresp.keyspaces.add(name=r['name'], axis_name=r['axis_name'], id=str(r['_id']), keys=r['keys'])

    # jsonify proto to send
    return toFlaskJson(_protoresp)

  except Exception:
    return "Invalid SearchKeySpacesRequest\n"


@app.route('/v1/frame/dataframes/search')
def searchDataFrames():
  dataframes = mongo.db.dataframe.find({}, {"contents":0})
  return str(list(dataframes))


# //TODO
@app.route('/v1/frame/dataframe/slice')
def sliceDataFrame():
  return 'Not Implemented.'


### helpers
def toFlaskJson(protoObject):
    """
    Serialises a protobuf object as a flask Response object
    """
    js = json_format._MessageToJsonObject(protoObject, True)
    return jsonify(js)

def fromJson(json, protoClass):
    """
    Deserialise json into an instance of protobuf class
    """
    return json_format.Parse(json, protoClass())

def getMongoFieldFilter(filterList, maptype):
  return {"$in": map(maptype, filterList)}


if __name__ == '__main__':
    app.run(debug=True)
from flask import Flask, request, jsonify
from flask.ext.pymongo import PyMongo
import json

from proto.framespace import framespace_pb2 as models
from proto.framespace import framespace_service_pb2 as services

import google.protobuf.json_format as json_format

# name passed to flask app will bind to db
app = Flask('framespace')
# connects to the mongodb server running on port 27017
mongo = PyMongo(app)

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

    # get proto
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


@app.route('/v1/frame/units/search')
def searchUnits():
  units = mongo.db.units.find()
  return str(list(units))


@app.route('/v1/frame/keyspaces/search')
def searchKeySpaces():
  keyspaces = mongo.db.units.find({}, {"keys": 0})
  return str(list(keyspaces))


@app.route('/v1/frame/dataframes/search')
def searchDataFrames():
  dataframes = mongo.db.dataframe.find({}, {"contents":0})
  return str(list(dataframes))


# //TODO
@app.route('/v1/frame/dataframe/slice')
def sliceDataFrame():
  return 'Not Implemented.'


### helpers
def toFlaskJson(protoObject, indent=None):
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

if __name__ == '__main__':
    app.run(debug=True)
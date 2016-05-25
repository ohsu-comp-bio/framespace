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
    else:
      # just experimenting with required fields
      return "An axis name is required for keyspace search.\n"
    
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
      result = mongo.db.keyspace.find()

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


@app.route('/v1/frame/dataframes/search', methods=['POST'])
def searchDataFrames():

  if not request.json:
    return "Bad content type, must be application/json\n"

  try:

    # get proto, validates
    jreq = fromJson(json.dumps(request.json), services.SearchDataFramesRequest)

    # handle masks
    mask_contents = setMask(jreq.dataframe_ids, unicode('mask-contents'), 'contents')
    mask_keys = setMask(jreq.keyspace_ids, unicode('mask-keys'), 'keys')

    # handle filters
    filters = {}
    if len(jreq.dataframe_ids) > 0:
      filters['_id'] = getMongoFieldFilter(filt_dataframes, ObjectId)

    if len(jreq.keyspace_ids) > 0:
      filters['major'] = getMongoFieldFilter(filt_keyspaces, ObjectId)
      filters['minor'] = getMongoFieldFilter(filt_keyspaces, ObjectId)

    # if len(jreq.units) > 0:
    #   filters['units'] = getMongoFieldFilter(jreq.units, )

    if len(filters) > 0:
      # return str(filters)
      result = mongo.db.dataframe.find(filters, mask_contents)
    else:
      result = mongo.db.dataframe.find({}, mask_contents)

    # make proto
    _protoresp = services.SearchDataFramesResponse()
    for r in result:

      kmaj_name, kmaj_keys = getKeySpaceInfo(r['major'], mask_keys)
      kmin_name, kmin_keys = getKeySpaceInfo(r['minor'], mask_keys)

      contents = buildVectors(r['contents'], kmin_name)
      print len(contents)

      _protoresp.dataframes.add(id=str(r['_id']), \
        major=models.Dimension(keySpaceId=str(r['major']), keys=kmaj_keys), \
        minor=models.Dimension(keySpaceId=str(r['minor']), keys=kmin_keys), \
        # contents=buildVectors(r['contents'], )
        units=r['units'])

    # jsonify proto to send
    return toFlaskJson(_protoresp)

  except Exception:
    return "Invalid SearchDataFramesRequest\n"

def buildVectors(vector_ids, key_name):
  # get data
  conts = []

  for vector_id in vector_ids:
    vector = mongo.db.vector.find_one({"_id": ObjectId(vector_id)})
    main_key = vector.pop(key_name)

    protovec = models.Vector()
    protovec.key = main_key
    for k,v in vector.items():
      protovec.contents.add(models.KeyValue(key=str(k), float_data=float(v)))
    conts.append(protovec)

  return conts


def setMask(request_list, identifier, mask):
  if identifier in request_list:
    request_list.remove(identifier)
    return {mask: 0}
  return None

def getKeySpaceInfo(keyspace_id, mask):
  print 'calling this'
  keyspace = mongo.db.keyspace.find_one({"_id": ObjectId(keyspace_id)}, mask)
  return keyspace['name'], keyspace.get('keys', None)

# //TODO
@app.route('/v1/frame/dataframe/slice')
def sliceDataFrame():
  return 'Not Implemented.'

def nullifyToken(json):
  if json.get('nextPageToken', None) != None:
    json['nextPageToken'] = None
  return json

### helpers
def toFlaskJson(protoObject):
    """
    Serialises a protobuf object as a flask Response object
    """
    js = json_format._MessageToJsonObject(protoObject, True)
    return jsonify(nullifyToken(js))

def fromJson(json, protoClass):
    """
    Deserialise json into an instance of protobuf class
    """
    return json_format.Parse(json, protoClass())

def getMongoFieldFilter(filterList, maptype):
  return {"$in": map(maptype, filterList)}


if __name__ == '__main__':
    app.run(debug=True)
from flask import Flask, request, jsonify
from flask.ext.pymongo import PyMongo
from bson import ObjectId
import json

from proto.framespace import framespace_pb2 as models
from proto.framespace import framespace_service_pb2 as services

import google.protobuf.json_format as json_format
from google.protobuf import struct_pb2
from google.protobuf import any_pb2
from google.protobuf import wrappers_pb2
from google.protobuf.util import json_format_proto3_pb2


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

    # jsonify proto to sen
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
      filters['_id'] = getMongoFieldFilter(jreq.dataframe_ids, ObjectId)

    if len(jreq.keyspace_ids) > 0:
      filters['major'] = getMongoFieldFilter(jreq.keyspace_ids, ObjectId)
      filters['minor'] = getMongoFieldFilter(jreq.keyspace_ids, ObjectId)

    if len(filters) > 0:
      # return str(filters)
      result = mongo.db.dataframe.find(filters, mask_contents)
    else:
      result = mongo.db.dataframe.find({}, mask_contents)

    # make proto
    _protoresp = services.SearchDataFramesResponse(dataframes=[])
    count = 0
    for r in result:
      kmaj_name, kmaj_keys = getKeySpaceInfo(r['major'], mask_keys)
      kmin_name, kmin_keys = getKeySpaceInfo(r['minor'], mask_keys)

      dataframe = models.DataFrame(id=str(r['_id']), \
        major=models.Dimension(keySpaceId=str(r['major']), keys=kmaj_keys), \
        minor=models.Dimension(keySpaceId=str(r['minor']), keys=kmin_keys), \
        contents=[])

      _protoresp.dataframes.extend([dataframe])

    # jsonify proto to send
    return toFlaskJson(_protoresp)

  except Exception:
    return "Invalid SearchDataFramesRequest\n"

# //TODO
@app.route('/v1/frame/dataframe/slicepb', methods = ['POST'])
def sliceDataFramePb():
  if not request.json:
    return "Bad content type, must be application/json\n"

  try:

    # get proto, validates
    jreq = fromJson(json.dumps(request.json), services.SliceDataFrameRequest)

    # masks for development
    # mask_contents = {"contents": 0}
    mask_contents = None
    mask_keys = {"keys": 0}

    # handle filters
    filters = {}
    if jreq.dataframe_id != '':
      result = mongo.db.dataframe.find_one({"_id": ObjectId(jreq.dataframe_id)}, mask_contents)
    else:
      return "DataFrame ID Required for SliceDataFrameRequest."

    # make proto
    kmaj_name, kmaj_keys = getKeySpaceInfo(result['major'], mask_keys)
    kmin_name, kmin_keys = getKeySpaceInfo(result['minor'], mask_keys)

    _protodf = models.DataFrame(id=str(result['_id']), \
        major=models.Dimension(keySpaceId=str(result['major']), keys=kmaj_keys), \
        minor=models.Dimension(keySpaceId=str(result['minor']), keys=kmin_keys), \
        contents=[])

    if mask_contents is None:
      vc = result.get('contents', [])
      # need to add page size here
      if jreq.page_size > 0:
        ps_vc = vc[:jreq.page_size]
      else:
        ps_vc = vc

      vectors = mongo.db.vector.find({"_id": {"$in": ps_vc}})

      for vector in vectors:
        main_key = vector.pop(kmin_name)
        del vector['_id']

        #response time ~15 seconds map<string, string> contents 
        # _protovec = models.Vector(key=main_key, contents={str(k):str(v) for k,v in vector.items()})

        # better use of protobuf, but still ~1 min google.protobuf.Struct contents
        _protovec = models.Vector(key=main_key)
        for k,v in vector.items():
          _protovec.contents[str(k)] = v
        # print json_format.MessageToJson(_protovec, True)

        _protodf.contents.extend([_protovec])

    # jsonify proto to send
    return toFlaskJson(_protodf)

  except Exception:
    return "Invalid SliceDataFrameRequest\n"

# //TODO
@app.route('/v1/frame/dataframe/slice', methods = ['POST'])
def sliceDataFrame():
  """
  Slice DataFrame is more heavier than the other endpoints, thus there is a HUGE speed up by by-passing the protobuf.
  """
  if not request.json:
    return "Bad content type, must be application/json\n"

  try:

    # masks for development
    # mask_contents = {"contents": 0}
    mask_contents = None
    # mask_keys = {"keys": 0}
    mask_keys = None
    dataframe_id = request.json.get('dataframeId', None)
    page_size = request.json.get('pageSize', 0)

    # handle filters
    filters = {}
    if dataframe_id is not None:
      result = mongo.db.dataframe.find_one({"_id": ObjectId(dataframe_id)}, mask_contents)
    else:
      return "DataFrame ID Required for SliceDataFrameRequest."

    # make proto
    kmaj_name, kmaj_keys = getKeySpaceInfo(result['major'], mask_keys)
    kmin_name, kmin_keys = getKeySpaceInfo(result['minor'], mask_keys)

    dataframe = {'id': str(result['_id']), \
                 'major': {'id': str(result['major']), 'keys': kmaj_keys}, \
                 'minor': {'id': str(result['minor']), 'keys': kmin_keys}, \
                 'contents': []}

    if mask_contents is None:
      vc = result.get('contents', [])
      # get page size
      if page_size > 0:
        ps_vc = vc[:page_size]
      else:
        ps_vc = vc

    vectors = mongo.db.vector.find({"_id": {"$in": ps_vc}})
    dataframe['contents'] = [createContents(vector, kmin_name) for vector in vectors]

    return jsonify(dataframe)

  except Exception:
    return "Invalid SliceDataFrameRequest\n"

def createContents(vector, kmin_name):
  key = vector.pop(kmin_name)
  # del vector[kmin_name]
  del vector['_id']
  return {'key': key, 'contents': vector, 'index':0, 'info':{}}

### helpers
def nullifyToken(json):
  if json.get('nextPageToken', None) != None:
    json['nextPageToken'] = None
  return json

def toFlaskJson(protoObject):
    """
    Serialises a protobuf object as a flask Response object
    """
    js = json_format._MessageToJsonObject(protoObject, True)
    # js = json_format.MessageToJson(protoObject, True)
    # return jsonify(js)
    return jsonify(nullifyToken(js))

def fromJson(json, protoClass):
    """
    Deserialise json into an instance of protobuf class
    """
    return json_format.Parse(json, protoClass())

def getMongoFieldFilter(filterList, maptype):
  return {"$in": map(maptype, filterList)}

def setMask(request_list, identifier, mask):
  if identifier in request_list:
    request_list.remove(identifier)
    return {mask: 0}
  return None

def getKeySpaceInfo(keyspace_id, mask):
  keyspace = mongo.db.keyspace.find_one({"_id": ObjectId(keyspace_id)}, mask)
  return keyspace['name'], keyspace.get('keys', [])

if __name__ == '__main__':
    app.run(debug=True)
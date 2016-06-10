import os, json
from flask import Flask, request, jsonify
from pymongo import MongoClient
from bson import ObjectId

from proto.framespace import framespace_pb2 as models
from proto.framespace import framespace_service_pb2 as services

import google.protobuf.json_format as json_format

# name passed to flask app will bind to db,
# if not docker, then run locally
app = Flask('framespace')
try:
  mongo = MongoClient(os.environ['DB_PORT_27017_TCP_ADDR'], 27017)
except:
  mongo = MongoClient()

db = mongo['framespace']

@app.route('/axes/search', methods = ['POST'])
def searchAxes():
  """
  POST /axes/search
  { "names" : ["gene"] }
  """
  # validate request
  print 'reaching here'
  req = getRequest(request)
  print req

  try:
    print db.collections
    # get proto, validates
    jreq = fromJson(json.dumps(req), services.SearchAxesRequest)
    print json_format.MessageToJson(jreq, True)

    # query backend
    if len(jreq.names) > 0:
      result = db.axis.find({"name": getMongoFieldFilter(jreq.names, str)})
    else:
      result = db.axis.find()

    # make proto
    _protoresp = services.SearchAxesResponse()
    for r in result:
      _protoresp.axes.add(name=r['name'], description=r['description'])

    # jsonify proto to sen
    return toFlaskJson(_protoresp)

  except Exception:
    return "Invalid SearchAxesRequest\n"


@app.route('/units/search', methods = ['POST'])
def searchUnits():
  """
  POST /units/search
  { "names": ["gene-exp"] }
  """

  # validate request
  req = getRequest(request)
  
  try:

    # get proto, validates
    jreq = fromJson(json.dumps(req), services.SearchUnitsRequest)

    filters = {}
    # query backend
    if len(jreq.names) > 0:
      filters['name'] = getMongoFieldFilter(jreq.names, str)

    if len(jreq.ids) > 0:
      filters['_id'] = getMongoFieldFilter(jreq.ids, ObjectId)

    if len(filters) > 0:
      result = db.units.find(filters)
    else:
      result = db.units.find()

    # make proto
    _protoresp = services.SearchUnitsResponse()
    for r in result:
      _protoresp.units.add(name=r['name'], description=r['description'], id=str(r['_id']))

    # jsonify proto to send
    return toFlaskJson(_protoresp)

  except Exception:
    return "Invalid SearchUnitsRequest\n"


@app.route('/keyspaces/search', methods = ['POST'])
def searchKeySpaces():
  """
  POST /keyspaces/search
  {"names": ["name1"], "axis_names": ["axis_name1"], "keyspace_ids": ["57118a40b5262889d1de9c94"], "keys": ["key1"] }
  """
  # validate request
  if not request.json:
    return "Bad content type, must be application/json\n"

  if request.json.get('axisNames', None) is None:
    return 'Axis name required for keyspace search.\n'

  try:

    # get proto, validates
    jreq = fromJson(json.dumps(request.json), services.SearchKeySpacesRequest)

    # handle masks
    mask = setMask(jreq.keys, unicode('mask'), "keys")

    # consolidate filters
    filters = {}
    if len(jreq.names) > 0:
      filters['name'] = getMongoFieldFilter(jreq.names, str)
    
    if len(jreq.keyspace_ids) > 0:
      filters['_id'] = getMongoFieldFilter(jreq.keyspace_ids, ObjectId)

    if len(jreq.axis_names) > 0:
      filters['axis_name'] = getMongoFieldFilter(jreq.axis_names, str)
    
    if len(jreq.keys) > 0:
      filters['keys'] = getMongoFieldFilter(jreq.keys, str)

    if len(filters) > 0:
      result = db.keyspace.find(filters, mask)
    else:
      result = db.keyspace.find()

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


@app.route('/dataframes/search', methods=['POST'])
def searchDataFrames():

  if not request.json:
    return "Bad content type, must be application/json\n"

  check_ks = request.json.get('keyspaceIds', None)
  if check_ks is None or (len(check_ks) == 1 and check_ks[0] == unicode('mask-keys')):
    return 'keyspaceIds required when searching dataframes.\n'

  try:

    # get proto, validates
    jreq = fromJson(json.dumps(request.json), services.SearchDataFramesRequest)

    # handle masks, ommitting contents from endpoint
    mask_contents = None
    # mask_contents = setMask(jreq.dataframe_ids, unicode('mask-contents'), 'contents')
    mask_keys = setMask(jreq.keyspace_ids, unicode('mask-keys'), 'keys')

    # handle filters
    filters = {}
    if len(jreq.dataframe_ids) > 0:
      filters['_id'] = getMongoFieldFilter(jreq.dataframe_ids, ObjectId)

    if len(jreq.keyspace_ids) > 0:
      filters['$or'] = [{'major': getMongoFieldFilter(jreq.keyspace_ids, ObjectId)}]
      filters['$or'].append({'minor': getMongoFieldFilter(jreq.keyspace_ids, ObjectId)})

    if len(filters) > 0:
      result = db.dataframe.find(filters, mask_contents)
    else:
      result = db.dataframe.find({}, mask_contents)

    # make proto
    _protoresp = services.SearchDataFramesResponse(dataframes=[])
    for r in result:
      kmaj_name, kmaj_keys = getKeySpaceInfo(r['major'], mask_keys)
      kmin_name, kmin_keys = getKeySpaceInfo(r['minor'], mask_keys)

      dataframe = models.DataFrame(id=str(r['_id']), \
        major=models.Dimension(keyspace_id=str(r['major']), keys=kmaj_keys), \
        minor=models.Dimension(keyspace_id=str(r['minor']), keys=kmin_keys), \
        units=[], contents=[])

      for unit in r['units']:
        dataframe.units.extend([models.Unit(name=unit['name'], \
                                            description=unit['description'])])

      _protoresp.dataframes.extend([dataframe])

    # jsonify proto to send
    return toFlaskJson(_protoresp)

  except Exception:
    return "Invalid SearchDataFramesRequest\n"


@app.route('/dataframe/slice', methods = ['POST'])
def sliceDataFrame():
  """
  Speed up by bypassing
  """
  if not request.json:
    return "Bad content type, must be application/json\n"

  if request.json.get('dataframeId', None) is None:
    return "dataframeId required for sliceDataframe.\n"

  try:

    # filtering objects
    mask_keys = {"keys": 0}

    # inits
    filters = {}
    vec_filters = {}
    kmaj_keys = None
    kmin_keys = None

    dataframe_id = request.json.get('dataframeId')
    page_start = request.json.get('pageStart', 0)

    new_major = checkDimension(request.json, 'newMajor')
    new_minor = checkDimension(request.json, 'newMinor')

    if dataframe_id is not None:
      result = db.dataframe.find_one({"_id": ObjectId(dataframe_id)})
    else:
      return "DataFrame ID Required for SliceDataFrameRequest."

    # prep vector query
    vc = result.get('contents', [])
    page_end = request.json.get('pageEnd', len(vc))

    # if page start is outside of dataframe length, return empty
    if page_start > len(vc):
      dataframe = {"id": str(result["_id"]), \
                 "major": {"keyspaceId": str(result['major']), "keys": []}, \
                 "minor": {"keyspaceId": str(result['minor']), "keys": []}, \
                 "contents": []}
      return jsonify(dataframe)

    elif page_end > len(vc):
      page_end = len(vc)

    # set filter object
    vec_filters["_id"] = {"$in": vc[page_start:page_end]}

    # subset major
    if new_major is not None:
      kmaj_keys = {"contents."+str(k):1 for k in new_major['keys']}
      kmaj_keys['key'] = 1

    # subset minor
    if new_minor is not None:
      vec_filters['key'] = {"$in": new_minor['keys']}

    vectors = db.vector.find(vec_filters, kmaj_keys)
    vectors.batch_size(1000000)

    # contents = map(createContents, vectors)
    contents = {vector["key"]:vector["contents"] for vector in vectors}

    # avoid invalid keys passing through to keys
    # explore impacts on response time
    if new_major is not None:
      kmaj_keys = contents[contents.keys()[0]].keys()

    if new_minor is not None:
      kmin_keys = contents.keys()

    dataframe = {"id": str(result["_id"]), \
                 "major": {"keyspaceId": str(result['major']), "keys": kmaj_keys}, \
                 "minor": {"keyspaceId": str(result['minor']), "keys": kmin_keys}, \
                 "contents": contents}

    return jsonify(dataframe)

  except Exception:
    return "Invalid SliceDataFrameRequest\n"


def checkDimension(request, dimension):
  check_dim = request.get(dimension, None)
  if check_dim is not None:
    if len(check_dim.get('keys', [])) <= 0:
      return None
  return check_dim

def nullifyToken(json):
  if json.get('nextPageToken', None) is not None:
    json['nextPageToken'] = None
  return json

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
  try:
    return {"$in": map(maptype, filterList)}
  except:
    return None


def setMask(request_list, identifier, mask):
  if identifier in request_list:
    request_list.remove(identifier)
    return {mask: 0}
  return None

def getKeySpaceInfo(keyspace_id, mask=None):
  keyspace = db.keyspace.find_one({"_id": ObjectId(keyspace_id)}, mask)
  return keyspace['name'], keyspace.get('keys', [])

def getRequest(request, return_json={"names":[]}):
  """
  Helper method to handle empty jsons
  """
  if request.get_json() == {}:
    return return_json
  elif not request.json:
    return "Bad content type, must be application/json\n"

  return request.json

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
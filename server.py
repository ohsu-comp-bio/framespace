import os, json
import util as util
from flask import Flask, request, jsonify
from pymongo import MongoClient
from bson import ObjectId

from proto.framespace import framespace_pb2 as fs

# used for development
#from google.protobuf import json_format

# name passed to flask app will bind to db,
app = Flask('framespace')
# if not docker, then run locally
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
  req = util.getRequest(request)

  try:
    # get proto, validates
    jreq = util.fromJson(json.dumps(req), fs.SearchAxesRequest)

    # query backend
    if len(jreq.names) > 0:
      result = db.axis.find({"name": util.getMongoFieldFilter(jreq.names, str)})
    else:
      result = db.axis.find()

    # make proto
    _protoresp = fs.SearchAxesResponse()
    for r in result:
      _protoresp.axes.add(name=r['name'], description=r['description'])

    return util.toFlaskJson(_protoresp)

  except Exception as e:
    return "".join([str(e), "\n"])

@app.route('/axes', methods = ['GET'])
def axes():
  """
  GET /axes
  Return all axes
  """
  try:
    result = db.axis.find()

    # make proto
    _protoresp = fs.SearchAxesResponse()
    for r in result:
      _protoresp.axes.add(name=r['name'], description=r['description'])

    return util.toFlaskJson(_protoresp)

  except Exception as e:
    return "".join([str(e), "\n"])

@app.route('/axes/<name>', methods= ['GET'])
def axesName(name):
  """
  GET /axes/name
  Return axis with a given name
  """
  try:
    result = db.axis.find_one({"name": str(name)})

    _axis = fs.Axis()

    if result:
      _axis = fs.Axis(name=result['name'], description=result['description'])
      return util.toFlaskJson(_axis)

    return jsonify({})

  except Exception as e:
    return "".join([str(e), "\n"])


@app.route('/units/search', methods = ['POST'])
def searchUnits():
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


@app.route('/units', methods = ['GET'])
def units():
  """
  GET /units
  Return all units
  """
  try:
    result = db.units.find()

    # make proto
    _protoresp = fs.SearchUnitsResponse()
    for r in result:
      _protoresp.units.add(id=str(r['_id']), name=r['name'], description=r['description'])

    return util.toFlaskJson(_protoresp)

  except Exception as e:
    return "".join([str(e), "\n"])


@app.route('/units/<unit_id>', methods = ['GET'])
def unitsId(unit_id):
  """
  GET /units/{unit_id}
  Return unit with the give id
  """
  try:
    result = db.units.find_one({"_id": ObjectId(unit_id)})

    # make proto
    if result:
      _unit = fs.Unit(id=str(result['_id']), name=result['name'], description=result['description'])
      return util.toFlaskJson(_unit)
    else:
      return jsonify({})

  except Exception as e:
    return "".join([str(e), "\n"])



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
    jreq = util.fromJson(json.dumps(request.json), fs.SearchKeySpacesRequest)

    # handle masks
    mask = util.setMask(jreq.keys, unicode('mask'), "keys")

    # consolidate filters
    filters = {}
    if len(jreq.names) > 0:
      filters['name'] = util.getMongoFieldFilter(jreq.names, str)
    
    if len(jreq.keyspace_ids) > 0:
      filters['_id'] = util.getMongoFieldFilter(jreq.keyspace_ids, ObjectId)

    if len(jreq.axis_names) > 0:
      filters['axis_name'] = util.getMongoFieldFilter(jreq.axis_names, str)
    
    if len(jreq.keys) > 0:
      filters['keys'] = util.getMongoFieldFilter(jreq.keys, str)

    if len(filters) > 0:
      result = db.keyspace.find(filters, mask)
    else:
      result = db.keyspace.find()

    # make proto
    _protoresp = fs.SearchKeySpacesResponse()
    for r in result:
      if r.get('keys', None) is None:
        r['keys'] = []
      _protoresp.keyspaces.add(name=r['name'], axis_name=r['axis_name'], id=str(r['_id']), keys=r['keys'])

    return util.toFlaskJson(_protoresp)

  except Exception as e:
    return "".join([str(e), "\n"])


@app.route('/dataframes/search', methods=['POST'])
def searchDataFrames():
  """
  POST {"keyspaceIds": [ID1, ID2]}
  Search for dataframes in FrameSpace registered to a given keyspace. 
  Contents omitted in return for clarity.
  """

  if not request.json:
    return "Bad content type, must be application/json\n"

  check_ks = request.json.get('keyspaceIds', None)
  if check_ks is None or (len(check_ks) == 1 and check_ks[0] == unicode('mask-keys')):
    return 'keyspaceIds required when searching dataframes.\n'

  try:

    # get proto, validates
    jreq = util.fromJson(json.dumps(request.json), fs.SearchDataFramesRequest)

    # handle masks, ommitting contents from endpoint
    mask_contents = {"contents": 0}
    mask_keys = util.setMask(jreq.keyspace_ids, unicode('mask-keys'), 'keys')

    # handle filters
    filters = {}
    if len(jreq.dataframe_ids) > 0:
      filters['_id'] = util.getMongoFieldFilter(jreq.dataframe_ids, ObjectId)

    if len(jreq.keyspace_ids) > 0:
      filters['$or'] = [{'major': util.getMongoFieldFilter(jreq.keyspace_ids, ObjectId)}]
      filters['$or'].append({'minor': util.getMongoFieldFilter(jreq.keyspace_ids, ObjectId)})

    # query backend
    if len(filters) > 0:
      result = db.dataframe.find(filters, mask_contents)
    else:
      result = db.dataframe.find({}, mask_contents)

    # make proto
    _protoresp = fs.SearchDataFramesResponse(dataframes=[])
    for r in result:
      kmaj_name, kmaj_keys = util.getKeySpaceInfo(db, r['major'], mask_keys)
      kmin_name, kmin_keys = util.getKeySpaceInfo(db, r['minor'], mask_keys)

      dataframe = fs.DataFrame(id=str(r['_id']), \
        major=fs.Dimension(keyspace_id=str(r['major']), keys=kmaj_keys), \
        minor=fs.Dimension(keyspace_id=str(r['minor']), keys=kmin_keys), \
        units=[], contents=[])

      for unit in r['units']:
        dataframe.units.extend([fs.Unit(name=unit['name'], \
                                            description=unit['description'])])

      _protoresp.dataframes.extend([dataframe])

    return util.toFlaskJson(_protoresp)

  except Exception as e:
    return "".join([str(e), "\n"])


@app.route('/dataframe/slice', methods = ['POST'])
def sliceDataFrame():
  """
  POST {"dataframeId": ID}
  Returns a dataframe or a subset of a dataframe. 
  Unsupported: Transpose via passing dimensions. 
  Speed up by by-passing proto message creation in response
  """
  if not request.json:
    return "Bad content type, must be application/json\n"

  if request.json.get('dataframeId', None) is None:
    return "dataframeId required for sliceDataframe.\n"

  try:

    # inits
    vec_filters = {}

    # validate request
    jreq = util.fromJson(json.dumps(request.json), fs.SliceDataFrameRequest)

    # first request to get dataframe
    result = db.dataframe.find_one({"_id": ObjectId(str(jreq.dataframe_id))})

    # prep vector query
    vc = result.get('contents', [])
    
    # save page end for later check
    page_end = int(jreq.page_end)
    # if page start is outside of dataframe length, return empty
    if jreq.page_start > len(vc):
      dataframe = {"id": str(result["_id"]), \
                 "major": {"keyspaceId": str(result['major']), "keys": []}, \
                 "minor": {"keyspaceId": str(result['minor']), "keys": []}, \
                 "contents": []}
      return jsonify(dataframe)

    elif jreq.page_end > len(vc) or len(jreq.new_minor.keys) > 0 or jreq.page_end == 0:
      jreq.page_end = len(vc)

    # construct vector filters
    vec_filters["_id"] = {"$in": vc[jreq.page_start:jreq.page_end]}

    kmaj_keys = None
    if len(jreq.new_major.keys) > 0:
      kmaj_keys = {"contents."+str(k):1 for k in jreq.new_major.keys}
      kmaj_keys['key'] = 1

    if len(jreq.new_minor.keys) > 0:
      vec_filters['key'] = {"$in": map(str, jreq.new_minor.keys)}

    # seconrd query to backend to get contents
    vectors = db.vector.find(vec_filters, kmaj_keys)
    vectors.batch_size(1000000)

    # construct response
    contents = {vector["key"]:vector["contents"] for vector in vectors}

    # avoid invalid keys passing through to keys
    # explore impacts on response time
    kmaj_keys = []
    if len(jreq.new_major.keys) > 0:
      kmaj_keys = contents[contents.keys()[0]].keys()

    # return keys in dimension, 
    # if the whole dimension is not returned
    kmin_keys = []
    if len(jreq.new_minor.keys) > 0 or page_end < len(vc):
      kmin_keys = contents.keys()

    dataframe = {"id": str(result["_id"]), \
                 "major": {"keyspaceId": str(result['major']), "keys": kmaj_keys}, \
                 "minor": {"keyspaceId": str(result['minor']), "keys": kmin_keys}, \
                 "contents": contents}

    return jsonify(dataframe)

  except Exception as e:
    return "".join([str(e), "\n"])


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)

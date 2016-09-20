"""
Collection of util functions used in server.py
"""

import json
from flask import request, jsonify
from google.protobuf import json_format
from bson import ObjectId

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

def getMongoFieldFilter(filterList, maptype, from_get=False):

  # catch GET calls
  if from_get:
    filterList = filterList[0].split(',')

  try:
    return {"$in": map(maptype, filterList)}
  except:
    return None

def setMask(request_list, identifier, mask):

  if identifier in request_list:
    request_list.remove(identifier)
    return {mask: 0}
  return None

def getKeySpaceInfo(db, keyspace_id, mask=None):
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

def authenticate(request):
  token = request.headers.get('authorization', None)
  return str(token)


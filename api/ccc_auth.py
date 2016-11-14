import requests
import json

def strToBool(s):
  return s.lower() in ('true', 'True')

def extractToken(request):
  try:
    jwt_token = str(request.headers['Authorization'].split(' ')[1])
  except:
    return False 

  return jwt_token

def extractResource(request):
  resource = request.path
  print resource
  if request.query_string:
    resource += "?"+request.query_string
  print "resource:", resource
  return resource

def keyspacesByPass(request):
  pass

def validateRulesEngine(request):
  """
  Pass token, action, and resource to the rules engine
  to validate the request
  """
  print dir(request)
  print request.url
  method = request.method
  resource = extractResource(request)
  jwt_token = extractToken(request)
  # r = requests.post('http://192.168.11.252:8903/v1/ruleEngine/validation', \
  r = requests.post('http://127.0.0.1:5000/v1/ruleEngine/validation', \
    data = json.dumps({'token': jwt_token, 'action':str(method), 'resources': [str(resource)]}), \
    headers={'content-type': 'application/json'})

  if strToBool(r.content):
    return True

  return False

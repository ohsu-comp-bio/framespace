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
  # transform all resource requests to the /keyspaces/ID format
  if resource.split("/")[1] != 'keyspaces':
    resource = '/keyspaces'
    if request.method == 'POST':
      request_data = json.loads(request.data)
      try:
        # /dataframes/search
        request_data['keyspaceIds'].remove('mask-keys')
        resource = "/".join([resource, request_data['keyspaceIds'][0]])
      except:
        # /dataframe/slice
        resource = "/".join([resource,request_data['newMajor']['keyspaceId']])
  if request.query_string:
    resource += "/"+request.query_string.replace('keyspaceIds=','').replace('newMajorId=', '')

def validateRulesEngine(request):
  """
  Pass token, action, and resource to the rules engine
  to validate the request
  """
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

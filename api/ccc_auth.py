import requests
import json

def strToBool(s):
  return s.lower() in ('true', 'True')

def validateRulesEngine(request):
  """
  Pass token, action, and resource to the rules engine
  to validate the request
  """
  print dir(request)
  print request.url
  method = request.method
  # rules engine does not support host:ip
  resource = request.path
  if request.query_string:
    resource += "?"+request.query_string
  print "resource:", resource
  try:
    jwt_token = str(request.headers['Authorization'].split(' ')[1])
  except:
    return False	
  print jwt_token
  r = requests.post('http://192.168.11.252:8903/v1/ruleEngine/validation', \
    data = json.dumps({'token': jwt_token, 'action':str(method), 'resources': [str(resource)]}), \
    headers={'content-type': 'application/json'})

  if strToBool(r.content):
    return True

  return False

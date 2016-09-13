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
  resource = request.url
  jwt_token = str(request.headers['Authorization'].split(' ')[1])

  r = requests.post('http://localhost:5000/v1/ruleEngine/validation', \
    data = json.dumps({'token': jwt_token, 'action':str(method), 'resources': str(resource)}), \
    headers={'content-type': 'application/json'})

  if strToBool(r.content):
    return True

  return False
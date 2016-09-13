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
  jwt_token = str(request.headers['Authorization'].split(' ')[1])

  r = requests.post('http://localhost:5000/v1/ruleEngine/validation', \
    data = json.dumps({'token': jwt_token, 'action':str(request.method), 'resources': str(request.url)}), \
    headers={'content-type': 'application/json'})

  return strToBool(r.content)
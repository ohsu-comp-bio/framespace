import os
from flask import Flask
from flask_restful import Resource

from pymongo import MongoClient

from api import FramespaceApi
from api.units import Units, Unit
from api.axes import Axis, Axes
from api.keyspaces import KeySpace, KeySpaces
from api.dataframe import DataFrame

app = Flask(__name__)
api = FramespaceApi(app)

# if not docker, then run locally
try:
  mongo = MongoClient(os.environ['DB_PORT_27017_TCP_ADDR'], 27017, connect=False)
except:
  mongo = MongoClient('0.0.0.0', 27017, connect=False)

db = mongo['framespace']

api.add_resource(Axes, '/axes', '/axes/search', resource_class_kwargs={'db': db})
api.add_resource(Axis, '/axes/<name>', resource_class_kwargs={'db': db})
api.add_resource(Units, '/units', '/units/search', resource_class_kwargs={'db': db})
api.add_resource(Unit, '/units/<name>', resource_class_kwargs={'db': db})
api.add_resource(KeySpace, '/keyspaces/<keyspace_id>', resource_class_kwargs={'db': db})
api.add_resource(KeySpaces, '/keyspaces', '/keyspaces/search', resource_class_kwargs={'db': db})
api.add_resource(DataFrame, '/dataframe', resource_class_kwargs={'db': db})


if __name__ == '__main__':
    app.run(host='0.0.0.0', threaded=True, debug=True)

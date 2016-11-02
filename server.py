import os
from flask import Flask
from flask_restful import Resource, Api
from functools import wraps

from pymongo import MongoClient

from api.units import Units, Unit
from api.axes import Axis, Axes
from api.keyspaces import KeySpace, KeySpaces
from api.dataframes import DataFrames
from api.dataframe import DataFrame, Transpose

application = Flask(__name__)
api = Api(application)

# if not docker, then run locally
try:
  mongo = MongoClient(os.environ['DB_PORT_27017_TCP_ADDR'], 27017, connect=False)
except:
  mongo = MongoClient('0.0.0.0', 27017)

db = mongo['framespace']

api.add_resource(Axes, '/framespace/axes', '/framespace/axes/search', resource_class_kwargs={'db': db})
api.add_resource(Axis, '/framespace/axes/<name>', resource_class_kwargs={'db': db})
api.add_resource(Units, '/framespace/units', '/framespace/units/search', resource_class_kwargs={'db': db})
api.add_resource(Unit, '/framespace/units/<name>', resource_class_kwargs={'db': db})
api.add_resource(KeySpace, '/framespace/keyspaces/<keyspace_id>', resource_class_kwargs={'db': db})
api.add_resource(KeySpaces, '/framespace/keyspaces', '/framespace/keyspaces/search', resource_class_kwargs={'db': db})
api.add_resource(DataFrames, '/framespace/dataframes', '/framespace/dataframes/search', resource_class_kwargs={'db': db})
api.add_resource(DataFrame, '/framespace/dataframe/<dataframe_id>', '/framespace/dataframe/slice', resource_class_kwargs={'db': db})
api.add_resource(Transpose, '/framespace/dataframe/transpose/<dataframe_id>', resource_class_kwargs={'db': db})


if __name__ == '__main__':
    application.run(host='0.0.0.0', threaded=True, debug=True)

import traceback

class ServerException(Exception):
  httpStatus = 500

class BadRequestException(Exception):
  httpStatus = 400

class RequiredFieldException(BadRequestException):
  def __init__(self, required_field):
    self.message = "The {} field may not be blank.".format(
      required_field)

class JsonRequiredException(BadRequestException):
  message = "Bad content type, must be application/json."

class NotFoundException(Exception):
  httpStatus = 404
  status_code = 404

class ObjectNotFoundException(NotFoundException):
  message = "The request object was not found."

class AxisNotFoundException(NotFoundException):
  def __init__(self, axis_name):
    self.message = "The requested Axis '{}' was not found.".format(
      axis_name)

class UnitNotFoundException(NotFoundException):
  def __init__(self, unit_name):
    self.message = "The requested Unit '{}' was not found.".format(
      unit_name)

class KeySpaceNotFoundException(NotFoundException):
  def __init__(self, keyspace_id):
    self.message = "The requested KeySpace with id '{}' was not found.".format(
      keyspace_id)

class DataFrameNotFoundException(NotFoundException):
  def __init__(self, dataframe_id):
    self.message = "The requested DataFrame with id '{}' was not found.".format(
      dataframe_id)
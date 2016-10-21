import traceback

class ServerException(Exception):
    httpStatus = 500

class BadRequestException(Exception):
    httpStatus = 400

class JsonRequiredException(BadRequestException):
    message = "Bad content type, must be application/json."

class NotFoundException(Exception):
    httpStatus = 404
    status_code = 404

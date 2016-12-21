import sys
import traceback

from flask import jsonify, current_app
from flask_restful import Api

def get_traceback():
    debug_mode = current_app.config.get("DEBUG", False)
    if debug_mode:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        return traceback.format_exception(exc_type, exc_value, exc_traceback)
    else:
        return ""

class FramespaceApi(Api):
    def handle_error(self, exception):
        try:
            response = jsonify({
                "message": getattr(exception, "message", "Unknown error"),
                "traceback": get_traceback(),
            })
            default_status = 500
            response.status_code = getattr(exception, "httpStatus", default_status)
            return response

        # It's possible that an exception will be throw while handling the
        # error, in which case Flask/Flask-restful aren't very helpful,
        # so try to mitigate that here.
        except Exception as e:
            response = jsonify({
                "message": "Unknown error occurred.",
                "traceback": get_traceback(),
            })
            response.status_code = 500
            return response

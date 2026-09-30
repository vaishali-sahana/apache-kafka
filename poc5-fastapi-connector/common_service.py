import sys

from fastapi import HTTPException
from logger_config import MyLogger

logger= MyLogger.get_logger(__name__)

class AppServices:
    @staticmethod
    def app_response(status_code: int, message: str, success: bool = None,
                     data: any = None) -> dict:
        response = {
            "status_code": status_code,
            "message": message,
            "success": success,
            "data": data
        }

        return response

    @staticmethod
    def handle_exception(exception, is_raise=False):
        exc_type, _, tb = sys.exc_info()
        f = tb.tb_frame
        line_no, filename, function_name = \
            tb.tb_lineno, f.f_code.co_filename, f.f_code.co_name

        message = exception.detail if hasattr(exception, "detail") and bool(
            exception.detail) \
            else f"Exception type: {exc_type}, " \
                 f"Exception message: {exception.__str__()}, " \
                 f"Filename: {filename}, " \
                 f"Function name: {function_name} " \
                 f"Line number: {line_no}"

        logger.error(f"Exception error message: {message}")

        if is_raise:
            raise HTTPException(
                status_code=exception.status_code if hasattr(exception,
                                                             "status_code") else
               500,
                detail="Internal Server Error")

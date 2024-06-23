import json
from time import sleep
from types import GeneratorType
from typing import Callable, Any, Union

from pymongo.errors import (
    ServerSelectionTimeoutError,
    AutoReconnect,
    NetworkTimeout,
    ConnectionFailure,
)


class BaseMotorDanticException(Exception):
    pass


class NotDeclaredField(BaseMotorDanticException):
    def __init__(self, field_name: str, fields: list, *args):
        self.field_name = field_name
        self.fields = fields
        super().__init__(*args)

    def __str__(self):
        return f"This field - {self.field_name} not declared in {self.fields}"


class DoesNotExist(BaseMotorDanticException):
    def __init__(self, model_name: str, *args):
        super().__init__(args)
        self.model_name = model_name

    def __str__(self):
        return f'row does not exist for model: {self.model_name}'


class MotordanticValidationError(BaseMotorDanticException):
    def __init__(self, error_message: Any, pydanctic_validation_error: Any = None):
        self.error_message = error_message
        self.pydanctic_validation_error = pydanctic_validation_error

    def errors(self):
        if self.pydanctic_validation_error is None:
            return self.error_message
        return self.pydanctic_validation_error.errors()

    def json(self, *, indent: Union[None, int, str] = 2) -> str:
        if self.pydanctic_validation_error is None:
            return json.dumps(self.error_message, indent=indent)
        return self.pydanctic_validation_error.json()


class MotordanticInvalidArgsParams(BaseMotorDanticException):
    def __str__(self):
        return 'Arguments must be Query objects'


class MotordanticConnectionError(BaseMotorDanticException):
    pass


class MotordanticIndexError(BaseMotorDanticException):
    pass


def handle_and_convert_connection_errors(func: Callable) -> Any:
    """decorator for handle connection errors and raise MongoConnectionError

    Args:
        func (Callable):any query to mongo

    Returns:
        Any: data
    """

    def generator_wrapper(generator):
        yield from generator

    def main_wrapper(*args, **kwargs):
        counter = 1
        while True:
            try:
                result = func(*args, **kwargs)
                if isinstance(result, GeneratorType):
                    result = generator_wrapper(result)
                return result
            except (
                AutoReconnect,
                ServerSelectionTimeoutError,
                NetworkTimeout,
                ConnectionFailure,
            ) as e:
                counter += 1
                if counter > 3:
                    raise MotordanticConnectionError(str(e))
                sleep(counter)

    return main_wrapper

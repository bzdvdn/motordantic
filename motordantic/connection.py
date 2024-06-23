import os
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient
from bson.raw_bson import RawBSONDocument

from .singleton import Singleton
from .manager import ODMManager


class MotordanticConnection(object, metaclass=Singleton):
    __slots__ = (
        'address',
        'database_name',
        'max_pool_size',
        'server_selection_timeout_ms',
        'connect_timeout_ms',
        'socket_timeout_ms',
        'ssl_cert_path',
    )

    _connections: dict = {}

    def __init__(
        self,
        address: str,
        database_name: str,
        max_pool_size: int = 250,
        ssl_cert_path: Optional[str] = None,
        server_selection_timeout_ms: int = 60000,
        connect_timeout_ms: int = 30000,
        socket_timeout_ms: int = 60000,
    ):
        self.address = address
        self.database_name = database_name
        self.max_pool_size = max_pool_size
        self.ssl_cert_path = ssl_cert_path
        self.server_selection_timeout_ms = server_selection_timeout_ms
        self.connect_timeout_ms = connect_timeout_ms
        self.socket_timeout_ms = socket_timeout_ms

    def _init_mongo_connection(self, connect: bool = False) -> AsyncIOMotorClient:  # type: ignore
        connection_params: dict = {
            'host': self.address,
            'connect': connect,
            'serverSelectionTimeoutMS': self.server_selection_timeout_ms,
            'maxPoolSize': self.max_pool_size,
            'connectTimeoutMS': self.connect_timeout_ms,
            'socketTimeoutMS': self.socket_timeout_ms,
        }
        if self.ssl_cert_path:
            connection_params['tlsCAFile'] = self.ssl_cert_path
            connection_params['tlsAllowInvalidCertificates'] = bool(self.ssl_cert_path)
        client = AsyncIOMotorClient(
            **connection_params,
            document_class=RawBSONDocument,
        )
        return client

    def _get_motor_client(self) -> AsyncIOMotorClient:  # type: ignore
        pid = os.getpid()
        if pid in self._connections:
            return self._connections[pid]
        else:
            mongo_connection = self._init_mongo_connection()
            self._connections[os.getpid()] = mongo_connection
            return mongo_connection


def connect(
    address: str,
    database_name: str,
    max_pool_size: int = 100,
    ssl_cert_path: Optional[str] = None,
    server_selection_timeout_ms: int = 60000,
    connect_timeout_ms: int = 30000,
    socket_timeout_ms: int = 60000,
) -> MotordanticConnection:
    """init connection to mongodb

    Args:
        address (str): full connection string
        database_name (str): mongo db name
        max_pool_size (int, optional): max connection pool. Defaults to 100.
        ssl_cert_path (Optional[str], optional): path to ssl cert. Defaults to None.
        server_selection_timeout_ms (int, optional): ServerSelectionTimeoutMS. Defaults to 60000.
        connect_timeout_ms (int, optional): ConnectionTimeoutMS. Defaults to 30000.
        socket_timeout_ms (int, optional): SocketTimeoutMS. Defaults to 60000.

    Returns:
        MotordanticConnection: motordantic connection
    """
    os.environ['MOTORDANTIC_DATABASE'] = database_name
    os.environ['MOTORDANTIC_ADDRESS'] = address
    os.environ['MOTORDANTIC_MAX_POOL_SIZE'] = str(max_pool_size)
    os.environ['MOTORDANTIC_SERVER_SELECTION_TIMOUT_MS'] = str(
        server_selection_timeout_ms
    )
    os.environ['MOTORDANTIC_CONNECT_TIMEOUT_MS'] = str(connect_timeout_ms)
    os.environ['MOTORDANTIC_SOCKET_TIMEOUT_MS'] = str(socket_timeout_ms)
    if ssl_cert_path:
        os.environ['MOTORDANTIC_SSL_CERT_PATH'] = ssl_cert_path
    connection = MotordanticConnection(
        address=address,
        database_name=database_name,
        max_pool_size=max_pool_size,
        server_selection_timeout_ms=server_selection_timeout_ms,
        connect_timeout_ms=connect_timeout_ms,
        socket_timeout_ms=socket_timeout_ms,
        ssl_cert_path=ssl_cert_path,
    )
    ODMManager.use(connection)
    return connection

from typing import Union

from aiopg.sa import SAConnection
from databases import Database


class ConnectionAdapter:
    def __init__(self, connection: Union[SAConnection, Database]):
        if isinstance(connection, SAConnection):
            self.execute = connection.execute
            self.begin = connection.begin
            self.scalar = connection.scalar
        elif isinstance(connection, Database):
            self.execute = connection.execute
            self.begin = connection.transaction
            self.scalar = connection.fetch_val

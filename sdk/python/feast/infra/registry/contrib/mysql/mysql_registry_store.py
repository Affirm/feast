# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import os
import uuid
import pymysql
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryFile
from urllib.parse import urlparse

from pymysql.connections import Connection
from pymysql.cursors import Cursor


from feast.infra.registry.registry import RegistryConfig
from feast.infra.registry.registry_store import RegistryStore
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
from pydantic import StrictStr

REGISTRY_SCHEMA_VERSION = "1"

class MySQLRegistryConfig(RegistryConfig):
    type = "mysql"

    host: Optional[StrictStr] = None
    user: Optional[StrictStr] = None
    password: Optional[StrictStr] = None
    database: Optional[StrictStr] = None
    port: Optional[int] = None

class MySQLRegistryStore(RegistryStore):
    def __init__(self, registry_config: MySQLRegistryConfig, repo_path: Path):
        print("using mysql registry")
        self.db_config = MySQLRegistryConfig(
            host=registry_config.host,
            port=registry_config.port,
            database=registry_config.database,
            db_schema=registry_config.db_schema,
            user=registry_config.user,
            password=registry_config.password,
        )
        self.table_name = registry_config.path
        self.cache_ttl_seconds = registry_config.cache_ttl_seconds

    def _get_conn(self) -> Connection:
        if not self._conn:
            self._conn = pymysql.connect(
                host=self.db_config.host or "127.0.0.1",
                user=self.db_config.user or "test",
                password=self.db_config.password or "test",
                database=self.db_config.database or "feast",
                port=self.db_config.port or 3306,
                autocommit=True,
            )
        return self._conn

    def get_registry_proto(self) -> RegistryProto:
        registry_proto = RegistryProto()
        try:
            with self._get_conn(self.db_config) as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT registry
                    FROM {self.table_name}
                    WHERE version = (SELECT max(version) FROM {self.table_name})
                    """
                )
                row = cur.fetchone()
            if row:
                registry_proto = registry_proto.FromString(row[0])
        except pymysql.err.DatabaseError:
            pass
        return registry_proto

    def update_registry_proto(self, registry_proto: RegistryProto):
        """
        Overwrites the current registry proto with the proto passed in. This method
        writes to the registry path.

        Args:
            registry_proto: the new RegistryProto
        """
        pass

    def teardown(self):
        with self._get_conn(self.db_config) as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                DROP TABLE IF EXISTS {self.table_name};
                """
            )

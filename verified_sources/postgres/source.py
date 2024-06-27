import os
from typing import (Any, Optional, Tuple, Mapping, List)
import psycopg
from dat_core.connectors.sources.base import SourceBase
from dat_core.connectors.sources.stream import Stream
from verified_sources.postgres.specs import PostgresSpecification
from verified_sources.postgres.catalog import PostgresCatalog
from verified_sources.postgres.streams import PostgresStream0


class Postgres(SourceBase):
    _spec_class = PostgresSpecification
    _catalog_class = PostgresCatalog

    def check_connection(self, config: PostgresSpecification) -> Tuple[bool, Optional[Any]]:
        try:
            connection = self.create_connection(config)

            cursor = connection.cursor()
            cursor.execute("SELECT 1;")
            result = cursor.fetchone()
            print("Query result:", result)
            connection.close()
            return True, None
        except Exception as e:
            return False, str(e)
    
    def create_connection(self, config) -> Any:
        return psycopg.connect(
            conninfo=(
                f"host={config.connection_specification.host} port={config.connection_specification.port} "
                f"dbname={config.connection_specification.dbname} user={config.connection_specification.user} "
                f"password={config.connection_specification.password}"
            )
        )

    def discover(self, config: PostgresSpecification) -> Mapping[str, Any]:
        try:
            connection = self.create_connection(config)
            cursor = connection.cursor()
            schemas = config.connection_specification.schemas
            
            schema_placeholders = ','.join(['%s'] * len(schemas))
        
            # Query to get all tables and their schemas from specified schemas
            cursor.execute(f"""
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE' AND table_schema IN ({schema_placeholders});
            """, schemas)
            
            tables = cursor.fetchall()
            schema_dict = {}

            for table_schema, table_name in tables:
                # Query to get columns for each table
                cursor.execute("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s;
                """, (table_schema, table_name))

                columns = cursor.fetchall()
                schema_dict[f"{table_schema}.{table_name}"] = {
                    "columns": [{ "name": col[0], "type": col[1] } for col in columns]
                }
            
            connection.close()
            return schema_dict
        except Exception as e:
            raise RuntimeError(f"Failed to discover schema: {str(e)}")

    def streams(self, config: Mapping[str, Any], json_schemas: Mapping[str, Mapping[str, Any]] = None) -> List[Stream]:
        return [
            PostgresStream0(config),
        ]

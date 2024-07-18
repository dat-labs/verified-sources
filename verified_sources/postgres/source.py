from typing import (
    Any, Dict, Optional,
    Tuple, Mapping, List,
)
from dat_core.pydantic_models import (
    ConnectorSpecification
)
from pydantic import Field, create_model
import psycopg
from dat_core.connectors.sources.base import SourceBase
from dat_core.connectors.sources.stream import Stream
from verified_sources.postgres.specs import PostgresSpecification
from verified_sources.postgres.catalog import PostgresTableStream
from verified_sources.postgres.streams import PostgresStream


class Postgres(SourceBase):
    _spec_class = PostgresSpecification
    _has_dynamic_streams = True

    def check_connection(self, config: PostgresSpecification) -> Tuple[bool, Optional[Any]]:
        """
        Checks the connection to Postgres using the provided configuration.

        Args:
            config (ConnectorSpecification): The configuration specifying the connection details.

        Returns:
            Tuple[bool, Optional[Any]]: A tuple containing the connection status and an error message
        """
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

    def create_connection(self, config) -> psycopg.connection:
        """
        Creates a connection to the Postgres database.
        
        Args:
            config (PostgresSpecification): The Postgres configuration object.
        
        Returns:
            psycopg.connection: The connection object.
        """
        return psycopg.connect(
            conninfo=(
                f"host={config.connection_specification.host} port={config.connection_specification.port} "
                f"dbname={config.connection_specification.dbname} user={config.connection_specification.user} "
                f"password={config.connection_specification.password}"
            )
        )

    def _get_tables(self, config: PostgresSpecification) -> Mapping[str, Any]:
        """
        Get all tables and their schemas from the specified schemas.

        Args:
            config (PostgresSpecification): The Postgres configuration object.
        
        Returns:
            Mapping[str, Any]: A dictionary containing the tables and their schemas.
        """
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
                    "columns": [{"name": col[0], "type": col[1]} for col in columns]
                }
            connection.close()
            print("Schema dict:", schema_dict)
            return schema_dict
        except Exception as e:
            raise RuntimeError(f"Failed to discover schema: {str(e)}")

    def create_pydantic_model(self, stream: Stream) -> Any:
        """
        Creates a Pydantic model class for the stream.

        Args:
            stream (Stream): The stream object.

        Returns:
            Any: The Pydantic model class.
        """
        return create_model(
            stream.name,
            name=(str, Field(
                stream.name,
                description='The name of the document stream.',
                json_schema_extra={'ui-opts': {'hidden': True}}
            )),
            json_schema=(dict, Field(
                stream.json_schema,
                description='The JSON schema for the document stream.',
                json_schema_extra={'ui-opts': {'hidden': True}}
            )),
            __base__=PostgresTableStream
        )

    def map_column_type_to_json_schema_type(self, column_type) -> str:
        """
        Maps the column type to a JSON schema type.

        Args:
            column_type (str): The column type.
        
        Returns:
            str: The JSON schema type
        """
        type_mapping = {
            'character varying': 'string',
            'timestamp without time zone': 'string',
            'json': 'object',
            'USER-DEFINED': 'string',
            'integer': 'integer'
        }
        # Default to string if type not found
        return type_mapping.get(column_type, 'string')

    def create_table_info(self, table_name: str, columns: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Creates the table info for the stream.

        Args:
            columns (List[Dict[str, Any]]): The columns of the table.
        
        Returns:
            Dict[str, Any]: The table info.
        """
        table_info = {}
        properties = {}

        for column in columns['columns']:
            column_name = column['name']
            column_type = column['type']
            json_schema_type = self.map_column_type_to_json_schema_type(
                column_type)

            properties[column_name] = {
                'type': json_schema_type
            }

        table_info[table_name] = {
            'type': 'object',
            'properties': properties
        }

        return table_info

    def streams(self, config: ConnectorSpecification) -> List[Stream]:
        """
        Dynamically creates streams based on the tables in the specified schemas.

        Args:
            config (ConnectorSpecification): The configuration specifying the connection details.

        Returns:
            List[Stream]: A list of streams.
        """
        tables = self._get_tables(config)
        streams = []

        for index, (schema_table, cols) in enumerate(tables.items(), start=1):
            stream_name = f"PostgresStream{index}"
            StreamClass = type(
                stream_name,
                (PostgresStream,),
                {}
            )
            _schema_table = schema_table.split('.')
            table_name = _schema_table[-1]
            schema = _schema_table[0]

            StreamClass.name = table_name
            StreamClass._schema = schema
            StreamClass._table_name = table_name
            StreamClass.json_schema = self.create_table_info(
                table_name=table_name, columns=cols
            )
            stream_instance = StreamClass(config=config)

            streams.append(stream_instance)

        return streams

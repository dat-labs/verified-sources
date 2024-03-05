import os
from typing import Tuple, Any, List, Mapping
from dat_core.connectors.sources.base import SourceBase
from dat_core.connectors.sources.stream import Stream
from verified_sources.wikipedia.streams import ContentSearch
from dat_core.pydantic_models.connector_specification import ConnectorSpecification
from dat_core.auth.core import NoAuth


class Wikipedia(SourceBase):
    """
    Wikipedia as a source
    """
    _spec_file = 'https://raw.githubusercontent.com/dc-rahul/verified-sources/feature/google-drive-and-wikipedia-forked/verified_sources/wikipedia/specs.yml'
    _catalog_file = 'https://raw.githubusercontent.com/dc-rahul/verified-sources/feature/google-drive-and-wikipedia-forked/verified_sources/wikipedia/catalog.yml'

    def check_connection(self, config: ConnectorSpecification) -> Tuple[bool, Any]:
        """
        Check whether the user provided config is able to make a connection 
        to Wikipedia or not
        """
        return True, 'No Authentication required'
    
    def streams(self, config: Mapping[str, Any], json_schemas: Mapping[str, Mapping[str, Any]]=None) -> List[Stream]:
        """
        Return list of available streams in Zendesk

        Args:
            config (Mapping[str, Any]): The user-provided configuration as specified by
              the source's spec.
            json_schemas (Mapping[str, Mapping[str, Any]]): List of json schemas with each item a dictionary
                with it's key as stream name
        Returns:
            List[Stream]: No need to explain
        """
        auth = NoAuth()
        if not json_schemas:
            catalog_json = self.read_catalog_file()
            streams = catalog_json['properties']['streams']['items']
            json_schemas = {_s['properties']['name']: _s['properties']['json_schema']['properties'] for _s in streams}
        return [ContentSearch(
            config=config, 
            # model_dict=self, 
            schema=json_schemas.get(ContentSearch.name), 
            authenticator=auth,
        )]
    

if __name__ == '__main__':
    config = ConnectorSpecification(name='Wikipedia', connectionSpecification={})
    catalog = Wikipedia().discover(config=config)
    print(catalog)
    doc_generator = Wikipedia().read(config=config, catalog=catalog)
    for doc in doc_generator:
        print(doc.model_dump_json())
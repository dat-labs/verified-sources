from typing import Any, Generator
from dat_core.connectors.sources.stream import Stream
from dat_core.pydantic_models import DatCatalog, DatDocumentStream, DatMessage, StreamState
from dat_core.doc_splitters.factory import doc_splitter_factory
from verified_sources.website_crawler.specs import WebsiteCrawlerSpecification

class URLCrawler(Stream):

    _name = 'url_crawler'
    _default_cursor = 'site_url'

    def __init__(self, config: WebsiteCrawlerSpecification) -> None:
        self._config = config
    
    def read_records(self, 
        catalog: DatCatalog,
        configured_stream: DatDocumentStream,
        cursor_value: Any = None
        ) -> Generator[DatMessage, Any, Any]:
        # Depends on the type of loader you are using
        _loader_config = {
            'web_path': self._config.connection_specification.site_url
        }
        doc_splitter = doc_splitter_factory.create(
            filepath=self._config.connection_specification.site_url,
            loader_key='web_crawler',
            splitter_key=str(configured_stream.advanced.chunking_strategy),
            loader_config=_loader_config,
            splitter_config=configured_stream.advanced.splitter_config.model_dump()
            )
        for chunk in doc_splitter.load_and_chunk():
            yield self.as_record_message(
                configured_stream=configured_stream,
                doc_chunk=chunk.page_content,
                data_entity=self._config.connection_specification.site_url,
                extra_metadata={'site_url': self._config.connection_specification.site_url}
            )
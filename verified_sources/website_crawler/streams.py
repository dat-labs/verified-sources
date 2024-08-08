"""
Module: url_crawler_module

This module contains the definition of the URLCrawler class, which is a Stream subclass for crawling and processing URLs.

Classes:
    URLCrawler: A class for crawling and processing URLs.

Dependencies:
    - typing: Used for type hints.
    - dat_core.connectors.sources.stream.Stream: A Stream class from the dat_core.connectors.sources.stream module.
    - dat_core.pydantic_models.DatCatalog: A DatCatalog class from the dat_core.pydantic_models module.
    - dat_core.pydantic_models.DatDocumentStream: A DatDocumentStream class from the dat_core.pydantic_models module.
    - dat_core.pydantic_models.DatMessage: A DatMessage class from the dat_core.pydantic_models module.
    - dat_core.pydantic_models.StreamState: A StreamState class from the dat_core.pydantic_models module.
    - verified_sources.common.doc_splitters.factory.doc_splitter_factory: A doc_splitter_factory function from the verified_sources.common.doc_splitters.factory module.
    - verified_sources.website_crawler.specs.WebsiteCrawlerSpecification: A WebsiteCrawlerSpecification class from the verified_sources.website_crawler.specs module.
"""

from typing import Any, Generator
from dat_core.connectors.sources.stream import Stream
from dat_core.pydantic_models import DatCatalog, DatDocumentStream, DatMessage, StreamState
from verified_sources.common.doc_splitters.factory import doc_splitter_factory, DocLoaderType, TextSplitterType
from verified_sources.website_crawler.specs import WebsiteCrawlerSpecification
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import SessionNotCreatedException

class URLCrawler(Stream):
    """
    URLCrawler class for crawling and processing URLs.

    Attributes:
        _name (str): The name of the URLCrawler stream ('url_crawler').
        _default_cursor (str): The default cursor value ('site_url').

    Methods:
        __init__: Initializes a new URLCrawler object.
        read_records: Reads records from the configured stream and yields DatMessage objects.
    """

    _name = 'url_crawler'
    _default_cursor = 'site_url'

    def __init__(self, config: WebsiteCrawlerSpecification) -> None:
        """
        Initializes a new URLCrawler object.

        Parameters:
            config (WebsiteCrawlerSpecification): The configuration object for URL crawling.
        """
        self._config = config
    
    def read_records(self, 
        catalog: DatCatalog,
        configured_stream: DatDocumentStream,
        cursor_value: Any = None
        ) -> Generator[DatMessage, Any, Any]:
        """
        Reads records from the configured stream and yields DatMessage objects.

        Parameters:
            catalog (DatCatalog): The DatCatalog object.
            configured_stream (DatDocumentStream): The configured DatDocumentStream object.
            cursor_value (Any, optional): The cursor value (default: None).

        Yields:
            Generator[DatMessage, Any, Any]: A generator yielding DatMessage objects.
        """
        try:
            chrome_options = Options()
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-dev-shm-usage')
            driver = webdriver.Chrome(options=chrome_options)
        except SessionNotCreatedException:
            service = webdriver.ChromeService(executable_path='/opt/chromedriver-linux64/chromedriver')
            options = webdriver.ChromeOptions()
            options.binary_location = '/opt/chrome-headless-shell-linux64/chrome-headless-shell'
            options.add_argument('--no-sandbox')
            driver = webdriver.Chrome(service=service,options=options)


        _loader_config = {
            'prefix': self._config.connection_specification.site_url,
            'driver': driver
        }
        _load_kwargs = {
            'base_url': self._config.connection_specification.site_url
        }
        doc_splitter = doc_splitter_factory.create(
            loader_key=DocLoaderType.WHOLE_SITE_READER,
            splitter_key=TextSplitterType.SPLIT_BY_CHARACTER_RECURSIVELY.value,
            loader_config=_loader_config
            # splitter_key=configured_stream.advanced.splitter_settings.strategy,
            # splitter_config=configured_stream.advanced.splitter_settings.config
            )
        for chunk in doc_splitter.load_and_chunk(**_load_kwargs):
            try:
                # Sometimes load_and_chunk return Document object, sometimes it's string
                _doc_chunk = chunk.page_content
            except (ValueError, AttributeError):
                _doc_chunk = chunk
            yield self.as_record_message(
                configured_stream=configured_stream,
                doc_chunk=_doc_chunk,
                data_entity=self._config.connection_specification.site_url,
                extra_metadata={'site_url': self._config.connection_specification.site_url}
            )

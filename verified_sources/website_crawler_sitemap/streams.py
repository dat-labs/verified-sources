from typing import Any, Generator, List, Optional
from dat_core.connectors.sources.stream import Stream
from dat_core.pydantic_models import DatCatalog, DatDocumentStream, DatMessage, StreamState
from dat_core.doc_splitters.factory import doc_splitter_factory, DocLoaderType, TextSplitterType
from verified_sources.website_crawler_sitemap.specs import WebsiteCrawlerSitemapSpecification, FilterSpecification
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from pydantic import HttpUrl
import requests

class CrawlerSitemap(Stream):
    """
    website_crawler_sitemapStream0 class for crawling and processing URLs.

    Attributes:
        _name (str): The name of the website_crawler_sitemapStream0 stream ('url_crawler').

    Methods:
        __init__: Initializes a new website_crawler_sitemapStream0 object.
        read_records: Reads records from the configured stream and yields DatMessage objects.
    """

    _name = 'crawler_sitemap'

    def __init__(self, config: WebsiteCrawlerSitemapSpecification) -> None:
        """
        Initializes a new website_crawler_sitemapStream0 object.

        Parameters:
            config (WebsiteCrawlerSitemapSpecification): The configuration object for URL crawling.
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
        _loader_config = {
            'site_url': self._config.connection_specification.site_url,
            'filters': self._config.connection_specification.filters
        }

        doc_splitter = doc_splitter_factory.create(
            loader_key=DocLoaderType.BEAUTIFUL_SOUP,
            splitter_key=TextSplitterType.SPLIT_BY_CHARACTER_RECURSIVELY.value,
            loader_config=_loader_config
        )

        for chunk in doc_splitter.load_and_chunk():
            try:
                _doc_chunk = chunk.page_content
            except (ValueError, AttributeError):
                _doc_chunk = chunk
            yield self.as_record_message(
                configured_stream=configured_stream,
                doc_chunk=_doc_chunk,
                data_entity=self._config.connection_specification.site_url,
                extra_metadata={'site_url': self._config.connection_specification.site_url}
            )

        
    def get_links(self, url: HttpUrl, filter: FilterSpecification) -> List[HttpUrl]:
        """
        Given a url, return list of all http URLs in it if a sitemap exists
        """
        sitemap_url = self.get_sitemap(url)
        if not sitemap_url:
            return []
        response = requests.get(sitemap_url)
        sitemap = response.content
        soup = BeautifulSoup(sitemap, 'xml')
        links = set()
        for loc in soup.find_all('loc'):
            link = loc.text
            links.add(link)

            # TODO

        return list(links)
    
    def get_sitemap(self, url: HttpUrl) -> Optional[HttpUrl]:
        parsed_url = urlparse(url)
        sitemap_url = f"{parsed_url.scheme}://{parsed_url.netloc}/sitemap.xml"
        response = requests.get(sitemap_url)
        if response.status_code == 200:
            return sitemap_url
        else:
            return None
        
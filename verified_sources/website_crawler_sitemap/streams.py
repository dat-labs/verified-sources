from typing import Any, Generator, HttpUrl, List, Optional
from dat_core.connectors.sources.stream import Stream
from dat_core.pydantic_models import DatCatalog, DatDocumentStream, DatMessage, StreamState
from dat_core.doc_splitters.factory import doc_splitter_factory, DocLoaderType, TextSplitterType
from verified_sources.website_crawler_sitemap.specs import WebsiteCrawlerSitemapSpecification
from urllib.parse import urlparse
from bs4 import BeautifulSoup
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

        
    def get_links(self, url: HttpUrl, filter: dict) -> List[HttpUrl]:
        """
        Given a url, return list of all http URLs in it if a sitemap exists
        """
        sitemap_url = self.get_sitemap(url)
        response = requests.get(sitemap_url)
        sitemap = response.content
        soup = BeautifulSoup(sitemap, 'xml')
        links = set()
        for loc in soup.find_all('loc'):
            link = loc.text
            
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
        
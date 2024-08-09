import os
from typing import (Any, Optional, Tuple, Mapping, List)
from urllib.parse import urlparse

from pydantic import HttpUrl
import requests
from dat_core.connectors.sources.base import SourceBase
from dat_core.connectors.sources.stream import Stream
from verified_sources.website_crawler_sitemap.specs import WebsiteCrawlerSitemapSpecification
from verified_sources.website_crawler_sitemap.catalog import WebsiteCrawlerSitemapCatalog
from verified_sources.website_crawler_sitemap.streams import CrawlerSitemap


class WebsiteCrawlerSitemap(SourceBase):
    _spec_class = WebsiteCrawlerSitemapSpecification
    _catalog_class = WebsiteCrawlerSitemapCatalog

    def check_connection(self, config: WebsiteCrawlerSitemapSpecification) -> Tuple[bool, Optional[Any]]:
        sitemap_url = self.get_sitemap(config.connection_specification.site_url)
        if not sitemap_url:
            return False, 'No sitemap found'
        else:
            return True, 'Sitemap found'

    def streams(self, config: Mapping[str, Any], json_schemas: Mapping[str, Mapping[str, Any]]=None) -> List[Stream]:
        return [
            CrawlerSitemap(config),
        ]
    
    def get_sitemap(self, url: HttpUrl) -> Optional[HttpUrl]:
        parsed_url = urlparse(url)
        sitemap_url = f"{parsed_url.scheme}://{parsed_url.netloc}/sitemap.xml"
        response = requests.get(sitemap_url)
        if response.status_code == 200:
            return sitemap_url
        else:
            return None
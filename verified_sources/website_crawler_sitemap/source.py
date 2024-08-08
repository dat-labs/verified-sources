import os
from typing import (Any, Optional, Tuple, Mapping, List)
from dat_core.connectors.sources.base import SourceBase
from dat_core.connectors.sources.stream import Stream
from verified_sources.website_crawler_sitemap.specs import WebsiteCrawlerSitemapSpecification
from verified_sources.website_crawler_sitemap.catalog import WebsiteCrawlerSitemapCatalog
from verified_sources.website_crawler_sitemap.streams import website_crawler_sitemapStream0


class WebsiteCrawlerSitemap(SourceBase):
    _spec_class = WebsiteCrawlerSitemapSpecification
    _catalog_class = WebsiteCrawlerSitemapCatalog

    def check_connection(self, config: WebsiteCrawlerSitemapSpecification) -> Tuple[bool, Optional[Any]]:
        # Implement your connection check logic here
        return False, {}

    def streams(self, config: Mapping[str, Any], json_schemas: Mapping[str, Mapping[str, Any]]=None) -> List[Stream]:
        return [
            website_crawler_sitemapStream0(config),
        ]
import os
from typing import (Any, Optional, Tuple, Mapping, List)
from urllib.parse import urlparse

from pydantic import HttpUrl
import requests
from dat_core.connectors.sources.base import SourceBase
from dat_core.connectors.sources.stream import Stream
from verified_sources.website_crawler_sitemap.specs import WebsiteCrawlerSitemapSpecification
from verified_sources.website_crawler_sitemap.catalog import WebsiteCrawlerSitemapCatalog, CrawlerSitemapStream
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
        
if __name__ == '__main__':
    website_crawler_sitemap = WebsiteCrawlerSitemap()

    urls = {
        'dtc-docs':'https://docs.datachannel.co/getting-started/1.0.0/index.html',
        'blogger':'https://blog-test-123-1.blogspot.com/',
    }

    _config = WebsiteCrawlerSitemapSpecification(
        module_name='website_crawler_sitemap',
        name='WebsiteCrawlerSitemap',
        connection_specification={
            'site_url': urls['dtc-docs'],
            'filter':{
                # This filter fetches the data of the first 5 levels of any url in the sitemap
                # starting with the url https://docs.datachannel.co/getting-started/1.0.0/applications/cloud_application
                'max_depth':'5',
                'prefix':'https://docs.datachannel.co/getting-started/1.0.0/applications/cloud_application',
                # 'sitemap_url':'https://blog-test-123-1.blogspot.com/sitemap.xml',
            }
        }
    )

    _stream = CrawlerSitemapStream(config=_config)
    _catalog = WebsiteCrawlerSitemapCatalog(document_streams=[_stream,])
    for msg in website_crawler_sitemap.read(_config, _catalog):
        print(msg.model_dump_json(), end="\n\n")
        # Uncomment this to write output to a file
        # with open('sitemap_data.txt', 'a') as f:
        #     f.write(msg.model_dump_json() + '\n\n')
from dat_core.connectors.sources.stream import Stream
import requests
from typing import Any, List, Mapping, Tuple
from dat_core.connectors.sources.base import SourceBase
from dat_core.pydantic_models import ConnectorSpecification
from verified_sources.website_crawler.streams import URLCrawler

class WebsiteCrawler(SourceBase):

    def check_connection(self, config: ConnectorSpecification) -> Tuple[bool, Any | None]:
        try:
            resp = requests.get(config.connection_specification.site_url)
            resp.raise_for_status()
            result, msg = True, 'url working'
        except requests.exceptions.ConnectionError as exc:
            print(repr(exc))
            result, msg = False, 'url not working'

        return result, msg
    
    def streams(self, config: Mapping[str, Any], json_schemas: Mapping[str, Mapping[str, Any]] = None) -> List[Stream]:
        return [
            URLCrawler(config)
        ]

if __name__ == '__main__':
    from dat_core.pydantic_models import ReadSyncMode, WriteSyncMode
    from verified_sources.website_crawler.specs import WebsiteCrawlerSpecification
    from verified_sources.website_crawler.catalog import (
        WebCrawlerCatalog, Crawler,
        Advanced, ChunkingStrategy
        )

    _specs = WebsiteCrawlerSpecification(connection_specification={'site_url': 'https://shopify.dev/docs/api/usage/pagination-graphql'})
    website_crawler = WebsiteCrawler()
    print(website_crawler.check(_specs))
    _stream = Crawler(
        namespace='my-crawler',
        read_sync_mode=ReadSyncMode.FULL_REFRESH,
        write_sync_mode=WriteSyncMode.REPLACE,
        advanced=Advanced(
            chunking_strategy=ChunkingStrategy.split_by_html_header,
            splitter_config={'headers_to_split_on': [('h2', 'h2'), ]}
            )
    )
    _catalog = WebCrawlerCatalog(document_streams=[_stream,])
    for msg in website_crawler.read(_specs, _catalog):
        print(msg.model_dump_json())

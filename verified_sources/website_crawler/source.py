from dat_core.connectors.sources.stream import Stream
import requests
from typing import Any, List, Mapping, Tuple
from dat_core.connectors.sources.base import SourceBase
from dat_core.pydantic_models import ConnectorSpecification
from verified_sources.website_crawler.streams import Crawler

class WebsiteCrawler(SourceBase):

    def check_connection(self, config: ConnectorSpecification) -> Tuple[bool, Any | None]:
        try:
            resp = requests.get(config.connection_specification.site_url)
            resp.raise_for_status()
            result, msg = True, 'url working'
        except Exception as exc:
            print(repr(exc))
            result, msg = False, 'url not working'

        return result, msg
    
    def streams(self, config: Mapping[str, Any], json_schemas: Mapping[str, Mapping[str, Any]] = None) -> List[Stream]:
        return [
            Crawler(config)
        ]

if __name__ == '__main__':
    from verified_sources.website_crawler.specs import WebsiteCrawlerSpecification
    from verified_sources.website_crawler.catalog import WebCrawlerCatalog, Crawler, ReadSyncMode, WriteSyncMode

    _specs = WebsiteCrawlerSpecification(connection_specification={'site_url': 'https://this.that.com'})
    website_crawler = WebsiteCrawler()
    website_crawler.check(_specs)
    _stream = Crawler(
        namespace='my-crawler',
        read_sync_mode=ReadSyncMode.FULL_REFRESH,
        write_sync_mode=WriteSyncMode.REPLACE,
    )
    _catalog = WebCrawlerCatalog(document_streams=[Crawler,])

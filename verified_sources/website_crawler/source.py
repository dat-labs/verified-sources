"""
Module: website_crawler_module

This module contains the definition of the WebsiteCrawler class, which is a SourceBase subclass for crawling websites.

Classes:
    WebsiteCrawler: A class for crawling websites and creating URLCrawler streams.

Dependencies:
    - dat_core.connectors.sources.stream.Stream: A Stream class from the dat_core.connectors.sources.stream module.
    - requests: Used for making HTTP requests.
    - typing: Used for type hints.
    - dat_core.connectors.sources.base.SourceBase: A SourceBase class from the dat_core.connectors.sources.base module.
    - dat_core.pydantic_models.ConnectorSpecification: A ConnectorSpecification class from the dat_core.pydantic_models module.
    - verified_sources.website_crawler.streams.URLCrawler: A URLCrawler class from the verified_sources.website_crawler.streams module.
"""
import os
from dat_core.connectors.sources.stream import Stream
import requests
from typing import Any, List, Mapping, Tuple
from dat_core.connectors.sources.base import SourceBase
from dat_core.pydantic_models import (
    ConnectorSpecification,
    DatMessage,
    DatLogMessage,
    Type,
    Level
)
from verified_sources.website_crawler.streams import URLCrawler

class WebsiteCrawler(SourceBase):
    """
    WebsiteCrawler class for crawling websites and creating URLCrawler streams.

    Methods:
        check_connection: Checks the connection to a website URL.
        streams: Generates URLCrawler streams based on the configuration.
    """

    def check_connection(self, config: ConnectorSpecification) -> Tuple[bool, Any | None]:
        """
        Checks the connection to a website URL.

        Parameters:
            config (ConnectorSpecification): The connector specification object.

        Returns:
            Tuple[bool, Any | None]: A tuple indicating the connection status (True for success, False for failure) and an optional message.
        """
        try:
            resp = requests.get(config.connection_specification.site_url)
            resp.raise_for_status()
            result, msg = True, 'URL is working'
        except (requests.exceptions.ConnectionError, requests.exceptions.HTTPError) as exc:
            _msg = DatMessage(
                type=Type.LOG,
                log=DatLogMessage(
                    level=Level.ERROR,
                    message=repr(exc)
                )
            )
            print(_msg.model_dump_json())
            result, msg = False, 'URL is not working'

        return result, msg
    
    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        Generates URLCrawler streams based on the configuration.

        Parameters:
            config (Mapping[str, Any]): The configuration mapping.

        Returns:
            List[Stream]: A list of URLCrawler streams.
        """
        return [
            URLCrawler(config)
        ]


if __name__ == '__main__':
    from dat_core.pydantic_models import ReadSyncMode, WriteSyncMode, Advanced, SplitByHtmlHeaderSettings, SplitByHtmlHeaderExtraConfig
    from verified_sources.website_crawler.specs import WebsiteCrawlerSpecification
    from verified_sources.website_crawler.catalog import (
        WebCrawlerCatalog, Crawler
        )

    _specs = WebsiteCrawlerSpecification(
        connection_specification={'site_url': 'https://shopify.dev/docs/api/usage/pagination-graphql'})
    website_crawler = WebsiteCrawler()
    print(website_crawler.check(_specs))
    _stream = Crawler(
        namespace='my-crawler',
        read_sync_mode=ReadSyncMode.INCREMENTAL,
        write_sync_mode=WriteSyncMode.REPLACE,
        advanced=Advanced(
            splitter_settings=SplitByHtmlHeaderSettings(config=SplitByHtmlHeaderExtraConfig()),
            )
    )
    _catalog = WebCrawlerCatalog(document_streams=[_stream,])
    for msg in website_crawler.read(_specs, _catalog):
        print(msg.model_dump_json())

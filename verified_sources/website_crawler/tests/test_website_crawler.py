from verified_sources.website_crawler.source import WebsiteCrawler
from verified_sources.website_crawler.catalog import WebsiteCrawlerCatalog
from dat_core.pydantic_models import (
    DatConnectionStatus, DatMessage, Type,
)
from verified_sources.website_crawler.specs import ConnectionSpecification, WebsiteCrawlerSpecification
from conftest import *


def test_check(valid_connection_object):
    check_connection_tpl = WebsiteCrawler().check(
        config=WebsiteCrawlerSpecification(
            name='WebsiteCrawler',
            connection_specification=valid_connection_object,
            module_name='website_crawler'
        )
    )
    assert isinstance(check_connection_tpl, DatConnectionStatus)
    assert check_connection_tpl.status.name == 'SUCCEEDED'


def test_discover(valid_connection_object):
    _d = WebsiteCrawler().discover(
        config=WebsiteCrawlerSpecification(
            name='WebsiteCrawler',
            connection_specification=valid_connection_object,
            module_name='website_crawler'
        )
    )
    assert isinstance(_d, dict)


def test_read(valid_connection_object, valid_catalog_object):
    config = WebsiteCrawlerSpecification(
        name='WebsiteCrawler',
        connection_specification=valid_connection_object,
        module_name='website_crawler'
    )
    website_crawler = WebsiteCrawler()
    records = website_crawler.read(
        config=config,
        catalog=WebsiteCrawlerCatalog(**valid_catalog_object),
    )
    for record in records:
        if record.type == Type.STATE:
            continue   
        assert DatMessage.model_validate(record)

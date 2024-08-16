from verified_sources.website_crawler_sitemap.source import WebsiteCrawlerSitemap
from verified_sources.website_crawler_sitemap.catalog import WebsiteCrawlerSitemapCatalog
from verified_sources.website_crawler_sitemap.specs import WebsiteCrawlerSitemapSpecification
from dat_core.pydantic_models import DatConnectionStatus, DatMessage, Type
from conftest import *


def test_check(valid_connection_object):
    check_connection_tpl = WebsiteCrawlerSitemap().check(
        config=WebsiteCrawlerSitemapSpecification(
            name='WebsiteCrawlerSitemap',
            connection_specification=valid_connection_object,
            module_name='website_crawler_sitemap'
        )
    )
    assert isinstance(check_connection_tpl, DatConnectionStatus)
    assert check_connection_tpl.status.name == 'SUCCEEDED'


def test_discover(valid_connection_object):
    _d = WebsiteCrawlerSitemap().discover(
        config=WebsiteCrawlerSitemapSpecification(
            name='WebsiteCrawlerSitemap',
            connection_specification=valid_connection_object,
            module_name='website_crawler_sitemap'
        )
    )
    assert isinstance(_d, dict)


def test_read(valid_connection_object, valid_catalog_object):
    config = WebsiteCrawlerSitemapSpecification(
        name='WebsiteCrawlerSitemap',
        connection_specification=valid_connection_object,
        module_name='website_crawler_sitemap'
    )

    website_crawler_sitemap = WebsiteCrawlerSitemap()
    records = website_crawler_sitemap.read(
        config=config,
        catalog=WebsiteCrawlerSitemapCatalog(**valid_catalog_object),
    )
    for record in records:
        if record.type == Type.STATE:
            continue   
        assert DatMessage.model_validate(record)

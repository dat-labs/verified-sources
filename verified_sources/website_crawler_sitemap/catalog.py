# generated by datamodel-codegen:
#   filename:  catalog.yml
#   timestamp: 2024-04-18T11:39:43+00:00

from __future__ import annotations

from typing import List, Optional

from pydantic import Field
from dat_core.pydantic_models import DatDocumentStream, DatCatalog, Advanced

class CrawlerSitemapStream(DatDocumentStream):
    name: Optional[str] = Field(
        'crawler_sitemap',
        description='The name of the document stream.',
        json_schema_extra={
            'ui-opts': {
                'hidden': True,
            }
        }
    )
    namespace: Optional[str] = Field(
        None, description='namespace the data is associated with'
    )


class WebsiteCrawlerSitemapCatalog(DatCatalog):
    document_streams: Optional[List[CrawlerSitemapStream]] = None

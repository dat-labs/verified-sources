from urllib.parse import urlparse
from pydantic import HttpUrl
import requests

def get_sitemap(url: HttpUrl):
        parsed_url = urlparse(url)
        sitemap_url = f"{parsed_url.scheme}://{parsed_url.netloc}/sitemap.xml"
        response = requests.get(sitemap_url)
        if response.status_code == 200:
            return sitemap_url
        else:
            return None

print(urlparse('https://docs.datachannel.co/getting-started/1.0.0/index.html'))
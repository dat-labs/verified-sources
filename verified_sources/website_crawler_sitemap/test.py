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
def get_link(link):
    parsed_url = urlparse(link)
    print(parsed_url)
    segments = parsed_url.path.split('/')
    link = f"{parsed_url.scheme}://{parsed_url.netloc}{'/'.join(segments[:3])}"
    return link

print(get_link('https://docs.datachannel.co/getting-started/1.0.0/index.html'))
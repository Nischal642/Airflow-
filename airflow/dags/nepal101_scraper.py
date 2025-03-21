import requests
from urllib.parse import urlparse
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import time
import json
import random
from pathlib import Path

# Configurations
BASE_URL = 'https://www.nepal101.net/'
SITEMAP_URL = 'https://www.nepal101.net/sitemaps.xml'
OUTPUT_DIR = Path('/home/nischalacharya/Documents/Tourism_Pipeline/dags/Output')
OUTPUT_FILE = OUTPUT_DIR / 'nepal101_content.json'
CRAWL_DELAY = (1, 2)
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15'
]
MAX_RETRIES = 3
TIMEOUT = 15

# Ensure output directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def get_headers():
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': BASE_URL
    }

def is_allowed(url):
    parsed = urlparse(url)
    path = parsed.path.lower()
    disallowed_paths = ['/wp-admin/']
    return not any(path.startswith(disallowed) for disallowed in disallowed_paths) or path == '/wp-admin/admin-ajax.php'

def parse_sitemap(sitemap_url):
    namespaces = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    urls = []
    try:
        response = requests.get(sitemap_url, headers=get_headers(), timeout=TIMEOUT)
        root = ET.fromstring(response.content)
        if root.tag == '{http://www.sitemaps.org/schemas/sitemap/0.9}sitemapindex':
            for sitemap in root.findall('sitemap:sitemap', namespaces):
                urls.extend(parse_sitemap(sitemap.find('sitemap:loc', namespaces).text.strip()))
        elif root.tag == '{http://www.sitemaps.org/schemas/sitemap/0.9}urlset':
            for url in root.findall('sitemap:url', namespaces):
                urls.append(url.find('sitemap:loc', namespaces).text.strip())
    except Exception as e:
        print(f"Error parsing sitemap: {e}")
    return urls

def scrape_content(url, session):
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(url, headers=get_headers(), timeout=TIMEOUT)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            for element in soup(['script', 'style', 'img', 'footer', 'nav', 'form']):
                element.decompose()
            content = [tag.get_text(strip=True) for tag in soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6'])]
            return "\n".join(filter(None, content))
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt+1} failed: {e}")
            time.sleep((2 ** attempt) + random.random())
    return None

def main():
    scraped_urls = set()
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            scraped_urls.update(json.loads(line)['url'] for line in f if line.strip())
    
    with requests.Session() as session:
        todo_urls = [url for url in parse_sitemap(SITEMAP_URL) if is_allowed(url) and url not in scraped_urls]
        print(f"Found {len(todo_urls)} URLs to scrape")
        with open(OUTPUT_FILE, 'a', encoding='utf-8') as f:
            for idx, url in enumerate(todo_urls, 1):
                print(f"[{idx}/{len(todo_urls)}] Processing: {url[:80]}...")
                if content := scrape_content(url, session):
                    f.write(json.dumps({'url': url, 'content': content}, ensure_ascii=False) + '\n')
                time.sleep(random.uniform(*CRAWL_DELAY) * (1.5 if idx % 50 == 0 else 1))

if __name__ == '__main__':
    main()

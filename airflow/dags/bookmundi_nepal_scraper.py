import requests
from urllib.parse import urlparse
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import time
import json
import random
from pathlib import Path

# Configuration
BASE_URL = 'https://www.bookmundi.com/y/nepal'
SITEMAP_URL = 'https://assets.bookmundi.com/sitemaps/sitemap.xml'
OUTPUT_FILE = '/home/nischalacharya/Documents/Pipeline/airflow/dags/Output_JSON/bookmundi_nepal_content.json'
CRAWL_DELAY = (5, 8)  # Randomized delay between requests
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15'
]
MAX_RETRIES = 3
TIMEOUT = 15

def get_headers():
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': BASE_URL
    }

def is_allowed(url):
    """Enforce hardcoded robots.txt rules"""
    parsed = urlparse(url)
    path = parsed.path.lower()
    query = parsed.query.lower()

    disallowed_paths = [
        '/site/captcha', '/themes/dashboard', '/themes/global/bower_components',
        '/themes/global/views', '/themes/old', '/mailbox', '/hybridauth/default/callback',
        '/rating-and-reviews', '/search', '/login', '/create-account', '/review-widget', 
        '/site/savejserror'
    ]

    for disallowed in disallowed_paths:
        if path.startswith(disallowed.lower()):
            if disallowed == '/site/captcha' and 'refresh=1' not in query:
                continue
            return False

    if 'pagespeed=noscript' in query:
        return False

    blocked_agents = ['ccbot', 'semrushbot', 'dotbot', 'rogerbot', 'seranking seochecker']
    current_ua = get_headers()['User-Agent'].lower()
    if any(agent in current_ua for agent in blocked_agents):
        return False

    return True

def is_nepal_related(url):
    """Filter for Nepal-specific content"""
    nepali_patterns = [
        '/y/nepal', '/nepal-', 'country=nepal', 'destination=nepal',
        'nepal-tour', 'nepal-trek', 'nepal-travel', '/np/', '-in-nepal'
    ]
    url_lower = url.lower()
    return any(pattern in url_lower for pattern in nepali_patterns)

def parse_sitemap(sitemap_url):
    """Parse sitemap with error handling"""
    namespaces = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    urls = []
    
    try:
        response = requests.get(sitemap_url, headers=get_headers(), timeout=TIMEOUT)
        response.raise_for_status()
        root = ET.fromstring(response.content)
        
        if root.tag == '{http://www.sitemaps.org/schemas/sitemap/0.9}sitemapindex':
            for sitemap in root.findall('sitemap:sitemap', namespaces):
                loc = sitemap.find('sitemap:loc', namespaces).text.strip()
                urls.extend(parse_sitemap(loc))
        elif root.tag == '{http://www.sitemaps.org/schemas/sitemap/0.9}urlset':
            for url in root.findall('sitemap:url', namespaces):
                loc = url.find('sitemap:loc', namespaces).text.strip()
                urls.append(loc)
    except Exception as e:
        print(f"Error parsing sitemap {sitemap_url}: {e}")
    
    return urls

def scrape_content(url, session):
    """Scrape content with retry logic"""
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(url, headers=get_headers(), timeout=TIMEOUT)
            response.raise_for_status()
            
            if response.status_code == 429:
                sleep_time = 30 * (attempt + 1)
                print(f"Rate limited. Waiting {sleep_time}s...")
                time.sleep(sleep_time)
                continue
                
            soup = BeautifulSoup(response.text, 'html.parser')
            
            for element in soup(['script', 'style', 'img', 'footer', 'nav', 'form']):
                element.decompose()
            
            content = []
            for tag in soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
                text = tag.get_text(strip=True)
                if text:
                    content.append(text)
            
            return "\n".join(content) if content else None

        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt+1} failed: {e}")
            time.sleep((2 ** attempt) + random.random())
    
    return None

def main():
    # Ensure the output directory exists
    output_path = Path(OUTPUT_FILE)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    scraped_urls = set()

    if output_path.exists():
        with open(output_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    scraped_urls.add(data['url'])
                except json.JSONDecodeError:
                    continue

    with requests.Session() as session:
        all_urls = parse_sitemap(SITEMAP_URL)
        filtered_urls = [
            url for url in all_urls
            if is_allowed(url) and is_nepal_related(url)
        ]
        todo_urls = [url for url in filtered_urls if url not in scraped_urls]

        print(f"Found {len(todo_urls)} Nepal URLs to scrape")

        with open(output_path, 'a', encoding='utf-8') as f:
            for idx, url in enumerate(todo_urls, 1):
                print(f"[{idx}/{len(todo_urls)}] Processing: {url[:80]}...")

                content = scrape_content(url, session)
                if content:
                    record = json.dumps({'url': url, 'content': content}, ensure_ascii=False)
                    f.write(record + '\n')
                    f.flush()

                # Randomized delay with progressive backoff
                delay = random.uniform(CRAWL_DELAY[0], CRAWL_DELAY[1])
                if idx % 50 == 0:
                    delay *= 1.5
                time.sleep(delay)

if __name__ == '__main__':
    main()

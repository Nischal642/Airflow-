import requests
from urllib.parse import urlparse
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import time
import json
from pathlib import Path

# Configuration
BASE_URL = 'https://www.himalayanglacier.com/'
HEADERS = {
    'User-Agent': 'MosaicScraper/1.0 (+https://example.com/scraper)'
}
CRAWL_DELAY = 5  # Seconds between requests
SITEMAP_URL = 'https://www.himalayanglacier.com/sitemap_index.xml'
OUTPUT_FILE = '/home/nischalacharya/Documents/Tourism_Pipeline/dags/Output/himalayan_glacier_content.json'

def is_allowed(url):
    """Hardcoded robots.txt rules for himalayanglacier.com"""
    parsed = urlparse(url)
    path = parsed.path

    # Disallow rule for User-agent: *
    if path.startswith('/register/'):
        return False

    # Allow all other paths
    return True

def parse_sitemap(sitemap_url):
    """Recursively parse sitemap and sitemap index files"""
    namespaces = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    urls = []
    
    try:
        response = requests.get(sitemap_url, headers=HEADERS, timeout=10)
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

def scrape_content(url):
    """Scrape specific content elements while removing images"""
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Remove unwanted elements
        for element in soup(['script', 'style', 'img']):
            element.decompose()
        
        # Extract specific content elements
        content = []
        for tag in soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
            text = tag.get_text(strip=True)
            if text:
                content.append(text)
        
        return "\n".join(content) if content else None
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return None

def load_existing_data(output_file):
    """Load existing scraped data from the output file"""
    if Path(output_file).exists():
        with open(output_file, 'r', encoding='utf-8') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {}
    return {}

def save_data(output_file, data):
    """Ensure the output directory exists and save scraped data"""
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def main():
    try:
        # Load existing scraped data
        scraped_data = load_existing_data(OUTPUT_FILE)
        
        # Extract all URLs from sitemaps
        all_urls = parse_sitemap(SITEMAP_URL)
        
        # Filter URLs through hardcoded robots.txt rules
        allowed_urls = [url for url in all_urls if is_allowed(url)]
        print(f"Found {len(allowed_urls)} allowed URLs to scrape")
    
        # Scrape content from allowed URLs
        for idx, url in enumerate(allowed_urls, 1):
            if url in scraped_data:
                print(f"Skipping already scraped URL {idx}/{len(allowed_urls)}: {url}")
                continue
            
            print(f"Scraping URL {idx}/{len(allowed_urls)}: {url}")
            content = scrape_content(url)
            if content:
                scraped_data[url] = content
                # Save data after each URL is scraped
                save_data(OUTPUT_FILE, scraped_data)
            time.sleep(CRAWL_DELAY)
        
        print(f"\nSuccessfully scraped {len(scraped_data)} pages")
        print("Sample preview:")
        for url, content in list(scraped_data.items())[:3]:
            print(f"\nURL: {url}")
            print(f"Content Preview: {content[:300]}...")

    except Exception as e:
        print(f"Error in main process: {e}")

if __name__ == '__main__':
    main()

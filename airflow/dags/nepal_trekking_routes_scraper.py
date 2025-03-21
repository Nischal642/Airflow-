import requests
from urllib.parse import urlparse
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import time
import json
import os

# Configuration
SITEMAP_URL = 'https://nepaltrekkingroutes.com/sitemap.xml'
HEADERS = {
    'User-Agent': 'MosaicScraper/1.0 (+https://example.com/scraper)'
}
CRAWL_DELAY = 2  # Seconds between requests
OUTPUT_DIR = '/home/nischalacharya/Documents/Pipeline/airflow/dags/Output_JSON'
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'nepalTrekRoute.json')

def ensure_directory_exists(directory):
    """Ensure the output directory exists."""
    if not os.path.exists(directory):
        os.makedirs(directory)

def is_allowed(url):
    """Check if URL is allowed by robots.txt rules for nepaltrekkingroutes.com"""
    parsed = urlparse(url)
    path = parsed.path
    
    disallowed_paths = [
        '/blog/search', '/trip-booking', '/custom-payment', '/private-booking',
        '/lminute-booking', '/cart', '/checkout', '/download_pdf'
    ]
    
    return not any(path.startswith(disallowed) for disallowed in disallowed_paths)

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
        content = [tag.get_text(strip=True) for tag in soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']) if tag.get_text(strip=True)]
        
        return "\n".join(content) if content else None
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return None

def load_existing_data(output_file):
    """Load existing scraped data from the output file"""
    if os.path.exists(output_file):
        with open(output_file, 'r', encoding='utf-8') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {}
    return {}

def save_data(output_file, data):
    """Save scraped data to the output file"""
    ensure_directory_exists(OUTPUT_DIR)
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def main():
    try:
        # Ensure the output directory exists
        ensure_directory_exists(OUTPUT_DIR)

        # Load existing scraped data
        scraped_data = load_existing_data(OUTPUT_FILE)
        
        # Extract all URLs from sitemaps
        all_urls = parse_sitemap(SITEMAP_URL)
        
        # Filter URLs through robots.txt rules
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

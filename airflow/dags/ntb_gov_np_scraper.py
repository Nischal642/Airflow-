import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import time
import json
from collections import deque
from pathlib import Path

# Configuration
BASE_URL = 'https://ntb.gov.np/en'
HEADERS = {
    'User-Agent': 'NTBScraper/1.0 (+https://example.com/scraper)'
}
CRAWL_DELAY = 3  # Seconds between requests
MAX_PAGES = 250  # Maximum number of pages to scrape
OUTPUT_DIR = Path('/home/nischalacharya/Documents/Tourism_Pipeline/dags/Output/')
OUTPUT_FILE = OUTPUT_DIR / 'ntb_gov_np_content.json'

# Ensure the output directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def is_allowed(url):
    """Hardcoded robots.txt rules for ntb.gov.np"""
    return True  # No disallowed paths in robots.txt

def get_links(url):
    """Extract all internal links from a page"""
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        links = set()
        for a in soup.find_all('a', href=True):
            href = a['href']
            full_url = urljoin(BASE_URL, href)
            if full_url.startswith(BASE_URL):
                links.add(full_url)
        
        return links
    except Exception as e:
        print(f"Error getting links from {url}: {e}")
        return set()

def scrape_content(url):
    """Scrape specific content elements while removing images"""
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Remove unwanted elements
        for element in soup(['script', 'style', 'img', 'nav', 'footer']):
            element.decompose()
        
        content = []
        for tag in soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'article']):
            text = tag.get_text(strip=True)
            if text:
                content.append(text)
        
        return "\n".join(content) if content else None
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return None

def main():
    try:
        visited = set()
        to_visit = deque([BASE_URL])
        scraped_data = {}
        
        while to_visit and len(scraped_data) < MAX_PAGES:
            current_url = to_visit.popleft()
            
            if current_url in visited:
                continue
                
            visited.add(current_url)
            
            print(f"Scraping {current_url}")
            content = scrape_content(current_url)
            if content:
                scraped_data[current_url] = content
                
                new_links = get_links(current_url)
                for link in new_links:
                    if link not in visited and is_allowed(link):
                        to_visit.append(link)
            
            time.sleep(CRAWL_DELAY)
            
        # Save results to JSON
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(scraped_data, f, ensure_ascii=False, indent=2)
        
        print(f"\nSuccessfully scraped {len(scraped_data)} pages")
        print(f"Results saved to {OUTPUT_FILE}")
        print("Sample preview:")
        for url, content in list(scraped_data.items())[:3]:
            print(f"\nURL: {url}")
            print(f"Content Preview: {content[:300]}...")

    except Exception as e:
        print(f"Error in main process: {e}")

if __name__ == '__main__':
    main()

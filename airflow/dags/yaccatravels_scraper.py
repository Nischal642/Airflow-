import requests
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import time
import json
import random
from pathlib import Path

# Configuration
BASE_URL = 'https://www.yaccatravels.com/'
OUTPUT_DIR = Path('/home/nischalacharya/Documents/Pipeline/airflow/dags/Output_JSON')
OUTPUT_FILE = OUTPUT_DIR / 'yaccatravels_content.json'
CRAWL_DELAY = (2, 3)  # Random delay between requests
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15'
]
MAX_RETRIES = 3
TIMEOUT = 15

# Ensure the output directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def get_headers():
    """Returns a random user-agent header."""
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': BASE_URL
    }

def is_allowed(url):
    """Filter out unwanted URLs (e.g., admin panels, login pages)."""
    return not urlparse(url).path.lower().startswith('/apanel/')

def extract_links(html, base_url):
    """Extract internal links from a webpage."""
    soup = BeautifulSoup(html, 'html.parser')
    return [urljoin(base_url, a['href']) for a in soup.find_all('a', href=True)]

def scrape_content(url, session):
    """Scrape textual content from a webpage, removing unnecessary elements."""
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(url, headers=get_headers(), timeout=TIMEOUT)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Remove unwanted elements
            for element in soup(['script', 'style', 'img', 'footer', 'nav', 'form']):
                element.decompose()

            # Extract textual content
            content = [tag.get_text(strip=True) for tag in soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6'])]
            return "\n".join(filter(None, content)), extract_links(response.text, url)

        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt+1} failed: {e}")
            time.sleep((2 ** attempt) + random.random())

    return None, []

def main():
    """Main function to scrape the website and save content to a JSON file."""
    scraped_urls, todo_urls = set(), [BASE_URL]

    # Load previously scraped URLs if the file exists
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            try:
                scraped_data = json.load(f)
                scraped_urls.update(scraped_data.keys())  # Store only URLs
            except json.JSONDecodeError:
                print("Warning: Could not decode existing JSON file. Starting fresh.")

    with requests.Session() as session:
        scraped_data = {}  # Dictionary to store results
        while todo_urls:
            url = todo_urls.pop(0)
            if url in scraped_urls:
                continue

            print(f"Processing: {url[:80]}...")

            content, links = scrape_content(url, session)
            if content:
                scraped_data[url] = content
                scraped_urls.add(url)

                # Add new links to the queue if they haven't been visited
                todo_urls.extend(
                    link for link in links if is_allowed(link) and link not in scraped_urls and link not in todo_urls
                )

            # Respect crawl delay
            time.sleep(random.uniform(*CRAWL_DELAY))

        # Save results to JSON
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(scraped_data, f, ensure_ascii=False, indent=2)

        print(f"\nSuccessfully scraped {len(scraped_data)} pages")
        print(f"Results saved to {OUTPUT_FILE}")

if __name__ == '__main__':
    main()

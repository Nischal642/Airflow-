import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import time
import json
from pathlib import Path

# URL for the sitemap index
SITEMAP_INDEX_URL = "https://yetitravels.com/sitemap_index.xml"

# Define the output directory and file
OUTPUT_DIR = Path("/home/nischalacharya/Documents/Tourism_Pipeline/dags/Output/")
OUTPUT_FILE = OUTPUT_DIR / "yetitravels_content.json"

# Ensure the output directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def fetch_xml(url):
    """Fetches XML content from a URL and returns the root Element."""
    response = requests.get(url)
    response.raise_for_status()
    return ET.fromstring(response.content)

def extract_urls_from_sitemap(sitemap_url):
    """Extracts all URLs from a given sitemap."""
    print(f"Fetching sitemap: {sitemap_url}")
    root = fetch_xml(sitemap_url)
    urls = []
    for url in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}url'):
        loc = url.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc')
        if loc is not None and loc.text:
            urls.append(loc.text)
    return urls

def scrape_page_content(url):
    """Scrapes page content from the given URL, excluding images."""
    print(f"Scraping URL: {url}")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    
    # Remove image tags
    for img in soup.find_all('img'):
        img.decompose()
    
    # Extract text from <p> tags and heading tags (<h1>-<h6>)
    content = []
    for tag in soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
        text = tag.get_text(strip=True)
        if text:
            content.append(text)
    
    return "\n".join(content)

def main():
    """Main function to extract URLs, scrape content, and save as JSON."""
    
    # Fetch the sitemap index
    sitemap_index = fetch_xml(SITEMAP_INDEX_URL)

    # Extract sub-sitemap URLs from the index
    sitemap_urls = []
    for sitemap in sitemap_index.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}sitemap'):
        loc = sitemap.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc')
        if loc is not None and loc.text:
            sitemap_urls.append(loc.text)

    print(f"Found {len(sitemap_urls)} sitemaps.")

    # Extract URLs from each sub-sitemap
    all_page_urls = []
    for sitemap_url in sitemap_urls:
        urls = extract_urls_from_sitemap(sitemap_url)
        all_page_urls.extend(urls)
        time.sleep(1)  # Respect crawl delay

    print(f"Found {len(all_page_urls)} page URLs.")

    # Scrape content from each page
    scraped_contents = {}
    for page_url in all_page_urls:
        content = scrape_page_content(page_url)
        if content:
            scraped_contents[page_url] = content
        time.sleep(1)  # Respect crawl delay

    # Save the results as a JSON file
    with open(OUTPUT_FILE, "w", encoding="utf-8") as json_file:
        json.dump(scraped_contents, json_file, ensure_ascii=False, indent=4)

    print(f"\nSuccessfully scraped {len(scraped_contents)} pages")
    print(f"Results saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()

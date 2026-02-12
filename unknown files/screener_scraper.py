#!/usr/bin/env python3
"""
Screener.in Industry and Stock Symbol Scraper
Scrapes all industries and extracts stock symbols from company pages.
"""

import requests
from bs4 import BeautifulSoup
import time
import csv
import re
from urllib.parse import urljoin, urlparse
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ScreenerScraper:
    def __init__(self):
        self.base_url = "https://www.screener.in"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.scraped_data = []
        
    def get_page(self, url, delay=1):
        """Fetch a page with error handling and rate limiting"""
        try:
            logger.info(f"Fetching: {url}")
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            time.sleep(delay)  # Rate limiting
            return response.text
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
    
    def extract_industry_urls(self):
        """Extract all industry URLs from the market overview page"""
        market_url = f"{self.base_url}/market/"
        html = self.get_page(market_url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Find industry links - they should be in the industries table
        industry_links = []
        
        # Look for links that contain industry paths like /market/IN02/IN0201/IN020101/IN020101002/
        links = soup.find_all('a', href=True)
        
        for link in links:
            href = link['href']
            # Filter for industry URLs (contain /market/IN followed by industry IDs)
            if '/market/IN' in href and href.startswith('/market/'):
                full_url = urljoin(self.base_url, href)
                industry_name = link.get_text(strip=True)
                
                # Skip empty names or numbers
                if industry_name and not industry_name.replace('.', '').isdigit():
                    industry_links.append({
                        'url': full_url,
                        'name': industry_name,
                        'relative_url': href
                    })
        
        # Remove duplicates while preserving order
        seen_urls = set()
        unique_links = []
        for link in industry_links:
            if link['relative_url'] not in seen_urls:
                seen_html = link['relative_url']
                unique_links.append(link)
                seen_urls.add(link['relative_url'])
        
        logger.info(f"Found {len(unique_links)} unique industries")
        return unique_links
    
    def extract_companies_from_industry(self, industry_url, industry_name):
        """Extract company URLs from an industry page"""
        html = self.get_page(industry_url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        company_links = []
        
        # Look for company links that typically have format like /company/SYMBOL/
        links = soup.find_all('a', href=True)
        
        for link in links:
            href = link['href']
            # Filter for company URLs
            if '/company/' in href and href.startswith('/company/'):
                full_url = urljoin(self.base_url, href)
                company_name = link.get_text(strip=True)
                
                # Skip empty names or very short names
                if company_name and len(company_name) > 2:
                    company_links.append({
                        'url': full_url,
                        'name': company_name,
                        'symbol': self.extract_symbol_from_url(href)
                    })
        
        logger.info(f"Industry '{industry_name}': Found {len(company_links)} companies")
        return company_links
    
    def extract_symbol_from_url(self, url_path):
        """Extract stock symbol from URL path like /company/EICHERMOT/consolidated/"""
        # Extract symbol from /company/SYMBOL/ or /company/SYMBOL/consolidated/
        match = re.search(r'/company/([^/]+)/', url_path)
        if match:
            return match.group(1).strip()
        return None
    
    def scrape_all_data(self):
        """Main scraping function that orchestrates the entire process"""
        logger.info("Starting Screener.in scraping...")
        
        # Step 1: Get all industry URLs
        industries = self.extract_industry_urls()
        
        # Step 2: For each industry, get companies
        for i, industry in enumerate(industries, 1):
            logger.info(f"Processing industry {i}/{len(industries)}: {industry['name']}")
            
            companies = self.extract_companies_from_industry(industry['url'], industry['name'])
            
            # Step 3: Store the data
            for company in companies:
                if company['symbol']:  # Only include if we extracted a valid symbol
                    self.scraped_data.append({
                        'stock_name': company['name'],
                        'symbol': company['symbol'],
                        'industry': industry['name'],
                        'url': company['url']
                    })
            
            # Add delay between industries to be respectful
            time.sleep(2)
        
        logger.info(f"Scraping completed. Found {len(self.scraped_data)} companies with symbols")
    
    def save_to_csv(self, filename="screener_stocks.csv"):
        """Save the scraped data to CSV file"""
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            if self.scraped_data:
                fieldnames = ['stock_name', 'symbol', 'industry', 'url']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(self.scraped_data)
        
        logger.info(f"Data saved to {filename}")
    
    def save_to_txt(self, filename="screener_stocks.txt"):
        """Save in the requested format: Stock Name,Basic Industry"""
        with open(filename, 'w', encoding='utf-8') as txtfile:
            for item in self.scraped_data:
                txtfile.write(f"{item['stock_name']},{item['industry']}\n")
        
        logger.info(f"Data saved to {filename}")

def main():
    scraper = ScreenerScraper()
    
    try:
        scraper.scrape_all_data()
        
        if scraper.scraped_data:
            # Save in both formats
            scraper.save_to_csv()
            scraper.save_to_txt()
            
            print(f"\nScraping completed successfully!")
            print(f"Total companies found: {len(scraper.scraped_data)}")
            print("Data saved to:")
            print("- screener_stocks.csv (detailed format)")
            print("- screener_stocks.txt (Stock Name,Basic Industry format)")
            
            # Show a sample of the data
            print(f"\nSample of scraped data:")
            for i, item in enumerate(scraper.scraped_data[:5]):
                print(f"{item['stock_name']},{item['industry']}")
            
        else:
            print("No data was scraped. Check the logs for errors.")
            
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        print(f"Error occurred: {e}")

if __name__ == "__main__":
    main()


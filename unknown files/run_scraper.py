#!/usr/bin/env python3
"""
Simple runner script for the Screener.in scraper
"""

from screener_scraper import ScreenerScraper

def main():
    print("=" * 50)
    print("Screener.in Stock Symbol Scraper")
    print("=" * 50)
    print()
    print("This will scrape all industries from Screener.in and extract stock symbols.")
    print("The output will be saved as:")
    print("1. screener_stocks.csv (detailed format)")
    print("2. screener_stocks.txt (Stock Name,Basic Industry format)")
    print()
    
    response = input("Do you want to continue? (y/N): ").strip().lower()
    
    if response in ['y', 'yes']:
        scraper = ScreenerScraper()
        
        try:
            scraper.scrape_all_data()
            
            if scraper.scraped_data:
                scraper.save_to_csv()
                scraper.save_to_txt()
                
                print(f"\n" + "=" * 50)
                print("SCRAPING COMPLETED SUCCESSFULLY!")
                print("=" * 50)
                print(f"Total companies found: {len(scraper.scraped_data)}")
                print(f"Files created: screener_stocks.csv, screener_stocks.txt")
                
                # Show sample data
                print(f"\nSample data:")
                print("-" * 30)
                for item in scraper.scraped_data[:10]:
                    print(f"{item['stock_name']},{item['industry']}")
                
                if len(scraper.scraped_data) > 10:
                    print(f"... and {len(scraper.scraped_data) - 10} more entries")
                    
            else:
                print("\nNo data was scraped. Please check the connection and try again.")
                
        except Exception as e:
            print(f"\nAn error occurred: {e}")
            print("Please check your internet connection and try again.")
            
    else:
        print("Scraping cancelled.")
        return
    
    print("\nPress Enter to exit...")
    input()

if __name__ == "__main__":
    main()


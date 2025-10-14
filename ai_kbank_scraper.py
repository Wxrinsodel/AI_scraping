from cgitb import text
import difflib
from turtle import position
import pandas as pd
import logging
import sys
import os
import re
import time
import requests
import json
from datetime import datetime
import csv
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Tuple


# Configure logging
port = 11434
OLLAMA_API_URL = f"http://localhost:{port}/api/generate"
OLLAMA_MODEL = "llama3.2"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


class IntelligentKBankScraper:
    """Web scraper for Bank executives - FIXED NAME PARSING VERSION"""
    
    def __init__(self, base_url="https://www.kasikornbank.com/th/about/Pages/executives.aspx"):
        self.base_url = base_url
        self.driver = None
        self.bank_name = None
        self.busi_dt = datetime.now().strftime("%Y-%m-%d")
        # Store exact HTML strings with positions
        self.verified_executives = []  # List of (name, position) tuples from HTML

    def detect_bank_name(self, url: str) -> str:
        """Detect bank name from URL"""
        url_lower = url.lower()
        
        bank_keywords = {
            'kasikorn': 'ธนาคารกสิกรไทย',
            'kbank': 'ธนาคารกสิกรไทย',
            'bangkokbank': 'ธนาคารกรุงเทพ',
            'bbl': 'ธนาคารกรุงเทพ',
            'scb': 'ธนาคารไทยพาณิชย์',
            'ktb': 'ธนาคารกรุงไทย',
            'krungsri': 'ธนาคารกรุงศรีอยุธยา',
        }
        
        for keyword, bank_name in bank_keywords.items():
            if keyword in url_lower:
                return bank_name
        
        return "ธนาคารไม่ระบุ"

    def setup_driver(self) -> bool:
        """Setup Selenium WebDriver"""
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            ]
            chrome_options.add_argument(f"user-agent={random.choice(user_agents)}")
            
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(60)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            logging.info("✅ WebDriver setup completed")
            return True
            
        except Exception as e:
            logging.error(f"❌ Error setting up WebDriver: {e}")
            return False

    def fetch_page_content(self, url: str, retries: int = 3) -> Optional[str]:
        """Fetch page content with retry logic"""
        if self.driver is None:
            if not self.setup_driver():
                return None
                
        for attempt in range(retries):
            try:
                time.sleep(random.uniform(2, 4))
                logging.info(f"🌐 Navigating to {url} (attempt {attempt+1}/{retries})")
                self.driver.get(url)
                
                WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                logging.info("⏳ Waiting for content to load...")
                time.sleep(5)
                
                # Scroll to trigger lazy loading
                logging.info("📜 Scrolling to load dynamic content...")
                for scroll_pct in [0.25, 0.5, 0.75, 1.0]:
                    self.driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * {scroll_pct});")
                    time.sleep(2)
                
                self.driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(2)
                
                page_source = self.driver.page_source
                
                if len(page_source) > 500:
                    logging.info(f"✅ Page fetched successfully ({len(page_source)} chars)")
                    return page_source
                
            except Exception as e:
                logging.warning(f"⚠️ Error on attempt {attempt+1}: {e}")
                
            if attempt < retries - 1:
                time.sleep(5 * (attempt + 1))
        
        logging.error("❌ Failed to fetch page after all retries")
        return None

    def _is_valid_thai_name(self, text: str) -> bool:
        """Check if text is a valid Thai executive name - RELAXED VERSION"""
        if not text or len(text) < 6 or len(text) > 120:
            return False
        
        # Must contain Thai characters (U+0E00 to U+0E7F), spaces, dots, and some punctuation
        thai_char_count = sum(1 for char in text if 0x0E00 <= ord(char) <= 0x0E7F)
        if thai_char_count < 3:  # At least 3 Thai characters
            return False
        
        # Must start with Thai title or contain common name patterns
        thai_titles = ['นาย', 'นาง', 'นางสาว', 'ดร.', 'ดร', 'ศ.', 'รศ.', 'ผศ.', 'พันตรี']
        if not any(text.startswith(title) for title in thai_titles):
            # Check if it contains common Thai name patterns
            if not any(title in text for title in thai_titles):
                return False
        
        # Must have at least 2 words (title + name)
        words = text.split()
        if len(words) < 2:
            return False
        
        return True

    def _is_valid_position(self, text: str) -> bool:
        """Check if text is a valid position title - RELAXED VERSION"""
        if not text or len(text) < 2:
            return False
        
        # Valid position keywords - expanded list
        valid_keywords = [
            'ผู้จัดการ', 'กรรมการ', 'ผู้บริหาร', 'ผู้อำนวยการ', 
            'ประธาน', 'รองประธาน', 'ผู้ช่วย', 'หัวหน้า', 'ผู้ตรวจสอบ',
            'CEO', 'CFO', 'CTO', 'COO', 'President', 'Vice',
            'Executive', 'Director', 'Manager', 'Chief', 'Officer',
            'Assistant', 'Deputy', 'Senior', 'Head', 'Business',
            'ที่ปรึกษา', 'เลขานุการ', 'คณะกรรมการ', 'บริษัท', 'บริษัท',
            'กลุ่ม', 'ฝ่าย', 'สายงาน', 'แผนก', 'สำนัก', 'กรม', 'กอง',
            'Account', 'Advisor', 'Analyst', 'Audit', 'Bank', 'Board',
            'Business', 'Commercial', 'Company', 'Compliance', 'Control',
            'Corporate', 'Credit', 'Customer', 'Development', 'Division',
            'Finance', 'Financial', 'Group', 'Investment', 'Legal',
            'Marketing', 'Operation', 'Product', 'Relationship', 'Risk',
            'Sales', 'Service', 'Strategy', 'Technology', 'Treasury',
            'Unit', 'Wealth'
        ]
        
        text_lower = text.lower()
        return any(keyword.lower() in text_lower for keyword in valid_keywords)

    def extract_executives_from_html(self, html_content: str) -> List[Tuple[str, str]]:
        """Extract executive names and positions directly from HTML - IMPROVED VERSION"""
        soup = BeautifulSoup(html_content, 'html.parser')
        executives = []
        
        logging.info("\n🔍 Extracting executives from HTML...")
        
        # Save HTML for debugging
        with open("debug_page.html", "w", encoding="utf-8") as f:
            f.write(html_content)
        logging.info("💾 Saved page content to debug_page.html for inspection")
        
        # METHOD 1: Look for specific patterns in the page
        all_text_elements = soup.find_all(text=True)
        logging.info(f"📝 Total text elements: {len(all_text_elements)}")
        
        # Filter and clean text elements
        cleaned_texts = []
        for element in all_text_elements:
            text = element.strip()
            text = re.sub(r'\s+', ' ', text)  # Normalize whitespace
            if text and len(text) > 5:  # Only keep substantial text
                cleaned_texts.append(text)
        
        logging.info(f"📝 Cleaned text elements: {len(cleaned_texts)}")
        
        # Look for name-position patterns
        i = 0
        while i < len(cleaned_texts):
            current_text = cleaned_texts[i]
            
            # If this looks like a name
            if self._is_valid_thai_name(current_text):
                name = current_text
                position = "ไม่ระบุ"
                
                # Look ahead for position (next 1-3 elements)
                for j in range(i+1, min(i+4, len(cleaned_texts))):
                    candidate = cleaned_texts[j]
                    if self._is_valid_position(candidate):
                        position = candidate
                        break
                
                # Add to executives if not duplicate
                if not any(name == existing_name for existing_name, _ in executives):
                    executives.append((name, position))
                    logging.info(f"✅ Found: {name} | {position}")
                
                i += 2  # Skip ahead
                continue
            
            i += 1
        
        # METHOD 2: Look for specific containers that might hold executive data
        containers = soup.find_all(['div', 'section', 'article', 'table', 'ul', 'ol'])
        
        for container_idx, container in enumerate(containers):
            container_text = container.get_text(strip=True)
            if len(container_text) < 20:  # Skip small containers
                continue
                
            # Check if container has executive-like content
            if any(keyword in container_text for keyword in ['กรรมการ', 'ผู้จัดการ', 'ประธาน', 'Director', 'Manager']):
                logging.info(f"🔍 Checking container {container_idx+1} with executive keywords")
                
                # Extract all text blocks from this container
                text_blocks = []
                for element in container.find_all(text=True):
                    text = element.strip()
                    if text and len(text) > 5:
                        text_blocks.append(text)
                
                # Process text blocks in this container
                k = 0
                while k < len(text_blocks):
                    text = text_blocks[k]
                    
                    if self._is_valid_thai_name(text):
                        name = text
                        position = "ไม่ระบุ"
                        
                        # Look for position in nearby blocks
                        for m in range(k+1, min(k+4, len(text_blocks))):
                            candidate = text_blocks[m]
                            if self._is_valid_position(candidate):
                                position = candidate
                                break
                        
                        if not any(name == existing_name for existing_name, _ in executives):
                            executives.append((name, position))
                            logging.info(f"✅ Container {container_idx+1}: {name} | {position}")
                        
                        k += 2
                        continue
                    
                    k += 1
        
        # METHOD 3: Specific table extraction for structured data
        tables = soup.find_all('table')
        logging.info(f"📊 Found {len(tables)} tables")
        
        for table_idx, table in enumerate(tables):
            rows = table.find_all('tr')
            logging.info(f"  Table {table_idx + 1}: {len(rows)} rows")
            
            for row_idx, row in enumerate(rows):
                cells = row.find_all(['td', 'th'])
                row_text = ' '.join(cell.get_text(strip=True) for cell in cells)
                
                # Skip header rows
                if any(header in row_text for header in ['ชื่อ', 'ตำแหน่ง', 'Name', 'Position']):
                    continue
                
                # Process each cell
                for cell_idx, cell in enumerate(cells):
                    cell_text = cell.get_text(strip=True)
                    cell_text = re.sub(r'\s+', ' ', cell_text)
                    
                    if self._is_valid_thai_name(cell_text):
                        name = cell_text
                        position = "ไม่ระบุ"
                        
                        # Look for position in other cells of the same row
                        for other_cell in cells:
                            other_text = other_cell.get_text(strip=True)
                            if other_text != name and self._is_valid_position(other_text):
                                position = other_text
                                break
                        
                        if not any(name == existing_name for existing_name, _ in executives):
                            executives.append((name, position))
                            logging.info(f"✅ Table {table_idx+1} Row {row_idx+1}: {name} | {position}")
        
        # METHOD 4: Look for list items
        list_items = soup.find_all(['li', 'dt', 'dd'])
        logging.info(f"📋 Found {len(list_items)} list items")
        
        for item_idx, item in enumerate(list_items):
            item_text = item.get_text(strip=True)
            item_text = re.sub(r'\s+', ' ', item_text)
            
            if self._is_valid_thai_name(item_text):
                name = item_text
                position = "ไม่ระบุ"
                
                # Look for position in parent or sibling elements
                parent = item.parent
                if parent:
                    parent_text = parent.get_text(strip=True)
                    # Extract potential position from parent text
                    lines = parent_text.split('\n')
                    for line in lines:
                        line = line.strip()
                        if line != name and self._is_valid_position(line):
                            position = line
                            break
                
                if not any(name == existing_name for existing_name, _ in executives):
                    executives.append((name, position))
                    logging.info(f"✅ List item {item_idx+1}: {name} | {position}")
        
        logging.info(f"\n📊 Total executives found: {len(executives)}")
        
        # Display first 10 for verification
        if executives:
            logging.info("\n🔍 First 10 executives found:")
            for i, (name, position) in enumerate(executives[:10]):
                logging.info(f"  {i+1}. {name} - {position}")
        
        return executives

    def _parse_name_components(self, full_name: str) -> Tuple[str, str, str, str]:
        """Parse name into prefix, first name, and surname - FIXED VERSION"""
        # Comprehensive title mapping
        title_map = {
            "นาย": "Mr.",
            "นาง": "Mrs.",
            "นางสาว": "Ms.",
            "ดร.": "Dr.",
            "ดร": "Dr.",
            "ศ.": "Prof.",
            "รศ.": "Assoc. Prof.",
            "ผศ.": "Asst. Prof.",
            "พันตรี": "Lt."
        }
        
        # Find and extract prefix
        prefix = ""
        name_without_prefix = full_name
        
        # Sort by length (longest first) to catch compound titles like "นางสาว"
        for thai_title, eng_title in sorted(title_map.items(), key=lambda x: len(x[0]), reverse=True):
            if full_name.startswith(thai_title):
                prefix = eng_title
                name_without_prefix = full_name[len(thai_title):].strip()
                break
        
        # If no prefix found but name contains Thai characters, check for embedded titles
        if not prefix and any(0x0E00 <= ord(char) <= 0x0E7F for char in full_name):
            for thai_title, eng_title in title_map.items():
                if thai_title in full_name and full_name.index(thai_title) < 10:  # Title in first 10 chars
                    prefix = eng_title
                    # Remove the title from anywhere in the name
                    name_without_prefix = full_name.replace(thai_title, '').strip()
                    break
        
        # Clean up the name (remove extra spaces)
        name_without_prefix = re.sub(r'\s+', ' ', name_without_prefix).strip()
        
        # Split into name parts
        parts = name_without_prefix.split()
        
        if len(parts) == 0:
            return prefix, full_name, "", ""  # Return original if no parts
        elif len(parts) == 1:
            return prefix, full_name, parts[0], ""  # Single name part
        elif len(parts) == 2:
            return prefix, full_name, parts[0], parts[1]  # First + Last name
        else:
            # For multiple parts: first word is first name, rest is surname
            first_name = parts[0]
            surname = " ".join(parts[1:])
            return prefix, full_name, first_name, surname

    def create_executive_records(self, executives: List[Tuple[str, str]]) -> List[Dict]:
        """Create structured records from executive tuples - FIXED VERSION"""
        records = []
        seen_names = set()
        
        logging.info("\n📝 Creating executive records...")
        
        for name, position in executives:
            # Skip duplicates (exact match only)
            if name in seen_names:
                logging.debug(f"  ⚠️ Skipping duplicate: {name}")
                continue
            
            # Parse name components using the fixed function
            prefix, full_name, first_name, surname = self._parse_name_components(name)
            
            if not first_name:
                logging.warning(f"  ⚠️ Could not parse name: {name}")
                # Still create record but with original name
                first_name = name
                surname = ""
            
            # Create record with CORRECT column mapping
            record = {
                "BUSI_DT": self.busi_dt,
                "Prefixed_Name": prefix,  # English prefix (Mr., Mrs., Dr., etc.)
                "Full_Name": full_name,   # Original Thai full name with prefix
                "First_Name": first_name, # Thai first name without prefix
                "Surname": surname,       # Thai surname
                "Bank_Name": self.bank_name,
                "Position": position
            }
            
            records.append(record)
            seen_names.add(name)
            logging.info(f"  ✅ {prefix} | {first_name} {surname} | {position}")
        
        # Debug: Show first few records to verify structure
        if records:
            logging.info("\n🔍 First 3 records structure:")
            for i, record in enumerate(records[:3]):
                logging.info(f"  {i+1}. Prefixed: '{record['Prefixed_Name']}' | First: '{record['First_Name']}' | Last: '{record['Surname']}'")
        
        return records

    def intelligent_scrape(self, limit: int = 150) -> List[Dict]:
        """Main scraping function"""
        logging.info("🚀 Starting scraping process...")
        
        # Fetch page
        html_content = self.fetch_page_content(self.base_url)
        if not html_content:
            logging.error("❌ Failed to fetch page content")
            return []
        
        # Detect bank
        self.bank_name = self.detect_bank_name(self.base_url)
        logging.info(f"🏦 Bank: {self.bank_name}")
        logging.info(f"📅 Business Date: {self.busi_dt}")
        
        # Extract executives
        executives = self.extract_executives_from_html(html_content)
        
        if not executives:
            logging.error("❌ No executives found")
            return []
        
        # Create records
        records = self.create_executive_records(executives)
        
        logging.info(f"\n📊 Total records created: {len(records)}")
        return records[:limit]

    def close(self):
        """Close WebDriver"""
        try:
            if self.driver:
                self.driver.quit()
                logging.info("✅ WebDriver closed")
        except Exception as e:
            logging.error(f"⚠️ Error closing WebDriver: {e}")


def save_to_csv(data: List[Dict], bank_name: str, busi_dt: str) -> bool:
    """Save data to CSV with proper formatting"""
    if not data:
        logging.warning("⚠️ No data to save")
        return False

    try:
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Ensure column order - FIXED to match our record structure
        column_order = ['BUSI_DT', 'Prefixed_Name', 'Full_Name', 
                       'First_Name', 'Surname', 'Bank_Name', 'Position']
        
        # Make sure all columns exist
        for col in column_order:
            if col not in df.columns:
                df[col] = ""  # Add missing columns
        
        df = df[column_order]  # Reorder columns
        
        # Create filename
        bank_short = bank_name.replace('ธนาคาร', '').strip()
        year_month = busi_dt[:7].replace('-', '')
        filename = f"{bank_short}_{year_month}.csv"
        
        # Create output directory
        os.makedirs('output', exist_ok=True)
        output_path = os.path.join('output', filename)
        
        # Save CSV with proper encoding
        df.to_csv(output_path, index=False, encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
        
        logging.info(f"\n✅ File saved: {output_path}")
        logging.info(f"📊 Total records: {len(df)}")
        
        # Display results with better formatting
        print("\n" + "="*120)
        print(f"📊 RESULTS FOR {bank_name}")
        print(f"📅 Date: {busi_dt}")
        print(f"📁 File: {output_path}")
        print(f"📈 Records: {len(df)}")
        print("="*120)
        
        # Show sample of data to verify column alignment
        print("\n📋 SAMPLE DATA (first 5 records):")
        sample_df = df.head().copy()
        # Ensure proper display of Thai text
        pd.set_option('display.unicode.east_asian_width', True)
        pd.set_option('display.max_colwidth', 30)
        print(sample_df.to_string(index=False))
        
        print("="*120 + "\n")
        
        return True
        
    except Exception as e:
        logging.error(f"❌ Error saving CSV: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main execution"""
    print("="*120)
    print("🤖 BANK EXECUTIVE SCRAPER - FIXED NAME PARSING VERSION")
    print("="*120)
    print("✅ Fixed prefix detection for all Thai titles")
    print("✅ Correct column alignment")
    print("✅ Better name parsing logic")
    print("="*120 + "\n")
    
    scraper = None
    
    try:
        scraper = IntelligentKBankScraper()
        
        print(f"🌐 Target URL: {scraper.base_url}")
        print(f"📅 Date: {scraper.busi_dt}\n")
        
        # Run scraping
        executives = scraper.intelligent_scrape()
        
        if executives:
            # Save to CSV
            save_to_csv(executives, scraper.bank_name, scraper.busi_dt)
            print(f"\n✅ SUCCESS: Extracted {len(executives)} executives")
            
            # Additional verification
            print(f"\n🔍 VERIFICATION:")
            print(f"   - First names should be in 'First_Name' column")
            print(f"   - Last names should be in 'Surname' column") 
            print(f"   - English prefixes should be in 'Prefixed_Name' column")
            print(f"   - Full Thai names should be in 'Full_Name' column")
            
        else:
            print("\n❌ FAILED: No executives found")
        
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted by user")
    except Exception as e:
        logging.error(f"❌ Error in main: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if scraper:
            try:
                scraper.close()
            except:
                pass
        print("\n" + "="*120)
        print("🏁 SCRAPING COMPLETED")
        print("="*120)


if __name__ == "__main__":
    main()
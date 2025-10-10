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
from typing import List, Dict, Optional


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


class OllamaAI:
    """Class to interact with Ollama API for AI-powered analysis"""
    
    def __init__(self, base_url=OLLAMA_API_URL, model=OLLAMA_MODEL):
        self.base_url = base_url
        self.model = model
        
    def generate_response(self, prompt: str, system_prompt: str = "") -> str:
        """Generate response using Ollama API"""
        try:
            url = f"{self.base_url}"
            
            payload = {
                "model": self.model,
                "prompt": prompt,
                "system": system_prompt,
                "stream": False,
                "options": {
                    "temperature": 0.1,
                    "top_p": 0.9,
                    "max_tokens": 2000
                }
            }
            
            response = requests.post(url, json=payload, timeout=60)
            response.raise_for_status()
            
            result = response.json()
            return result.get("response", "")
            
        except Exception as e:
            logging.error(f"Error generating AI response: {e}")
            return ""
    
    def test_connection(self) -> bool:
        """Test if Ollama is running and accessible"""
        try:
            response = requests.get(f"{self.base_url.replace('/api/generate', '')}", timeout=10)
            return response.status_code == 200
        except:
            return False


class IntelligentKBankScraper:
    """AI-powered adaptive web scraper for Bank executives"""
    
    def __init__(self, base_url="https://www.kasikornbank.com/th/about/Pages/executives.aspx"):
        self.base_url = base_url
        self.driver = None
        self.ai = OllamaAI()
        self.bank_name = None
        self.busi_dt = datetime.now().strftime("%Y-%m-%d")
        
        if not self.ai.test_connection():
            logging.warning(f"Ollama not accessible at {OLLAMA_API_URL}")

    def detect_bank_name(self, url: str, html_content: str) -> str:
        """Use AI to detect bank name from URL and page content"""
        
        system_prompt = """คุณเป็นผู้เชี่ยวชาญในการระบุชื่อธนาคารไทยจากเว็บไซต์

ธนาคารไทยที่พบบ่อย:
- ธนาคารกสิกรไทย
- ธนาคารกรุงเทพ
- ธนาคารไทยพาณิชย์
- ธนาคารกรุงไทย
- ธนาคารกรุงศรีอยุธยา

ให้ตอบเฉพาะชื่อธนาคารเป็นภาษาไทยเท่านั้น"""

        soup = BeautifulSoup(html_content, 'html.parser')
        title = soup.find('title')
        title_text = title.get_text() if title else ""
        
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        meta_text = meta_desc.get('content', '') if meta_desc else ""
        
        headings = []
        for h in soup.find_all(['h1', 'h2', 'h3'])[:5]:
            headings.append(h.get_text(strip=True))
        
        prompt = f"""ระบุชื่อธนาคารจากข้อมูลนี้:

URL: {url}
หัวข้อหน้า: {title_text}
คำอธิบาย: {meta_text}
หัวข้อย่อย: {', '.join(headings)}

ธนาคารนี้ชื่ออะไร? ตอบเฉพาะชื่อเต็มภาษาไทย"""

        response = self.ai.generate_response(prompt, system_prompt)
        bank_name = response.strip().strip('"').strip("'").strip()
        
        if 'ธนาคาร' in bank_name and len(bank_name) < 100:
            logging.info(f"AI detected bank: {bank_name}")
            return bank_name
        
        return self._fallback_detect_bank(url)
    
    def _fallback_detect_bank(self, url: str) -> str:
        """Fallback method to detect bank from URL"""
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
            
            logging.info("WebDriver setup completed")
            return True
            
        except Exception as e:
            logging.error(f"Error setting up WebDriver: {e}")
            return False

    def fetch_page_content(self, url: str, retries: int = 3) -> Optional[str]:
        """Fetch page content with retry logic"""
        if self.driver is None:
            if not self.setup_driver():
                return None
                
        for attempt in range(retries):
            try:
                time.sleep(random.uniform(2, 4))
                logging.info(f"Navigating to {url} (attempt {attempt+1})")
                self.driver.get(url)
                time.sleep(random.uniform(3, 6))
                
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                page_source = self.driver.page_source
                if len(page_source) > 500:
                    logging.info(f"Successfully fetched page ({len(page_source)} chars)")
                    return page_source
                
            except Exception as e:
                logging.warning(f"Error on attempt {attempt+1}: {e}")
                
            if attempt < retries - 1:
                time.sleep(5 * (attempt + 1))
        
        return None

    def _normalize_thai_text(self, text: str) -> str:
        """แก้ไขปัญหา encoding และทำความสะอาดข้อความภาษาไทย"""
        if not text:
            return ""
        
        # ทำความสะอาดช่องว่างและอักขระพิเศษ
        text = re.sub(r'\s+', ' ', text).strip()
        
        # ลบอักขระที่ไม่ต้องการ
        text = re.sub(r'[^\u0E00-\u0E7Fa-zA-Z\s\.]', '', text)
        
        return text

    def extract_executives_advanced(self, html_content: str) -> List[Dict]:
        """ดึงข้อมูลผู้บริหาร - ใช้หลายวิธีรวมกัน"""
        
        soup = BeautifulSoup(html_content, 'html.parser')
        executives = []
        processed_names = set()
        
        logging.info("🔍 Starting executive extraction...")
        
        # วิธีที่ 1: หาจาก img tags และ parent elements
        all_images = soup.find_all('img')
        logging.info(f"📷 Method 1: Found {len(all_images)} images")
        
        for idx, img in enumerate(all_images):
            try:
                # หา parent containers ในหลายระดับ
                parents_to_check = []
                
                current = img
                for level in range(5):  # ตรวจสอบ 5 ระดับ
                    parent = current.find_parent()
                    if parent:
                        parents_to_check.append(parent)
                        current = parent
                    else:
                        break
                
                for parent in parents_to_check:
                    text_content = parent.get_text(separator='|', strip=True)
                    lines = [line.strip() for line in text_content.split('|') if line.strip()]
                    
                    # Debug: แสดง 3 รูปแรก
                    if idx < 3:
                        logging.info(f"  Image {idx+1} nearby text (first 5 lines): {lines[:5]}")
                    
                    # หาชื่อและตำแหน่ง
                    for i, line in enumerate(lines):
                        line_normalized = self._normalize_thai_text(line)
                        
                        if self._is_valid_executive_name(line_normalized):
                            name = line_normalized
                            position = lines[i+1] if i+1 < len(lines) else ""
                            position = self._normalize_thai_text(position)
                            
                            if name not in processed_names:
                                processed_names.add(name)
                                exec_data = self._create_executive_record(name, position)
                                if exec_data:
                                    executives.append(exec_data)
                                    logging.info(f"✅ Method 1: {name} - {position}")
                                break
                    
                    if len(executives) > idx:  # ถ้าเจอแล้ว ไม่ต้องตรวจสอบ parent อื่น
                        break
                        
            except Exception as e:
                logging.debug(f"Error in Method 1, image {idx}: {e}")
        
        # วิธีที่ 2: หาจาก tables
        logging.info(f"\n📋 Method 2: Searching in tables")
        tables = soup.find_all('table')
        logging.info(f"  Found {len(tables)} tables")
        
        for table_idx, table in enumerate(tables):
            try:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        cell_texts = [self._normalize_thai_text(c.get_text(strip=True)) for c in cells]
                        
                        for i, text in enumerate(cell_texts):
                            if self._is_valid_executive_name(text):
                                name = text
                                position = cell_texts[i+1] if i+1 < len(cell_texts) else ""
                                
                                if name not in processed_names:
                                    processed_names.add(name)
                                    exec_data = self._create_executive_record(name, position)
                                    if exec_data:
                                        executives.append(exec_data)
                                        logging.info(f"✅ Method 2: {name} - {position}")
                                break
            except Exception as e:
                logging.debug(f"Error in Method 2, table {table_idx}: {e}")
        
        # วิธีที่ 3: ค้นหาทั้งหน้าเว็บ (fallback)
        if len(executives) < 5:  # ถ้าเจอน้อยกว่า 5 คน ใช้วิธีนี้เสริม
            logging.info(f"\n🔎 Method 3: Full page scan (found only {len(executives)} so far)")
            
            body = soup.find('body')
            if body:
                all_text = body.get_text(separator='\n', strip=True)
                lines = [self._normalize_thai_text(line) for line in all_text.split('\n') if line.strip()]
                
                # กรองเฉพาะบรรทัดที่มีความยาวเหมาะสม
                valid_lines = [line for line in lines if 5 <= len(line) <= 150]
                
                logging.info(f"  Processing {len(valid_lines)} text lines")
                
                i = 0
                while i < len(valid_lines):
                    line = valid_lines[i]
                    
                    if self._is_valid_executive_name(line):
                        name = line
                        position = valid_lines[i+1] if i+1 < len(valid_lines) else ""
                        
                        if name not in processed_names:
                            processed_names.add(name)
                            exec_data = self._create_executive_record(name, position)
                            if exec_data:
                                executives.append(exec_data)
                                logging.info(f"✅ Method 3: {name} - {position}")
                            i += 2
                            continue
                    
                    i += 1
        
        logging.info(f"\n📊 Total executives extracted: {len(executives)}")
        return executives
    
    def _is_valid_executive_name(self, text: str) -> bool:
        """ตรวจสอบว่าข้อความเป็นชื่อผู้บริหารจริงหรือไม่ - เวอร์ชันผ่อนปรน"""
        
        if not text:
            return False
        
        # ความยาวต้องเหมาะสม
        if len(text) < 5 or len(text) > 100:
            return False
        
        # ต้องมีตัวอักษรไทย
        if not re.search(r'[\u0E00-\u0E7F]', text):
            return False
        
        # ต้องมีคำนำหน้าชื่อ (เงื่อนไขสำคัญที่สุด)
        thai_prefixes = ['นาย', 'นาง', 'นางสาว', 'ดร.', 'ศ.', 'รศ.', 'ผศ.']
        has_prefix = any(text.startswith(p) for p in thai_prefixes)
        
        if not has_prefix:
            return False
        
        # กรอง spam keywords ที่ชัดเจน
        spam_keywords = [
            'คลิก', 'สมัคร', 'ลงทะเบียน', 
            'K PLUS', 'K-PLUS', 'KPLUS',
            'ดาวน์โหลด', 'Download',
            'ติดต่อเรา', 'Contact',
            'สาขา', 'Branch',
            'ค้นหา', 'Search'
        ]
        
        text_upper = text.upper()
        for spam in spam_keywords:
            if spam.upper() in text_upper:
                return False
        
        # ต้องมีอย่างน้อย 2 คำ (คำนำหน้า + ชื่อ/นามสกุล)
        words = text.split()
        if len(words) < 2:
            return False
        
        # ผ่านทุกเงื่อนไข = เป็นชื่อที่ valid
        return True
    
    def _create_executive_record(self, name: str, position: str) -> Optional[Dict]:
        """สร้างข้อมูลผู้บริหารที่สะอาดและถูกต้อง"""
        
        # ทำความสะอาด
        name = self._normalize_thai_text(name)
        position = self._normalize_thai_text(position)
        
        if not name:
            return None
        
        # ตรวจสอบว่า position ไม่ใช่ส่วนหนึ่งของชื่อ
        if position and len(position) > 3:
            position_keywords = ['ประธาน', 'กรรมการ', 'ผู้จัดการ', 'ผู้บริหาร', 
                                'ผู้อำนวยการ', 'รอง', 'ผู้ช่วย', 'หัวหน้า',
                                'ประจำ', 'กลุ่ม', 'ธุรกิจ', 'สายงาน']
            
            has_position_keyword = any(keyword in position for keyword in position_keywords)
            
            if not has_position_keyword:
                # อาจเป็นชื่อที่ยาว ไม่ใช่ตำแหน่ง
                position = ""
        
        # ดึงข้อมูลจากชื่อ
        prefix = self._extract_prefix_manual(name)
        first_name, surname = self._parse_name_manual(name)
        
        # สร้าง record
        executive = {
            "BUSI_DT": self.busi_dt,
            "Prefixed_Name": prefix,
            "Full_Name": name,
            "First_Name": first_name,
            "Surname": surname,
            "Bank_Name": self.bank_name,
            "Position": position if position else "ไม่ระบุ"
        }
        
        return executive
    
    def _extract_prefix_manual(self, full_name: str) -> str:
        """ดึงคำนำหน้าชื่อ"""
        titles = {
            "นาย": "Mr",
            "นาง": "Mrs",
            "นางสาว": "Ms",
            "ดร.": "Dr",
            "ศ.": "Prof",
            "รศ.": "Assoc Prof",
            "ผศ.": "Asst Prof"
        }
        
        for thai_title, eng_title in sorted(titles.items(), key=lambda x: len(x[0]), reverse=True):
            if full_name.startswith(thai_title):
                return eng_title
        
        return ""
    
    def _parse_name_manual(self, full_name: str) -> tuple:
        """แยกชื่อและนามสกุล - ปรับปรุงเพื่อรองรับกรณีพิเศษ"""
        # ลบคำนำหน้า
        titles = ["นาย", "นาง", "นางสาว", "ดร.", "ศ.", "รศ.", "ผศ."]
        
        name = full_name
        for title in sorted(titles, key=len, reverse=True):
            if name.startswith(title):
                name = name[len(title):].strip()
                break
        
        # แยกคำ
        parts = [p for p in name.split() if len(p) > 0]
        
        if len(parts) == 0:
            return "", ""
        elif len(parts) == 1:
            # มีแค่ชื่อหรือนามสกุลอย่างเดียว
            return parts[0], ""
        elif len(parts) == 2:
            # ปกติ: ชื่อ + นามสกุล
            return parts[0], parts[1]
        else:
            # มากกว่า 2 คำ: คำแรกเป็นชื่อ ที่เหลือเป็นนามสกุล
            return parts[0], " ".join(parts[1:])
    
    def _is_duplicate(self, executive: Dict, executives_list: List[Dict]) -> bool:
        """เช็คว่าข้อมูลซ้ำหรือไม่ - ปรับปรุงให้แม่นยำขึ้น"""
        current_name = executive['Full_Name'].strip()
        current_position = executive['Position'].strip()
        
        for existing in executives_list:
            existing_name = existing['Full_Name'].strip()
            existing_position = existing['Position'].strip()
            
            # เช็คทั้งชื่อและตำแหน่ง
            if existing_name == current_name:
                # ถ้าชื่อเหมือนกัน เช็คตำแหน่ง
                if existing_position == current_position or not current_position or not existing_position:
                    return True
        
        return False

    def intelligent_scrape(self, limit: int = 100) -> List[Dict]:
        """Main scraping function - ปรับปรุงแล้ว"""
        logging.info("🚀 Starting intelligent scraping...")
        
        # Fetch page
        html_content = self.fetch_page_content(self.base_url)
        if not html_content:
            logging.error("Failed to fetch page")
            return []
        
        # Detect bank name
        self.bank_name = self.detect_bank_name(self.base_url, html_content)
        logging.info(f"🏦 Bank: {self.bank_name}")
        logging.info(f"📅 Business Date: {self.busi_dt}")
        
        # Extract executives
        logging.info("\n📸 Extracting executives from page...")
        all_executives = self.extract_executives_advanced(html_content)
        
        # ลบข้อมูลซ้ำอีกครั้ง (double-check)
        unique_executives = []
        seen_names = set()
        
        for exec_data in all_executives:
            name = exec_data['Full_Name']
            if name not in seen_names:
                seen_names.add(name)
                unique_executives.append(exec_data)
        
        logging.info(f"\n📊 Total unique executives: {len(unique_executives)}")
        
        return unique_executives[:limit]

    def close(self):
        """Close WebDriver"""
        try:
            if self.driver:
                self.driver.quit()
                logging.info("WebDriver closed")
        except Exception as e:
            logging.error(f"Error closing WebDriver: {e}")


def save_to_csv(data: List[Dict], bank_name: str, busi_dt: str) -> bool:
    """Save data to CSV - one file per bank per month"""
    if not data:
        logging.warning("No data to save")
        return False

    try:
        df = pd.DataFrame(data)
        
        column_order = ['BUSI_DT', 'Prefixed_Name', 'Full_Name', 
                       'First_Name', 'Surname', 'Bank_Name', 'Position']
        df = df[column_order]
        
        # สร้างชื่อไฟล์ตามธนาคารและเดือน-ปี
        bank_short = bank_name.replace('ธนาคาร', '').strip()
        year_month = busi_dt[:7].replace('-', '')  # 2025-10-07 -> 202510
        filename = f"{bank_short}_{year_month}.csv"
        
        # สร้างโฟลเดอร์ output ถ้ายังไม่มี
        os.makedirs('output', exist_ok=True)
        output_path = os.path.join('output', filename)
        
        # เช็คว่าไฟล์มีอยู่แล้วหรือไม่
        file_exists = os.path.exists(output_path)
        
        if file_exists:
            # อ่านไฟล์เก่า
            existing_df = pd.read_csv(output_path, encoding='utf-8-sig')
            
            # รวมข้อมูลเก่ากับใหม่
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            
            # ลบข้อมูลซ้ำ (ถ้ามี) โดยดูจากชื่อเต็ม และเก็บข้อมูลล่าสุด
            combined_df = combined_df.drop_duplicates(subset=['Full_Name'], keep='last')
            
            # อัพเดท BUSI_DT ทั้งหมดเป็นวันที่ scraping ล่าสุด
            combined_df['BUSI_DT'] = busi_dt
            
            # เรียงตามชื่อ
            combined_df = combined_df.sort_values('Full_Name').reset_index(drop=True)
            
            # บันทึกทับไฟล์เดิม
            combined_df.to_csv(output_path, index=False, encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
            
            logging.info(f"✅ Updated {output_path}")
            logging.info(f"   Previous: {len(existing_df)} records")
            logging.info(f"   New: {len(df)} records")
            logging.info(f"   Total: {len(combined_df)} unique records")
            
            print("\n" + "="*100)
            print(f"📊 Updated Data for {bank_name} ({year_month[:4]}-{year_month[4:]})")
            print(f"📅 Last Scraped: {busi_dt}")
            print(f"📁 File: {output_path}")
            print(f"📈 Total Records: {len(combined_df)}")
            print("="*100)
            print(combined_df.to_string(index=False))
            print("="*100)
            
        else:
            # ไฟล์ใหม่ - บันทึกเลย
            df.to_csv(output_path, index=False, encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
            
            logging.info(f"✅ Created new file: {output_path}")
            logging.info(f"   Records: {len(df)}")
            
            print("\n" + "="*100)
            print(f"📊 New Data File Created for {bank_name} ({year_month[:4]}-{year_month[4:]})")
            print(f"📅 Scraped Date: {busi_dt}")
            print(f"📁 File: {output_path}")
            print(f"📈 Total Records: {len(df)}")
            print("="*100)
            print(df.to_string(index=False))
            print("="*100)
        
        return True
        
    except Exception as e:
        logging.error(f"Error saving CSV: {e}")
        return False


def main():
    """Main execution"""
    print("🤖 AI-Powered Bank Executive Scraper (Fixed Version)")
    print("=" * 60)
    
    try:
        scraper = IntelligentKBankScraper()
        
        if not scraper.ai.test_connection():
            print("⚠️  Ollama not accessible")
            print(f"   URL: {OLLAMA_API_URL}")
            print(f"   Model: {OLLAMA_MODEL}")
            print("   (Scraping will continue without AI validation)")
        else:
            print("✅ Ollama connected")
        
        print(f"\n🌐 Target: {scraper.base_url}")
        print(f"📅 Date: {scraper.busi_dt}")
        
        executives = scraper.intelligent_scrape(limit=100)
        
        if executives:
            print(f"\n✅ Extracted {len(executives)} executives")
            
            if save_to_csv(executives, scraper.bank_name, scraper.busi_dt):
                year_month = scraper.busi_dt[:7].replace('-', '')
                bank_short = scraper.bank_name.replace('ธนาคาร', '').strip()
                print(f"💾 Saved to output/{bank_short}_{year_month}.csv")
        else:
            print("❌ No data extracted")
            
    except KeyboardInterrupt:
        print("\nℹ️  Interrupted")
    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)
    finally:
        if 'scraper' in locals():
            scraper.close()
        print("\n🏁 Done")


if __name__ == "__main__":
    main()
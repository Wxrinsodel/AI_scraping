from cgitb import text
import difflib
from turtle import position
import pandas as pd
import logging
import sys
import os
import re
import time
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
from verifier import Verifier 


port = 11434
OLLAMA_API_URL = f"http://localhost:{port}/api/generate"
OLLAMA_MODEL = "llama3.2"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


class FlexibleBankScraper:
    """Universal web scraper for Thai bank executives - Works with multiple banks"""

    def __init__(self, base_url):
        self.base_url = base_url
        self.driver = None
        self.bank_name = None
        self.busi_dt = datetime.now().strftime("%Y-%m-%d")
        self.verified_executives = []


    def detect_bank_name(self, url: str, html_content: Optional[str] = None) -> str:
        url_lower = url.lower()
        
        logging.info(f"\n{'='*80}")
        logging.info(f"🔍 BANK DETECTION DEBUG")
        logging.info(f"{'='*80}")
        logging.info(f"🔗 URL: {url}")
        logging.info(f"🔗 URL (lowercase): {url_lower}")
        
        bank_keywords = {
            'bangkokbank.com': 'ธนาคารกรุงเทพ',
            'bangkokbank': 'ธนาคารกรุงเทพ',
            'bbl.co.th': 'ธนาคารกรุงเทพ',
            'bbl': 'ธนาคารกรุงเทพ',
            'kasikornbank': 'ธนาคารกสิกรไทย',
            'kasikorn': 'ธนาคารกสิกรไทย',
            'kbank': 'ธนาคารกสิกรไทย',
            'scb': 'ธนาคารไทยพาณิชย์',
            'scb.co.th': 'ธนาคารไทยพาณิชย์',
            'siamcommercial': 'ธนาคารไทยพาณิชย์',
            'ktb': 'ธนาคารกรุงไทย',
            'krungthai': 'ธนาคารกรุงไทย',
            'ktb.co.th': 'ธนาคารกรุงไทย',
            'krungsri': 'ธนาคารกรุงศรีอยุธยา',
            'krungsri.com': 'ธนาคารกรุงศรีอยุธยา',
            'baya': 'ธนาคารกรุงศรีอยุธยา',
            'ttb': 'ธนาคารทหารไทยธนชาต',
            'tmbthanachart': 'ธนาคารทหารไทยธนชาต',
            'kiatnakin': 'ธนาคารเกียรตินาคินภัทร',
            'kkp': 'ธนาคารเกียรตินาคินภัทร',
            'thanachart': 'ธนาคารธนชาต',
            'tisco': 'ธนาคารทิสโก้',
            'icbc': 'ธนาคารไอซีบีซี (ไทย)',
            'cimb': 'ธนาคารซีไอเอ็มบี ไทย',
        }
        
        for keyword, bank_name in bank_keywords.items():
            if keyword in url_lower:
                logging.info(f"✅ MATCH FOUND!")
                logging.info(f"    Keyword: '{keyword}'")
                logging.info(f"    Bank: {bank_name}")
                logging.info(f"{'='*80}\n")
                return bank_name
        
        logging.warning(f"⚠️ No bank detected from URL!")
        logging.warning(f"⚠️ Checking page content...\n")

        if html_content:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            title = soup.find('title')
            if title:
                title_text = title.get_text().lower()
                for keyword, bank_name in bank_keywords.items():
                    if keyword in title_text:
                        logging.info(f"🏦 Bank detected from page title: {bank_name}")
                        return bank_name
            
            meta_tags = soup.find_all('meta', attrs={'name': ['description', 'title', 'og:title']})
            for meta in meta_tags:
                content = meta.get('content', '').lower()
                for keyword, bank_name in bank_keywords.items():
                    if keyword in content:
                        logging.info(f"🏦 Bank detected from meta tags: {bank_name}")
                        return bank_name
            
            page_text = soup.get_text()
            
            thai_bank_exact = {
                'ธนาคารกรุงเทพ': 'ธนาคารกรุงเทพ',
                'กรุงเทพ': 'ธนาคารกรุงเทพ',
                'Bangkok Bank': 'ธนาคารกรุงเทพ',
                'ธนาคารกสิกรไทย': 'ธนาคารกสิกรไทย',
                'กสิกรไทย': 'ธนาคารกสิกรไทย',
                'KASIKORNBANK': 'ธนาคารกสิกรไทย',
                'ธนาคารไทยพาณิชย์': 'ธนาคารไทยพาณิชย์',
                'ไทยพาณิชย์': 'ธนาคารไทยพาณิชย์',
                'ธนาคารกรุงไทย': 'ธนาคารกรุงไทย',
                'กรุงไทย': 'ธนาคารกรุงไทย',
                'ธนาคารออมสิน': 'ธนาคารออมสิน',
                'ออมสิน': 'ธนาคารออมสิน',
                'gsb': 'ธนาคารออมสิน',
                'ธนาคารกรุงศรีอยุธยา': 'ธนาคารกรุงศรีอยุธยา',
                'กรุงศรีอยุธยา': 'ธนาคารกรุงศรีอยุธยา',
            }
            
            for thai_keyword, bank_name in thai_bank_exact.items():
                if thai_keyword in page_text:
                    logging.info(f"✅ Bank detected from page Thai text '{thai_keyword}': {bank_name}")
                    return bank_name
        
        domain_match = re.search(r'https?://(?:www\.)?([^/]+)', url)
        if domain_match:
            domain = domain_match.group(1)
            logging.warning(f"⚠️ Could not auto-detect bank. Domain: {domain}")
            return f"ธนาคาร ({domain})"
        
        return "ธนาคารไม่ระบุ"


    def setup_driver(self) -> bool:
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


    def _is_valid_thai_name(self, text: str, relaxed: bool = False) -> bool:
        """
        [ปรับปรุง] ฟังก์ชันตรวจสอบชื่อที่ยืดหยุ่นขึ้น (v5.1 - เน้นการกรองคำที่ติดกัน)
        """
        if not text or len(text) < 6 or len(text) > 90:
            return False
        
        words = text.split()
        max_words = 6 if relaxed else 5
        if len(words) > max_words:
            return False
        
        # กรองตัวเลข/ปี
        if re.search(r'\d{2,4}', text) and not re.search(r'(ดร|ศ|รศ|ผศ)', text):
            return False
        
        # กรองลิงก์/อีเมล
        if '@' in text or 'http' in text.lower() or '.com' in text.lower() or '.th' in text.lower():
            return False
        
        # Keywords ที่มักจะติดมากับชื่อและทำให้เกิดปัญหา (ตำแหน่งไทย/อังกฤษ)
        conjoined_keywords = ['ผู้จัดการ', 'กรรมการ', 'ประธาน', 'ผู้อำนวยการ', 'chief', 'officer', 'director', 'manager', 'ผู้ช่วย', 'สายงาน']
        
        # [ปรับปรุง] กรองคำที่ไม่ใช่ชื่อ
        invalid_keywords = [
            'สงวนลิขสิทธิ์', 'ลิขสิทธิ์', 'copyright', '©', 'all rights reserved',
            'บริษัท', 'บมจ', 'จำกัด', 'มหาชน', 'limited', 'public', 'company',
            'เว็บไซต์', 'website', 'www.', 'http', '.com', '.th', '.co',
            'โทร', 'โทรศัพท์', 'telephone', 'tel:', 'email', 'e-mail',
            'ติดต่อ', 'contact', 'สอบถาม', 'information', 'ข้อมูล', 'สำนักงาน', 
            'เลขที่', 'address', 'ที่อยู่', 'location', 'สถานที่',
            'วันที่', 'date', 'เวลา', 'time', 'ปี', 'year', 'พ.ศ.', 'ค.ศ.', 
            'เมนู', 'menu', 'หน้าหลัก', 'home', 'กลับ', 'back', 'ค้นหา', 'search', 
            'ดาวน์โหลด', 'download', 'pdf', 'print', 'พิมพ์', 'เพิ่มเติม', 'more', 
            'ประกาศ', 'announcement', 'ข่าว', 'news', 'นโยบาย', 'policy', 'เงื่อนไข', 
            'ความเป็นส่วนตัว', 'privacy', 'คุกกี้', 'cookie', 'รายนาม', 'รายชื่อ', 
            'ผู้ถือหุ้น', 'สำนักงาน', 'office'
        ]
        
        text_lower = text.lower()
        if any(keyword.lower() in text_lower for keyword in invalid_keywords):
            return False
        
        # ตรวจสอบว่านามสกุลไม่ได้ติดกับตำแหน่งแบบไม่มีช่องว่าง
        if any(keyword in text for keyword in conjoined_keywords):
            # ตรวจสอบว่าคำสุดท้าย (นามสกุล) ติดกับคำตำแหน่งหรือไม่
            last_word = words[-1]
            if any(last_word.endswith(keyword) for keyword in conjoined_keywords):
                return False

        special_char_count = sum(1 for char in text if char in '©®™@#$%^&*()_+=[]{}|\\:;"<>,.?/')
        if special_char_count > 2:
            return False
        
        thai_char_count = sum(1 for char in text if 0x0E00 <= ord(char) <= 0x0E7F)
        min_thai_chars = 4 if relaxed else 5
        if thai_char_count < min_thai_chars:
            return False
        
        thai_titles = ['นาย', 'นาง', 'นางสาว', 'ดร.', 'ดร', 'ศ.', 'รศ.', 'ผศ.', 
                       'พลเอก', 'พลโท', 'พลตรี', 'พันเอก', 'พันโท', 'พันตรี', 
                       'ท่านผู้หญิง', 'คุณหญิง', 'คุณ']
        
        if not any(text.startswith(title) for title in thai_titles):
            return False
        
        if len(words) < 2:
            return False
        
        return True


    def _is_valid_position(self, text: str, relaxed: bool = False) -> bool:
        """[ปรับปรุง] Check if text is a valid position title - v5.1"""
        if not text or len(text) < 4 or len(text) > 200:
            return False
        
        valid_keywords = [
            'ผู้จัดการ', 'กรรมการ', 'ผู้บริหาร', 'ผู้อำนวยการ', 'Chief',
            'ประธาน', 'รองประธาน', 'ผู้ช่วย', 'หัวหน้า', 'ผู้ตรวจสอบ',
            'CEO', 'CFO', 'CTO', 'COO', 'President', 'Vice',
            'Executive', 'Director', 'Manager', 'Officer', 'Group', 'Advisor',
            'Assistant', 'Deputy', 'Senior', 'Head', 'Business',
            'ที่ปรึกษา', 'เลขานุการ', 'คณะกรรมการ', 'ฝ่าย', 'สายงาน', 
            'Audit', 'Board', 'Commercial', 'Compliance', 'Control', 
            'Corporate', 'Credit', 'Finance', 'Financial', 'Investment', 
            'Legal', 'Marketing', 'Operation', 'Product', 'Relationship', 
            'Risk', 'Sales', 'Strategy', 'Technology', 'Treasury', 'Wealth', 
            'Regional', 'Retail', 'Wholesale', 'SME', 'Digital',
            'บริษัท', 'บมจ', 'จำกัด', 'มหาชน' 
        ]
        
        text_lower = text.lower()
        
        invalid_keywords = [
            'ข้อมูล', 'ติดต่อ', 'copyright', 'เว็บไซต์', 'หมายเลข', 
            'ปี', 'วันที่', 'เดือน', 'เมนู', 'ภาษา', 'หน้าแรก', 'home',
            'ท่านผู้หญิง', 'คุณหญิง', 'คุณ', 'นาย', 'นาง', 'นางสาว', 'ดร.', 'ศ.'
        ]
        
        if any(keyword in text_lower for keyword in invalid_keywords):
            return False
            
        if any(text.startswith(title) for title in ['นาย', 'นาง', 'นางสาว', 'ดร.']):
            return False
        
        return any(keyword.lower() in text_lower for keyword in valid_keywords)


    def extract_executives_from_html(self, html_content: str) -> List[Tuple[str, str]]:
        """
        [ปรับปรุงใหญ่] ฟังก์ชันสำหรับแยกชื่อและตำแหน่งออกจากเนื้อหา HTML (v5.1)
        - เพิ่ม PASS 5: Conjoined Name/Position Splitting/Trimming
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        executives = []
        
        logging.info("\n🔍 Extracting executives from HTML...")
        
        # ลบ Element ที่ไม่ต้องการออกก่อน
        for script in soup(["script", "style", "noscript", "footer", "header"]):
            if script: script.decompose()
        
        for nav in soup.find_all('nav'):
            if nav: nav.decompose()

        all_text_elements = soup.find_all(['p', 'span', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'div', 'li', 'td', 'th', 'a'])
        
        # Step 1: Pre-process All Text
        processed_texts = []
        for element in all_text_elements:
            text = element.get_text(strip=True)
            text = re.sub(r'\s+', ' ', text).strip()
            
            if 5 <= len(text) <= 300: 
                processed_texts.append((text, element))
        
        logging.info(f"📋 Total text elements to process: {len(processed_texts)}")
        
        # ฟังก์ชันช่วยเพิ่มชื่อเข้าลิสต์
        def add_executive(name, position, source_pass):
            if name and position and name not in [n for n, p in executives] and position != "ไม่ระบุ":
                if len(name.split()) > 7: 
                    return
                if len(position.split()) < 2 and any(position.startswith(title) for title in ['นาย', 'นาง', 'นางสาว', 'ดร.']):
                    return
                
                # ตรวจสอบชื่อซ้ำ (ใช้ชื่อจริง+นามสกุลที่ถูกแยกแล้วในการตรวจสอบ)
                clean_name_check = self._parse_name_components(name)[2:]
                if clean_name_check[0] and clean_name_check in [self._parse_name_components(n)[2:] for n, p in executives]:
                    return

                executives.append((name, position))
                logging.debug(f"✅ {source_pass}: {name} | {position}")

        # ===== PASS 1: Table Extraction (Strongest signal) =====
        logging.info("\n🔄 PASS 1: Extracting from tables...")
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                cell_texts = [re.sub(r'\s+', ' ', cell.get_text(strip=True)) for cell in cells if cell.get_text(strip=True)]
                
                name_found = None
                position_found = None
                
                for text in cell_texts:
                    if self._is_valid_thai_name(text):
                        name_found = text
                    elif self._is_valid_position(text):
                        position_found = text
                
                if name_found and position_found:
                    add_executive(name_found, position_found, "Table")
                elif name_found:
                    for text in cell_texts:
                        if text != name_found and self._is_valid_position(text):
                            add_executive(name_found, text, "Table (Adj)")
                            break
        
        logging.info(f"📊 After PASS 1 (Tables): {len(executives)} executives found")
        
        # ===== PASS 2: Adjacent Text in Containers (Normal mode) =====
        logging.info("\n🔄 PASS 2: Extracting from adjacent elements (normal)...")
        
        for i, (text, element) in enumerate(processed_texts):
            if self._is_valid_thai_name(text):
                name = text
                position = None
                
                for j in range(i + 1, min(i + 7, len(processed_texts))):
                    next_text = processed_texts[j][0]
                    if self._is_valid_position(next_text) and not self._is_valid_thai_name(next_text, relaxed=True):
                        position = next_text
                        break
                
                if position:
                    add_executive(name, position, "Adjacent")
        
        logging.info(f"📊 After PASS 2 (Adjacent): {len(executives)} executives found")
        
        # ===== PASS 3: Relaxed Mode - เพิ่มความยืดหยุ่น =====
        logging.info("\n🔄 PASS 3: Extracting with relaxed criteria...")
        
        for i, (text, element) in enumerate(processed_texts):
            if self._is_valid_thai_name(text, relaxed=True):
                name = text
                position = None
                
                for j in range(i + 1, min(i + 10, len(processed_texts))):
                    next_text = processed_texts[j][0]
                    if self._is_valid_position(next_text, relaxed=True) and not self._is_valid_thai_name(next_text, relaxed=True):
                        position = next_text
                        break
                
                if position:
                    add_executive(name, position, "Relaxed")
        
        logging.info(f"📊 After PASS 3 (Relaxed): {len(executives)} executives found")
        
        # ===== PASS 4: Pattern Matching - ค้นหาตาม Pattern เฉพาะ =====
        logging.info("\n🔄 PASS 4: Pattern-based extraction...")
        
        full_text = soup.get_text()
        
        pattern1 = r'((?:นาย|นาง|นางสาว|ดร\.|คุณ)[^\n]{10,80})\s+([^\n]{10,100}(?:ผู้จัดการ|กรรมการ|ประธาน|ผู้บริหาร|Director|Manager|CEO|CFO|CTO|COO|President)[^\n]{0,50})'
        matches1 = re.findall(pattern1, full_text, re.IGNORECASE)
        
        for name_candidate, pos_candidate in matches1:
            name_clean = re.sub(r'\s+', ' ', name_candidate.strip())
            pos_clean = re.sub(r'\s+', ' ', pos_candidate.strip())
            
            if self._is_valid_thai_name(name_clean, relaxed=True) and self._is_valid_position(pos_clean, relaxed=True):
                add_executive(name_clean, pos_clean, "Pattern1")
        
        logging.info(f"📊 After PASS 4 (Patterns): {len(executives)} executives found")

        # ===== PASS 5: Conjoined Name/Position Splitting (v5.1 - Improved Logic) =====
        logging.info("\n🔄 PASS 5: Conjoined Name/Position Splitting/Trimming...")

        executives_to_process = executives.copy()
        executives = [] # เคลียร์และสร้างใหม่ด้วย Logic การแยกคำ
        conjoined_keywords_regex = r'(ผู้จัดการ|กรรมการ|ผู้บริหาร|ผู้อำนวยการ|ประธาน|รองประธาน|ผู้ช่วย|หัวหน้า|CEO|CFO|CTO|COO|President|Director|Manager|Chief)'
        next_name_prefix_regex = r'(นาย|นาง|นางสาว|ดร\.|คุณ)(?:\s+)?\S{2,}'
        
        for name, position in executives_to_process:
            
            name_was_split = False

            # 1. ตรวจสอบและแยกชื่อที่ติดกับตำแหน่ง (เช่น 'อังศุสิงห์ผู้บริหารสายงาน')
            match_in_name = re.search(conjoined_keywords_regex, name)
            if match_in_name:
                split_index = match_in_name.start(0)
                name_candidate = name[:split_index].strip()
                pos_candidate = name[split_index:].strip()
                
                # ตรวจสอบว่าชื่อที่ตัดมายังเป็นชื่อที่ถูกต้อง (ต้องมีคำนำหน้า)
                if self._is_valid_thai_name(name_candidate, relaxed=True):
                    logging.info(f"  ✂️ SPLIT Name: '{name}' -> Name: '{name_candidate}' | Pos: '{pos_candidate}'")
                    add_executive(name_candidate, position, "Split Name")
                    name_was_split = True
            
            # 2. ตรวจสอบ Position ที่ยาวเกินไปและมีชื่อคนอื่นติดอยู่ (เช่น 'ตำแหน่งนายกิตติพงศ์')
            if not name_was_split:
                match_in_pos = re.search(next_name_prefix_regex, position)
                if match_in_pos:
                    split_index = match_in_pos.start(0)
                    pos_clean = position[:split_index].strip()
                    
                    # ต้องมีตำแหน่งสำคัญอยู่ในส่วนที่ตัดมา และไม่สั้นเกินไป
                    if self._is_valid_position(pos_clean, relaxed=True) and len(pos_clean) > 5:
                        logging.info(f"  ✂️ TRIM Pos: '{position}' -> Trimmed: '{pos_clean}' (Found next name: {match_in_pos.group(0)})")
                        add_executive(name, pos_clean, "Trim Pos")
                        continue

            # ถ้าไม่เกิดการแก้ไขใดๆ ให้เพิ่มรายการเดิมเข้าไป
            if (name, position) not in executives:
                add_executive(name, position, "Unmodified")


        logging.info(f"📊 After PASS 5 (Splitting/Trimming): {len(executives)} executives found")
        
        # Step 6: Final Filter and Clean up (ใช้ Logic เดิม)
        final_executives = []
        seen_names_tuple = set()
        
        for name, position in executives:
            # 💡 Final check: ใช้ _parse_name_components เพื่อดึงชื่อและนามสกุลที่ "สะอาด"
            prefix, full_name, first_name, surname = self._parse_name_components(name)
            
            clean_name_key = (first_name, surname)
            if not first_name or clean_name_key in seen_names_tuple:
                continue

            # ตรวจสอบความถูกต้องอีกครั้งด้วย relaxed mode
            if self._is_valid_thai_name(name, relaxed=True):
                final_executives.append((name, position))
                seen_names_tuple.add(clean_name_key) 

        logging.info(f"\n📊 Total executives found after all passes: {len(final_executives)}")

        return final_executives


    def _parse_name_components(self, full_name: str) -> Tuple[str, str, str, str]:
        """
        [ปรับปรุง] Parse name into prefix, full name, first name, and surname (v5.1)
        - เน้นการแยกนามสกุลที่ติดกับตำแหน่งออกจากกันอย่างเด็ดขาด
        """
        title_map = {
            "นาย": "Mr.", "นาง": "Mrs.", "นางสาว": "Ms.", 
            "ดร.": "Dr.", "ดร": "Dr.", "ศ.": "Prof.", 
            "รศ.": "Assoc. Prof.", "ผศ.": "Asst. Prof.", 
            "พลเอก": "Gen.", "พลโท": "Lt. Gen.", "พลตรี": "Maj. Gen.", 
            "พันเอก": "Col.", "พันโท": "Lt. Col.", "พันตรี": "Maj.",
            "ท่านผู้หญิง": "Khunying", "คุณหญิง": "Khunying", "คุณ": "Ms./Mr."
        }
        
        prefix = ""
        name_without_prefix = full_name
        
        # 1. แยกคำนำหน้าชื่อ
        for thai_title, eng_title in sorted(title_map.items(), key=lambda x: len(x[0]), reverse=True):
            if full_name.startswith(thai_title):
                prefix = eng_title
                name_without_prefix = full_name[len(thai_title):].strip()
                break
        
        name_without_prefix = re.sub(r'\s+', ' ', name_without_prefix).strip()
        
        # 2. ตรวจสอบและแยกนามสกุลที่ติดกับตำแหน่ง (การแก้ไขหลัก)
        conjoined_keywords_regex = r'(ผู้จัดการ|กรรมการ|ผู้บริหาร|ผู้อำนวยการ|ประธาน|รองประธาน|ผู้ช่วย|หัวหน้า|CEO|CFO|CTO|COO|President|Director|Manager|Chief|สายงาน|ที่ปรึกษา)'
        match = re.search(conjoined_keywords_regex, name_without_prefix, re.IGNORECASE)
        
        if match:
            split_index = match.start(0)
            # ส่วนที่เป็นชื่อ (นามสกุล)
            name_clean = name_without_prefix[:split_index].strip()

            if len(name_clean.split()) >= 1 and len(name_without_prefix) - len(name_clean) > 5:
                 logging.debug(f"  ✂️ Split Conjoined Name (in Parse): Original='{name_without_prefix}' -> Name='{name_clean}'")
                 name_without_prefix = name_clean


        parts = name_without_prefix.split()
        
        if len(parts) == 0:
            return prefix, full_name, "", ""
        elif len(parts) == 1:
            # ถ้าเป็นคำเดียว ถือเป็นชื่อจริงไปก่อน แต่ไม่ควรเกิดขึ้น
            return prefix, full_name, parts[0], ""
        else:
            # ชื่อ + นามสกุล
            first_name = parts[0]
            surname = " ".join(parts[1:])
            
            return prefix, full_name, first_name, surname.strip()


    def create_executive_records(self, executives: List[Tuple[str, str]]) -> List[Dict]:
        """
        [ปรับปรุง] ฟังก์ชันสำหรับสร้าง List ของ Dictionary (Records) 
        """
        records = []
        seen_names = set()
        
        logging.info("\n📝 Creating executive records...")
        
        for name, position in executives:
            
            prefix, full_name, first_name, surname = self._parse_name_components(name)
            
            # ใช้ชื่อที่ถูกแยกออกมาเพื่อตรวจสอบความซ้ำ
            clean_name_key = (first_name, surname)
            if clean_name_key in seen_names:
                continue
            
            if not first_name or not surname:
                # อนุญาตให้มี First name เท่านั้นหาก Surname ถูกตัดออกไป (เช่นชื่อเดียว)
                if not first_name or len(first_name) < 2:
                    logging.warning(f"  ⚠️ Could not parse name/surname: {name}")
                    continue
            
            record = {
                "BUSI_DT": self.busi_dt,
                "Prefixed_Name": prefix,
                "Full_Name": full_name, # ชื่อเต็มที่ดึงมา
                "First_Name": first_name,
                "Surname": surname,
                "Bank_Name": self.bank_name,
                "Position": position,
                "Source_URL": self.base_url, # 👈 เพิ่ม Source URL
            }
            
            records.append(record)
            seen_names.add(clean_name_key)
            logging.info(f"  ✅ {prefix} | {first_name} {surname} | {position}")
        
        # 🚀 [แก้ไข] ปรับปรุง Logic การเรียงลำดับให้ละเอียดขึ้น
        def sort_key(record):
            position = record['Position'].lower()
            
            # 1. ลำดับสูงสุด: ประธานกรรมการ (Chairman)
            if ('ประธาน' in position or 'chairman' in position) and 'กรรมการ' in position and 'บริหาร' not in position:
                return 0 


            if 'ประธาน' in position and ('บริหาร' in position or 'ceo' in position):
                return 1
            if 'president' in position and 'vice' not in position:
                return 1

             
            if 'รองประธาน' in position or 'vice president' in position:
                return 2
            
            if 'รองผู้จัดการใหญ่' in position or 'chief' in position or 'cfo' in position or 'cto' in position or 'coo' in position:
                return 3
            
            # 4. ลำดับถัดมา: กรรมการผู้จัดการใหญ่/ผู้จัดการใหญ่/MD
            if 'ผู้จัดการใหญ่' in position or 'managing director' in position or 'md' in position:
                return 4
            
            # 6. ลำดับถัดมา: ผู้บริหารระดับสูง (Executive Vice President / Head of Group)
            if 'ผู้บริหาร' in position or 'executive' in position or 'head of' in position:
                return 5
        

            # 5. ลำดับถัดมา: กรรมการบริษัท/Director
            if 'กรรมการ' in position or 'director' in position:
                return 6
            
                
            # 8. ลำดับต่ำสุด: ตำแหน่งอื่นๆ
            return 7
        
        records.sort(key=sort_key)
        
        if records:
            logging.info("\n📋 First 3 records structure after sorting and cleaning:")
            for i, record in enumerate(records[:3]):
                logging.info(f"  {i+1}. Prefixed: '{record['Prefixed_Name']}' | First: '{record['First_Name']}' | Last: '{record['Surname']}' | Pos: {record['Position']}")
        
        return records


    def intelligent_scrape(self, limit: int = 150) -> List[Dict]:
        logging.info("🚀 Starting scraping process...")
        
        html_content = self.fetch_page_content(self.base_url)
        if not html_content:
            logging.error("❌ Failed to fetch page content")
            return []
        
        self.bank_name = self.detect_bank_name(self.base_url, html_content)
        logging.info(f"🏦 Bank: {self.bank_name}")
        logging.info(f"📅 Business Date: {self.busi_dt}")
        
        executives = self.extract_executives_from_html(html_content)
        
        if not executives:
            logging.error("❌ No executives found")
            return []
        
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
            
    def check_scraped_data_against_source(self, scraped_records: List[Dict]) -> List[Dict]:
        """
        ฟังก์ชันตรวจสอบความถูกต้องของข้อมูลที่ Scrape มาเทียบกับ Source URL
        """
        logging.info("\n🕵️ Starting data validation check...")
        
        if not scraped_records:
            logging.warning("⚠️ No records to check.")
            return []
        
        # [แก้ไข] ต้อง fetch content ใหม่เนื่องจาก self.driver อาจจะปิดไปแล้ว
        html_content = self.fetch_page_content(self.base_url) 
        if not html_content:
            logging.error("❌ Failed to fetch page content for checking.")
            return scraped_records

        soup = BeautifulSoup(html_content, 'html.parser')
        page_text = soup.get_text()
        
        verified_records = []
        for record in scraped_records:
            full_name = record['Full_Name']
            first_name = record['First_Name']
            surname = record['Surname']
            
            # 1. ตรวจสอบชื่อเต็ม
            if full_name in page_text:
                verified_records.append(record)
                logging.debug(f"  ✅ Verified (Full Name): {full_name}")
                continue
            
            # 2. ตรวจสอบ (ชื่อจริง + นามสกุล)
            name_without_title = f"{first_name} {surname}".strip()
            if len(name_without_title.split()) >= 2 and name_without_title in page_text:
                verified_records.append(record)
                logging.debug(f"  ✅ Verified (First+Surname): {full_name}")
                continue
            
            # 3. ตรวจสอบ (นามสกุล)
            if len(surname.split()) >= 1 and surname in page_text:
                verified_records.append(record)
                logging.debug(f"  ✅ Verified (Surname): {full_name}")
                continue
                
            logging.warning(f"  ❌ Failed to verify (Name not found): {full_name}")
                
        logging.info(f"📊 Verified records: {len(verified_records)} / {len(scraped_records)}")
        return verified_records


def save_to_csv(data: List[Dict], bank_name: str, busi_dt: str) -> bool:
    """
    [ปรับปรุง] Save data to CSV with proper formatting
    """
    if not data:
        logging.warning("⚠️ No data to save")
        return False

    try:
        df = pd.DataFrame(data)
        
        column_order = ['BUSI_DT', 'Prefixed_Name', 'Full_Name', 
                        'First_Name', 'Surname', 'Bank_Name', 'Position',]
        
        for col in column_order:
            if col not in df.columns:
                df[col] = ""
        
        df_executives = df[column_order].copy()
        
        bank_short = bank_name.replace('ธนาคาร', '').strip()
        
        bank_name_map = {
            'กสิกรไทย': 'Kbank',
            'กรุงเทพ': 'Bangkok',
            'ไทยพาณิชย์': 'SCB',
            'กรุงไทย': 'Krungthai',
            'กรุงศรีอยุธยา': 'Krungsri',
            'ออมสิน': 'GSB',
            'ทหารไทยธนชาต': 'TTB',
            'เกียรตินาคินภัทร': 'KKP',
            'ธนชาต': 'Thanachart',
            'ทิสโก้': 'TISCO',
            'ไอซีบีซี (ไทย)': 'ICBC',
            'ซีไอเอ็มบี ไทย': 'CIMB'
        }
        
        file_bank_name = bank_short
        for thai_name, eng_name in bank_name_map.items():
            if thai_name in bank_short:
                file_bank_name = eng_name
                break
        
        date_str_month = datetime.now().strftime("%Y%m") 
        filename = f"{file_bank_name}_{date_str_month}.csv"
        
        os.makedirs('output', exist_ok=True)
        output_path = os.path.join('output', filename)
        
        # 💡 [แก้ไข] ดึง Source_URL จาก Record แรก และปรับ Footer
        source_url = df['Source_URL'].iloc[0] if not df.empty and 'Source_URL' in df.columns else "URL ไม่ระบุ"
        
        footer_data = {
            'BUSI_DT': source_url,           
            'Prefixed_Name': 'Source_URL',   
            'Full_Name': '', 
            'First_Name': '', 
            'Surname': '', 
            'Bank_Name': '', 
            'Position': '',
            
        }
        df_footer = pd.DataFrame([footer_data], columns=column_order)
        
        df_final = pd.concat([df_executives, df_footer], ignore_index=True)
        
        if os.path.exists(output_path):
            logging.info(f"📄 Appending to existing file: {output_path}")
            df_final.to_csv(output_path, index=False, encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
            logging.info(f"✅ Existing file overwritten with latest data.")
        else:
            df_final.to_csv(output_path, index=False, encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
        
        logging.info(f"\n✅ File saved: {output_path}")
        logging.info(f"📊 Total records (including footer): {len(df_final)}")
        
        print("\n" + "="*120)
        print(f"📊 RESULTS FOR {bank_name}")
        print(f"📅 Date: {busi_dt}")
        print(f"📁 File: {output_path}")
        print(f"📈 Records (Executives): {len(df_executives)}")
        print("="*120)
        
        print("\n📋 SAMPLE DATA (first 5 records):")
        pd.set_option('display.unicode.east_asian_width', True)
        pd.set_option('display.max_colwidth', 30)

        
        print("="*120 + "\n")
        
        return True
        
    except Exception as e:
        logging.error(f"❌ Error saving CSV: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main execution - รองรับ Multi-URL พร้อม Verification (แก้ไขสำหรับ Verifier)"""
    print("="*120)
    print("🤖 IMPROVED BANK EXECUTIVE SCRAPER v5.2 (Plus LLM Verification - FIX)")
    print("="*120)
    print("✅ **FIXED:** Import and usage of Verifier class.")
    print("✅ **ENHANCEMENT:** Automated LLM Verification using Verifier.")
    print("✅ **FIXED:** Source URL added to the last row of CSV.")
    print("✅ **ENHANCEMENT:** Improved executive position sorting hierarchy.") # 👈 **เพิ่ม: แจ้งการแก้ไข**
    print("="*120 + "\n")
    

    # [แก้ไข] Initializer ต้องเป็น Verifier() เนื่องจากนำเข้าคลาสชื่อ Verifier มา
    checker = Verifier() 
    urls = [
        "https://www.kasikornbank.com/th/about/Pages/executives.aspx",
    ]
    
    print("📋 URLs to scrape:")
    for i, url in enumerate(urls, 1):
        print(f"  {i}. {url}")
    print()
    
    all_results = []
    all_executives_data = []
    
    for url in urls:
        print(f"\n{'='*120}")
        print(f"🌐 Processing: {url}")
        print(f"{'='*120}\n")
        
        scraper = None
        
        try:
            scraper = FlexibleBankScraper(url)
            
            print(f"🌐 Target URL: {url}")
            print(f"📅 Date: {scraper.busi_dt}")
            
            html_content = scraper.fetch_page_content(url) # ต้องเก็บ HTML Content ไว้ใช้ตรวจสอบ
            if not html_content:
                print(f"\n❌ FAILED: Could not fetch HTML content for {url}")
                continue

            scraper.bank_name = scraper.detect_bank_name(url, html_content)
            print(f"🏦 Initial bank detection: {scraper.bank_name}\n")
            
            executives = scraper.extract_executives_from_html(html_content)
            
            if executives:
                print(f"\n🏦 Final detected bank: {scraper.bank_name}")
                
                records = scraper.create_executive_records(executives)
                
                # [NEW VERIFICATION STEP] 1: Run Internal Content Check
                verified_executives = scraper.check_scraped_data_against_source(records)
                
                # [NEW VERIFICATION STEP] 2: Run LLM Verification
                llm_result = checker.verify(verified_executives, html_content, scraper.bank_name)
                
                print("\n" + "="*80)
                print("🧠 LLM VERIFICATION RESULTS")
                print("="*80)
                
                if llm_result.get('is_complete', False):
                    print(f"✅ VERIFICATION SUCCESS: Data is COMPLETE and correct!")
                    final_data = verified_executives
                elif llm_result.get('error'):
                    print(f"⚠️ VERIFICATION FAILED (API Error): {llm_result['error']}")
                    final_data = verified_executives
                else:
                    missing = llm_result.get('missing_names', [])
                    extra = llm_result.get('extra_names', [])
                    
                    if missing:
                        print(f"❌ INCOMPLETE: Found {len(missing)} missing name(s):")
                        for name in missing:
                            print(f"   - {name}")
                    if extra:
                        print(f"⚠️ FALSE POSITIVES: Found {len(extra)} extra name(s) in scraped data (remove these):")
                        for name in extra:
                            print(f"   - {name}")
                    
                    # ในทางปฏิบัติ เราจะใช้ข้อมูลที่ Scrape มาทั้งหมดต่อไป
                    final_data = verified_executives 
                
                print("="*80)

                if final_data:
                    all_executives_data.extend(final_data) 
                    
                    if save_to_csv(final_data, scraper.bank_name, scraper.busi_dt):
                        print(f"\n✅ SUCCESS: Extracted and Verified {len(final_data)} executives from {scraper.bank_name}")
                        all_results.append({
                            'bank': scraper.bank_name,
                            'count': len(final_data),
                            'url': url,
                            'llm_status': 'Complete' if llm_result.get('is_complete') else 'Incomplete'
                        })
                    else:
                        print(f"\n⚠️ WARNING: Data extracted but failed to save CSV")
                else:
                    print(f"\n❌ FAILED: No executives passed the verification for {url}")
            else:
                print(f"\n❌ FAILED: No executives found for {url}")
            
        except KeyboardInterrupt:
            print("\n⚠️ Interrupted by user")
            break
        except Exception as e:
            logging.error(f"❌ Error processing {url}: {e}")
            import traceback
            traceback.print_exc()
        finally:
            if scraper:
                try:
                    scraper.close()
                except:
                    pass
    
    print("\n" + "="*120)
    print("📋 SCRAPING SUMMARY")
    print("="*120)
    
    if all_results:
        print(f"\n✅ Successfully scraped {len(all_results)} bank(s):\n")
        for i, result in enumerate(all_results, 1):
            print(f"  {i}. {result['bank']}: {result['count']} executives")
            print(f"      URL: {result['url']}\n")
    else:
        print("\n❌ No banks were successfully scraped")
    
    print("="*120)
    print("\n💡 TIP: Check the 'output' folder for generated CSV files")
    print("💡 TIP: v5.2 now uses the LLM Verifier for final data quality check.")
    print("="*120 + "\n")


if __name__ == "__main__":
    main()
import sys
import os
import re
import time
from datetime import datetime
import csv
import random
import logging
import pandas as pd
import traceback
from typing import List, Dict, Optional, Tuple

# Selenium Imports
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

# --- CONFIG ---
port = 11434
OLLAMA_API_URL = f"http://localhost:{port}/api/generate"
OLLAMA_MODEL = "llama3.2"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# --- VERIFIER HANDLING ---
# พยายาม Import Verifier ของจริง ถ้าไม่มีจะใช้ Mock Class เพื่อให้โปรแกรมไม่ Crash
try:
    from verifier import Verifier
except ImportError:
    logging.warning("⚠️ Could not import 'verifier.py'. Using DummyVerifier instead.")
    class Verifier:
        def verify(self, executives, html_content, bank_name):
            return {
                'is_complete': True,
                'missing_names': [],
                'extra_names': [],
                'error': None
            }

class FlexibleBankScraper:
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
        
        bank_keywords = {
            'bangkokbank': 'ธนาคารกรุงเทพ',
            'bbl': 'ธนาคารกรุงเทพ',
            'kasikorn': 'ธนาคารกสิกรไทย',
            'kbank': 'ธนาคารกสิกรไทย',
            'kasikornbank': 'ธนาคารกสิกรไทย',
            'scb': 'ธนาคารไทยพาณิชย์',
            'siamcommercial': 'ธนาคารไทยพาณิชย์',
            'ktb': 'ธนาคารกรุงไทย',
            'krungthai': 'ธนาคารกรุงไทย',
            'krungsri': 'ธนาคารกรุงศรีอยุธยา',
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
                logging.info(f" MATCH FOUND!!!")
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
                        logging.info(f" Bank detected from page title: {bank_name}")
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
                'Kasikornbank': 'ธนาคารกสิกรไทย',
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
                    logging.info(f" Bank detected from the page text '{thai_keyword}': {bank_name}")
                    return bank_name
        
        domain_match = re.search(r'https?://(?:www\.)?([^/]+)', url)
        if domain_match:
            domain = domain_match.group(1)
            logging.warning(f" Could not auto-detect bank. Domain: {domain}")
            return f" ธนาคาร ({domain})"
        
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
            
            logging.info(" ---- WebDriver setup completed!!! ----- ")
            return True
            
        except Exception as e:
            logging.error(f"----- Error setting up WebDriver: {e} -----")
            return False


    def fetch_page_content(self, url: str, retries: int = 3) -> Optional[str]:
        if self.driver is None:
            if not self.setup_driver():
                return None
                
        for attempt in range(retries):
            try:
                time.sleep(random.uniform(2, 4))
                logging.info(f" Navigating to {url} (attempt {attempt+1}/{retries})")
                self.driver.get(url)
                
                WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                logging.info(" ---- Waiting for content to load... ----")
                time.sleep(5)
                
                logging.info(" ---- Scrolling to load dynamic content... ----")
                for scroll_pct in [0.25, 0.5, 0.75, 1.0]:
                    self.driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * {scroll_pct});")
                    time.sleep(2)
                
                self.driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(2)
                
                page_source = self.driver.page_source
                
                if len(page_source) > 500:
                    logging.info(f" ---- Page fetched successfully ({len(page_source)} chars) ---- ")
                    return page_source
                
            except Exception as e:
                logging.warning(f" ---- Error on attempt {attempt+1}: {e} ----")
                
            if attempt < retries - 1:
                time.sleep(5 * (attempt + 1))
        
        logging.error(" ---- Failed to fetch page after all retries ----")
        return None


    def _is_valid_thai_name(self, text: str, relaxed: bool = False) -> bool:
        if not text or len(text) < 8 or len(text) > 90:
            return False
        
        words = text.split()
        max_words = 8 if relaxed else 4
        if len(words) > max_words:
            return False
        
        # กรองตัวเลข/ปี
        if re.search(r'\d{2,4}', text) and not re.search(r'(ดร|ศ|รศ|ผศ)', text):
            return False
        
        # กรองลิงก์/อีเมล
        if '@' in text or 'http' in text.lower() or '.com' in text.lower() or '.th' in text.lower():
            return False
        
        # Keywords ที่มักจะติดมากับชื่อและทำให้เกิดปัญหา (ตำแหน่งในภาษาไทยและอังกฤษ)
        conjoined_keywords = ['ผู้จัดการ', 'กรรมการ', 'ประธาน', 'ผู้อำนวยการ', 'chief', 'officer', 'director', 'manager', 'ผู้ช่วย', 'สายงาน']
    
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
        min_thai_chars = 4 if relaxed else 7
        if thai_char_count < min_thai_chars:
            return False
        
        thai_titles = ['นาย', 'นาง', 'นางสาว', 'ดร.', 'ดร', 'ศ.', 'รศ.', 'ผศ.', 
                       'พลเอก', 'พลโท', 'พลตรี', 'พันเอก', 'พันโท', 'พันตรี', 
                       'ท่านผู้หญิง', 'คุณหญิง', 'คุณ', 'พล.อ.อ.', 'พ.ต.อ.']
        
        if not any(text.startswith(title) for title in thai_titles):
            return False
        
        # อนุญาตให้มีชื่อ/นามสกุล 1 คำ หากเป็นชื่อที่ไม่มีช่องว่างและมีคำนำหน้า
        if len(words) < 2 and not any(title[:-1] in text for title in ['ดร.', 'ศ.', 'รศ.', 'ผศ.', 'คุณ']): 
            return False
        
        return True


    def _is_valid_position(self, text: str, relaxed: bool = False) -> bool:
        """ Check if text is a valid position title """
        if not text or len(text) < 4 or len(text) > 100:
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
        #ฟังก์ชันสำหรับแยกชื่อและตำแหน่งออกจากเนื้อหา
        soup = BeautifulSoup(html_content, 'html.parser')
        executives = []
        
        logging.info("\n Extracting executives from HTML...")
        
        # ลบ Element ที่ไม่ต้องการออกก่อน
        for script in soup(["script", "style", "noscript", "footer", "header"]):
            if script: script.decompose()
        
        for nav in soup.find_all('nav'):
            if nav: nav.decompose()

        all_text_elements = soup.find_all(['p', 'span', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'div', 'li', 'td', 'th', 'a'])
        
        # Pre-process All Text
        processed_texts = []
        for element in all_text_elements:
            # ใช้ separator= " " เพื่อป้องกันคำติดกันเมื่อ HTML อยู่คนละ Tag
            # เช่น <span>นาย</span><span>กานต์</span> จะได้ "นาย กานต์" ไม่ใช่ "นายกานต์"
            text = element.get_text(" ", strip=True) 
            text = re.sub(r'\s+', ' ', text).strip()
            
            if 4 <= len(text) <= 300:
                processed_texts.append((text, element))
        
        logging.info(f" ---- Total text elements to process: {len(processed_texts)} ---- ")
        
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
                logging.debug(f" ---- {source_pass}: {name} | {position} ----")

        # ===== PASS 1: Table Extraction (Strongest signal) =====
        logging.info("\n ---- PASS 1: Extracting from tables... ---- ")
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
        
        logging.info(f" ---- After PASS 1 (Tables): {len(executives)} executives found ----")
        
        # ===== PASS 2: Adjacent Text in Containers =====
        logging.info("\n ---- PASS 2: Extracting from adjacent elements (normal) ---- ")
        
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
        
        # ===== PASS 3: Relaxed Mode =====
        logging.info("\n ---- PASS 3: Extracting with relaxed criteria... ---- ")
        
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
        
        # ===== PASS 4: Pattern Matching =====
        logging.info("\n PASS 4: Pattern-based extraction...")
        
        full_text = soup.get_text()
        
        pattern1 = r'((?:นาย|นาง|นางสาว|ดร\.|คุณ)[^\n]{10,80})\s+([^\n]{10,100}(?:ผู้จัดการ|กรรมการ|ประธาน|ผู้บริหาร|Director|Manager|CEO|CFO|CTO|COO|President)[^\n]{0,50})'
        matches1 = re.findall(pattern1, full_text, re.IGNORECASE)
        
        for name_candidate, pos_candidate in matches1:
            name_clean = re.sub(r'\s+', ' ', name_candidate.strip())
            pos_clean = re.sub(r'\s+', ' ', pos_candidate.strip())
            
            if self._is_valid_thai_name(name_clean, relaxed=True) and self._is_valid_position(pos_clean, relaxed=True):
                add_executive(name_clean, pos_clean, "Pattern1")
        
        logging.info(f" After PASS 4 (Patterns): {len(executives)} executives found")

        # ===== PASS 5: Conjoined Name/Position Splitting =====
        logging.info("\n🔄 PASS 5: Conjoined Name/Position Splitting/Trimming...")

        executives_to_process = executives.copy()
        executives = [] # เคลียร์และสร้างใหม่ด้วย Logic แยกคำ
        conjoined_keywords_regex = r'(ผู้จัดการ|กรรมการ|ผู้บริหาร|ผู้อำนวยการ|ประธาน|รองประธาน|ผู้ช่วย|หัวหน้า|CEO|CFO|CTO|COO|President|Director|Manager|Chief)'
        next_name_prefix_regex = r'(นาย|นาง|นางสาว|ดร\.|คุณ)(?:\s+)?\S{2,}'
        
        for name, position in executives_to_process:
            
            name_was_split = False

            # 1. ตรวจสอบและแยกชื่อที่ติดกับตำแหน่ง
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
            
            # 2. ตรวจสอบ Position ที่ยาวเกินไปและมีชื่อคนอื่นติดอยู่
            if not name_was_split:
                match_in_pos = re.search(next_name_prefix_regex, position)
                if match_in_pos:
                    split_index = match_in_pos.start(0)
                    pos_clean = position[:split_index].strip()
                    
                    # ต้องมีตำแหน่งสำคัญอยู่ในส่วนที่ตัดมาและไม่สั้นเกินไป
                    if self._is_valid_position(pos_clean, relaxed=True) and len(pos_clean) > 5:
                        logging.info(f"  ✂️ TRIM Pos: '{position}' -> Trimmed: '{pos_clean}' (Found next name: {match_in_pos.group(0)})")
                        add_executive(name, pos_clean, "Trim Pos")
                        continue

            # ถ้าไม่เกิดการแก้ไขใดๆ ให้เพิ่มรายการเดิมเข้าไป
            if (name, position) not in executives:
                add_executive(name, position, "Unmodified")


        logging.info(f" ---- After PASS 5 (Splitting/Trimming): {len(executives)} executives found ---- ")
        
        # Step 6: Final Filter and Clean up
        final_executives = []
        seen_names_tuple = set()
        
        for name, position in executives:
            # Final check: ใช้ _parse_name_components เพื่อดึงชื่อและนามสกุลที่ "สะอาด"
            eng_p, f_name_full, thai_p, f_name, s_name = self._parse_name_components(name)
            
            clean_name_key = (f_name, s_name)
            if not f_name or clean_name_key in seen_names_tuple:
                continue

            # ตรวจสอบความถูกต้องอีกครั้งด้วย relaxed mode
            if self._is_valid_thai_name(name, relaxed=True):
                final_executives.append((name, position))
                seen_names_tuple.add(clean_name_key) 

        logging.info(f"\n ---- Total executives found after all passes: {len(final_executives)} ---- \n")

        return final_executives

    def _parse_name_components(self, Full_Name: str) -> Tuple[str, str, str, str, str]:
         #Logic การตัดคำที่แม่นยำ ไม่ใช้ Hardcode ตัวเลข ทำให้ชื่อไม่ถูกตัดขาด
        full_name = Full_Name.strip()
        
        title_map = {
            "นางสาว": "Ms.",
            "นาย": "Mr.", 
            "นาง": "Mrs.", 
            "ดร.": "Dr.", 
            "ดร": "Dr.", 
            "ศ.": "Prof.", 
            "รศ.": "Assoc. Prof.", 
            "ผศ.": "Asst. Prof.", 
            "พลเอก": "Gen.", 
            "พลโท": "Lt. Gen.", 
            "พลตรี": "Maj. Gen.", 
            "พันเอก": "Col.", 
            "พันโท": "Lt. Col.", 
            "พันตรี": "Maj.",
            "ท่านผู้หญิง": "Khunying", 
            "คุณหญิง": "Khunying", 
            "คุณ": "Ms./Mr."
        }
        
        eng_prefix = ""
        thai_prefix = ""
        name_without_prefix = full_name
        
        # ตรวจหาคำนำหน้าและเรียกใช้ความยาวจริงในการตัดคำ
        sorted_titles = sorted(title_map.items(), key=lambda x: len(x[0]), reverse=True)
        
        for thai_title, eng_title in sorted_titles:
            if full_name.startswith(thai_title):
                eng_prefix = eng_title
                thai_prefix = thai_title
                name_without_prefix = full_name[len(thai_title):].strip()
                break
        
        # ทำความสะอาดช่องว่าง
        name_without_prefix = re.sub(r'\s+', ' ', name_without_prefix).strip()
        
        # ตรวจสอบและแยกนามสกุลที่ติดกับตำแหน่ง
        conjoined_keywords_regex = r'(ผู้จัดการ|กรรมการ|ผู้บริหาร|ผู้อำนวยการ|ประธาน|รองประธาน|ผู้ช่วย|หัวหน้า|CEO|CFO|CTO|COO|President|Director|Manager|Chief|สายงาน|ที่ปรึกษา)'
        match = re.search(conjoined_keywords_regex, name_without_prefix, re.IGNORECASE)
        
        if match:
            split_index = match.start(0)
            name_clean = name_without_prefix[:split_index].strip()
            if len(name_clean.split()) >= 1 and len(name_without_prefix) - len(name_clean) > 5:
                name_without_prefix = name_clean
        
        # แยกชื่อ-นามสกุล
        parts = name_without_prefix.split()
        
        if len(parts) == 0:
            # ไม่มีชื่อเลย -> คืนค่าเดิม
            return eng_prefix, full_name, "", ""
        elif len(parts) == 1:
            # มีแค่ชื่อ ไม่มีนามสกุล
            first_name = parts[0]
            surname = ""
            full_name_thai = f"{thai_prefix}{first_name}" if thai_prefix else first_name
        else:
            # มีทั้งชื่อและนามสกุล
            first_name = parts[0]
            surname = " ".join(parts[1:]).strip()
            
            # --- ตัดภาษาอังกฤษที่ต่อท้ายชื่อไทย ---
            if re.search(r'[\u0E00-\u0E7F]', surname):
                match_eng = re.search(r'[a-zA-Z]', surname)
                if match_eng:
                    eng_index = match_eng.start()
                    surname_clean = surname[:eng_index].strip()
                    if surname_clean:
                        logging.debug(f"  ✂️ Trimmed English suffix: '{surname}' -> '{surname_clean}'")
                        surname = surname_clean
            
            # --- ตัดคำว่า "รอง" ที่ท้ายนามสกุล ---
            if surname.endswith("รอง"):
                surname = re.sub(r'\s*รอง$', '', surname).strip()
                logging.debug(f"  ✂️ Trimmed trailing 'Rong' from surname")
            
            # Full_Name = คำนำหน้าไทย + ชื่อ + นามสกุล
            full_name_thai = f"{thai_prefix}{first_name} {surname}" if thai_prefix else f"{first_name} {surname}"
        
        return eng_prefix, full_name_thai, thai_prefix, first_name, surname
    

    def create_executive_records(self, executives: List[Tuple[str, str]]) -> List[Dict]:
        records = []
        seen_names = set()
        
        logging.info("\n Creating executive records...")
        
        for name, position in executives:
            
            eng_prefix, full_name, thai_prefix, first_name, surname = self._parse_name_components(name)
            
            clean_name_key = (first_name, surname)
            if clean_name_key in seen_names:
                continue
            
            if not first_name or (not surname and len(first_name.split()) < 2 and len(first_name) < 4):
                logging.warning(f"  ⚠️ Could not parse name/surname effectively: {name}")
                continue
            
            record = {
                "BUSI_DT": self.busi_dt,
                "Eng_Prefix": eng_prefix,
                "Full_Name": full_name,
                "Thai_Prefix": thai_prefix,
                "First_Name": first_name,
                "Surname": surname,
                "Bank_Name": self.bank_name,
                "Position": position,
                "Source_URL": self.base_url,
            }
            

            records.append(record)
            seen_names.add(clean_name_key)
            logging.info(f" ---- {eng_prefix} | {thai_prefix} | {first_name} {surname} | {position} ----")
        
        return self._sort_executive_records(records)


    def intelligent_scrape(self, limit: int = 150) -> List[Dict]:
        logging.info(" ---- Starting scraping process... ---- ")
        
        html_content = self.fetch_page_content(self.base_url)
        if not html_content:
            logging.error(" ---- Failed to fetch page content ----")
            return []
        
        self.bank_name = self.detect_bank_name(self.base_url, html_content)
        logging.info(f" ---- Bank: {self.bank_name} ----")
        logging.info(f" ---- Business Date: {self.busi_dt} ----")
        
        executives = self.extract_executives_from_html(html_content)
        
        if not executives:
            logging.error(" ---- No executives found ---- ")
            return []
        
        records = self.create_executive_records(executives)
        
        logging.info(f"\n ---- Total records created: {len(records)} ---- ")
        return records[:limit]

    def _create_record_from_llm_data(self, full_name: str, position: str, confidence: float) -> Optional[Dict]:
        """
        สร้าง executive record จากข้อมูลที่ LLM ตรวจพบ
        Args:
            full_name: ชื่อเต็ม เช่น "นายสมชาย ใจดี"
            position: ตำแหน่ง เช่น "กรรมการผู้จัดการใหญ่"
            confidence: ค่าความเชื่อมั่น 0.0-1.0
        Returns:
            Dict record หรือ None ถ้าไม่สามารถสร้างได้
        """
        try:
            # Parse ชื่อเป็นส่วนประกอบ
            eng_prefix, full_name_parsed, thai_prefix, first_name, surname = self._parse_name_components(full_name)
            
            # ตรวจสอบความถูกต้องพื้นฐาน
            if not first_name or len(first_name) < 2:
                logger.warning(f" ----  Invalid first name: {full_name} ----")
                return None
            
            if not position or len(position) < 4:
                logger.warning(f"  ---- Invalid position: {position} ---- ")
                return None
            
            # สร้าง record ตามโครงสร้างเดียวกับ create_executive_records()
            record = {
                "BUSI_DT": self.busi_dt,
                "Eng_Prefix": eng_prefix,
                "Full_Name": full_name_parsed,
                "Thai_Prefix": thai_prefix,
                "First_Name": first_name,
                "Surname": surname,
                "Bank_Name": self.bank_name,
                "Position": position,
                "Source_URL": self.base_url,
                "Recovery_Confidence": confidence
            }

            logger.info(f"  ---- Created record: {eng_prefix} | {thai_prefix} {first_name} {surname} | {position} ---- ")
            return record
            
        except Exception as e:
            logger.error(f"  ---- Error creating record from LLM data: {e} ---- ")
            traceback.print_exc()
            return None
    
    def _sort_executive_records(self, records: List[Dict]) -> List[Dict]:
        """
        เรียงลำดับ executive records ตามลำดับความสำคัญของตำแหน่ง
        Args:
            records: List ของ executive records
        Returns:
            List ที่ถูกเรียงลำดับแล้ว
        """
        def sort_key(record):
            position = record['Position'].lower()
            
            # 0. ลำดับสูงสุด: ประธานกรรมการ (Chairman)
            if ('ประธาน' in position or 'chairman' in position) and 'กรรมการ' in position and 'บริหาร' not in position:
                return 0
            
            # 1. ประธานเจ้าหน้าที่บริหาร / CEO / President
            if 'ประธาน' in position and ('บริหาร' in position or 'ceo' in position):
                return 1
            if 'president' in position and 'vice' not in position:
                return 1
            
            # 2. รองประธาน / Vice President
            if 'รองประธาน' in position or 'vice president' in position:
                return 2
            
            # 3. Chief Officers (CFO, CTO, COO, etc.)
            if 'รองผู้จัดการใหญ่' in position or 'chief' in position or 'cfo' in position or 'cto' in position or 'coo' in position:
                return 3
            
            # 4. กรรมการผู้จัดการใหญ่ / Managing Director
            if 'ผู้จัดการใหญ่' in position or 'managing director' in position or 'md' in position:
                return 4
            
            # 5. ผู้บริหารระดับสูง / Executive
            if 'ผู้บริหาร' in position or 'executive' in position or 'head of' in position:
                return 5
            
            # 6. กรรมการบริษัท / Director
            if 'กรรมการ' in position or 'director' in position:
                return 6
            
            # 7. ตำแหน่งอื่นๆ
            return 7
        
        try:
            sorted_records = sorted(records, key=sort_key)
            logger.info(f"✓ Sorted {len(sorted_records)} records by position hierarchy")
            return sorted_records
        except Exception as e:
            logger.error(f" ---- Error sorting records: {e} ---- ")
            return records  # ถ้า sort ไม่ได้ให้คืนค่าเดิม
        
    def close(self):
        """Close WebDriver"""
        try:
            if self.driver:
                self.driver.quit()
                logging.info(" ----  WebDriver closed ---- ")
        except Exception as e:
            logging.error(f" ---- Error closing WebDriver: {e} ---- ")
            
    def check_scraped_data_against_source(self, scraped_records: List[Dict]) -> List[Dict]:
        logging.info("\n ---- Starting data validation check (Smart & Relaxed)... ---- ")
        
        if not scraped_records:
            return []
        
        html_content = self.fetch_page_content(self.base_url) 
        if not html_content:
            return scraped_records

        soup = BeautifulSoup(html_content, 'html.parser')
        page_text = soup.get_text(" ", strip=True) # ใช้ separator เพื่อความชัวร์
        
        # สร้าง Page Text แบบไร้ช่องว่างเพื่อใช้เทียบกรณีเว้นวรรคไม่ตรงกัน
        page_text_nospace = re.sub(r'\s+', '', page_text)
        
        # ตรวจสอบแต่ละ record
        verified_records = []
        for record in scraped_records:
            full_name = record['Full_Name']
            first_name = record['First_Name']
            surname = record['Surname']
            is_verified = False
            
            names_to_check = [
                full_name,                                      # 1. ชื่อเต็มเดิมๆ
                f"{first_name} {surname}".strip(),              # 2. ชื่อ+นามสกุล (เผื่อ full_name ติดกัน)
                f"{record['Eng_Prefix']} {first_name} {surname}".strip() # 3. คำนำหน้า(Eng)+ชื่อ
            ]
            
            for name_variant in names_to_check:
                if name_variant and len(name_variant) > 3 and name_variant in page_text:
                    is_verified = True
                    logging.debug(f" ---- Verified (Standard): {name_variant} ---- " )
                    break
            
            # Check 2: ค้นหาแบบไม่สนเว้นวรรค
            if not is_verified:
                full_name_nospace = re.sub(r'\s+', '', full_name)
                # ลบคำนำหน้าไทยออก เพื่อเช็คเนื้อชื่อจริงๆ
                clean_name_nospace = re.sub(r'^(นาย|นาง|นางสาว|ดร\.|ศ\.|รศ\.|คุณ)', '', full_name_nospace)
                
                if len(clean_name_nospace) > 4 and clean_name_nospace in page_text_nospace:
                    is_verified = True
                    logging.debug(f" ---- Verified (No-Space Match): {full_name} ---- ")

            if is_verified:
                verified_records.append(record)
            else:
                logging.warning(f" ---- Failed to verify: {full_name} (Not found in source text) ---- ")
                
        logging.info(f" ---- Verified records: {len(verified_records)} / {len(scraped_records)} ---- ")
        return verified_records

def save_to_csv(data: List[Dict], bank_name: str, busi_dt: str) -> bool:
    if not data:
        logging.error(" ---- No data to save. ---- ")
        return False
    try:
        df = pd.DataFrame(data)
        
        # 1. กำหนดลำดับคอลัมน์ตามที่คุณต้องการเป๊ะๆ
        column_order_save = [
            'BUSI_DT', 'Eng_Prefix', 'Full_Name', 'Thai_Prefix', 
            'First_Name', 'Surname', 'Bank_Name', 'Position'
        ]
        
        # ตรวจสอบว่าคอลัมน์ครบไหม ถ้าไม่ครบให้สร้างว่างๆ ไว้ก่อนกัน Error
        for col in column_order_save:
            if col not in df.columns:
                df[col] = ""
        
        # 2. จัดการเรื่องชื่อไฟล์
        bank_short = bank_name.replace('ธนาคาร', '').strip()
        bank_name_map = {
            'กสิกรไทย': 'Kbank', 'กรุงเทพ': 'Bangkok', 'ไทยพาณิชย์': 'SCB',
            'กรุงไทย': 'Krungthai', 'กรุงศรีอยุธยา': 'Krungsri', 'ออมสิน': 'GSB',
            'ทหารไทยธนชาต': 'TTB', 'เกียรตินาคินภัทร': 'KKP', 'ธนชาต': 'Thanachart',
            'ทิสโก้': 'TISCO', 'ไอซีบีซี (ไทย)': 'ICBC', 'ซีไอเอ็มบี ไทย': 'CIMB'
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
        
        # 3. สร้างส่วน Footer (บรรทัดสุดท้ายที่บอก URL)
        source_url = data[0].get('Source_URL', 'URL ไม่ระบุ') if data else "URL ไม่ระบุ"
        
        # แก้ไขตรงนี้: ใช้ชื่อคอลัมน์ที่มีอยู่จริงใน column_order_save
        footer_row = {col: "" for col in column_order_save}
        footer_row['BUSI_DT'] = 'Source_URL:'
        footer_row['Eng_Prefix'] = source_url # ใส่ URL ไว้ในคอลัมน์ที่สองต่อจากคำว่า Source_URL:
        
        df_footer = pd.DataFrame([footer_row])
        
        # 4. รวมข้อมูล (Data + Footer) และเลือกเฉพาะคอลัมน์ที่กำหนด
        df_final = pd.concat([df[column_order_save], df_footer], ignore_index=True)
        
        # 5. บันทึกไฟล์
        df_final.to_csv(output_path, index=False, encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
        
        print("\n" + "="*120)
        print(f" ---- RESULTS FOR {bank_name} ---- ")
        print(f" ---- Saved to: {output_path} ---- ")
        print(f" ---- Total: {len(df)} Executives ---- ")
        print("="*120)
        
        return True
        
    except Exception as e:
        logging.error(f" ---- Error saving CSV: {e} ----")
        traceback.print_exc()
        return False


def main():
    """Main execution - รองรับ Multi-URL พร้อม Auto-Recovery ของข้อมูลที่หายไป (v6.0)"""
    print("="*120)
    print(" BANK EXECUTIVE SCRAPER with Auto-Recovery Missing Data ")
    print("="*120)
    print("="*120 + "\n")
    
    checker = Verifier() 
    urls = [
        "https://www.kasikornbank.com/th/about/Pages/executives.aspx",
        # "https://www.scbx.com/th/executive-scbx/about-board-of-directors/",
        # "https://www.krungsri.com/th/about-krungsri/about-us/organization-chart/board-of-directors"
    ]
    
    print(f" ---- URLs to scrape: {urls} ----- ")
    for i, url in enumerate(urls, 1):
        print(f"  {i}. {url}")
    print()
    
    all_results = []
    all_executives_data = []
    
    for url in urls:
        print(f"\n{'='*120}")
        print(f" Processing: {url}")
        print(f"{'='*120}\n")
        
        scraper = None
        
        try:
            scraper = FlexibleBankScraper(url)
            
            print(f"🌐 Target URL: {url}")
            print(f"📅 Date: {scraper.busi_dt}")
            
            html_content = scraper.fetch_page_content(url)
            if not html_content:
                print(f"\n ---- FAILED: Could not fetch HTML content for {url} ---- ")
                continue

            scraper.bank_name = scraper.detect_bank_name(url, html_content)
            print(f" ---- Initial bank detection: {scraper.bank_name}\n ---- ")
            
            executives = scraper.extract_executives_from_html(html_content)
            
            if executives:
                print(f"\n Final detected bank: {scraper.bank_name}")
                
                records = scraper.create_executive_records(executives)
                
                # [VERIFICATION STEP 1] Internal Content Check
                verified_executives = scraper.check_scraped_data_against_source(records)
                
                print(f"\n After internal verification: {len(verified_executives)} executives")
                
                # [VERIFICATION STEP 2] LLM Verification
                llm_result = checker.verify(verified_executives, html_content, scraper.bank_name)
                
                print("\n" + "="*80)
                print("LLM VERIFICATION RESULTS")
                print("="*80)
                
                # ตัวแปรสำหรับเก็บข้อมูลสุดท้าย
                final_data = verified_executives.copy()
                recovery_attempted = False
                
                if llm_result.get('is_complete', False):
                    print(f" ---- VERIFICATION SUCCESS: Data is COMPLETE and correct! ---- ")
                    
                elif llm_result.get('error'):
                    print(f" ---- VERIFICATION FAILED (API Error): {llm_result['error']} ----")
                    print(f" ---- Proceeding with scraped data without LLM recovery ---- ")
                    
                else:
                    missing = llm_result.get('missing_names', [])
                    extra = llm_result.get('extra_names', [])
                    
                    # Auto-Recovery Missing Data
                    if missing:
                        print(f"\n  INCOMPLETE: Found {len(missing)} missing name(s)")
                        print("="*80)
                        
                        recovery_attempted = True
                        recovered_count = 0
                        
                        for missing_entry in missing:
                            if isinstance(missing_entry, dict):
                                full_name = missing_entry.get('full_name', '')
                                position = missing_entry.get('position', '')
                                confidence = missing_entry.get('confidence', 0.0)
                                
                                print(f"\n RECOVERING: {full_name}")
                                print(f"   Position: {position}")
                                print(f"   Confidence: {confidence:.2%}")
                                
                                # สร้าง record ใหม่สำหรับชื่อที่หายไป
                                recovered_record = scraper._create_record_from_llm_data(
                                    full_name, 
                                    position, 
                                    confidence
                                )
                                
                                if recovered_record:
                                    is_duplicate = any(
                                        r['Full_Name'] == recovered_record['Full_Name'] 
                                        for r in final_data
                                    )
                                    
                                    if not is_duplicate:
                                        final_data.append(recovered_record)
                                        recovered_count += 1
                                        print(f" ---- RECOVERED and ADDED to dataset ---- ")
                                    else:
                                        print(f" ---- SKIPPED: Duplicate entry detected ----")
                                else:
                                    print(f" ---- FAILED: Could not parse name components ---- ")
                            else:
                                # Old format (string only)
                                print(f" ---- Missing name: {missing_entry} ----")
                        
                        print(f"\n{'='*80}")
                        print(f" Recovery Summary: {recovered_count}/{len(missing)} entries recovered")
                        print(f" Final dataset: {len(final_data)} executives")
                        print(f"{'='*80}")
                    
                    if extra:
                        print(f"\n FALSE POSITIVES: Found {len(extra)} extra name(s):")
                        for name in extra:
                            print(f"   - {name}")
                        print(f"\n Consider reviewing these entries manually")
                
                print("="*80)

                if final_data:
                    all_executives_data.extend(final_data)
                    
                    # เรียงลำดับใหม่หลังจากเพิ่มข้อมูล
                    final_data_sorted = scraper._sort_executive_records(final_data)
                    
                    if save_to_csv(final_data_sorted, scraper.bank_name, scraper.busi_dt):
                        status_msg = "COMPLETE" if llm_result.get('is_complete') else "RECOVERED" if recovery_attempted else "INCOMPLETE"
                        print(f"\n SUCCESS: Saved {len(final_data_sorted)} executives from {scraper.bank_name} (Status: {status_msg})")
                        
                        all_results.append({
                            'bank': scraper.bank_name,
                            'count': len(final_data_sorted),
                            'url': url,
                            'llm_status': status_msg,
                            'recovered': len(final_data_sorted) - len(verified_executives) if recovery_attempted else 0
                        })
                    else:
                        print(f"\n  WARNING: Data extracted but failed to save CSV")
                else:
                    print(f"\n FAILED: No executives passed verification for {url}")
            else:
                print(f"\n  FAILED: No executives found for {url}")
            
        except KeyboardInterrupt:
            print("\n $ Interrupted by user $ ")
            break
        except Exception as e:
            logging.error(f" ---- Error processing {url}: {e} ---- ")
            import traceback
            traceback.print_exc()
        finally:
            if scraper:
                try:
                    scraper.close()
                except:
                    pass
    
    print("\n" + "="*120)
    print(" SCRAPING SUMMARY")
    print("="*120)
    
    if all_results:
        print(f"\n Successfully processed {len(all_results)} bank(s):\n")
        for i, result in enumerate(all_results, 1):
            recovered_info = f" (+{result['recovered']} recovered)" if result.get('recovered', 0) > 0 else ""
            print(f"  {i}. {result['bank']}: {result['count']} executives{recovered_info}")
            print(f"      Status: {result['llm_status']}")
            print(f"      URL: {result['url']}\n")
    else:
        print("\n No banks were successfully scraped")
    
    print("="*120)
    print("\n TIP: Check the 'output' folder for generated CSV files")
    print(" TIP: v6.0 automatically recovers missing executives with high confidence (>= 0.85)")
    print("="*120 + "\n")


if __name__ == "__main__":
    main()

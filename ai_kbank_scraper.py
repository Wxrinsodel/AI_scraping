from cgitb import text
import difflib
from turtle import position
import pandas as pd
import logging # บันทึกสถานะการทำงาน
import sys
import os
import re
import time
import requests
import json
from datetime import datetime
import csv
import random
from selenium import webdriver #สำหรับการโหลดหน้าเว็บแบบไดนามิก
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Tuple


"""
การตั้งค่า OLLAMA API: กำหนดพอร์ต URL และชื่อโมเดลสำหรับ API ของ OLLAMA 
ซึ่งในโค้ดส่วนที่เหลือ ไม่ได้มีการเรียกใช้จริง (เป็นโค้ดที่เตรียมไว้สำหรับการใช้งาน LLM ภายหลัง)
"""
port = 11434
OLLAMA_API_URL = f"http://localhost:{port}/api/generate"
OLLAMA_MODEL = "llama3.2"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__) # 


class FlexibleBankScraper:
    """Universal web scraper for Thai bank executives - Works with multiple banks"""

    def __init__(self, base_url): #store as the name said
        self.base_url = base_url
        self.driver = None
        self.bank_name = None
        self.busi_dt = datetime.now().strftime("%Y-%m-%d")
        self.verified_executives = []


    def detect_bank_name(self, url: str, html_content: Optional[str] = None) -> str:
        #ตรวจจับชื่อธนาคารจาก URL หรือเนื้อหาหน้าเว็บหากมี
        url_lower = url.lower()
        
        logging.info(f"\n{'='*80}")
        logging.info(f"🔍 BANK DETECTION DEBUG")
        logging.info(f"{'='*80}")
        logging.info(f"🔗 URL: {url}")
        logging.info(f"🔗 URL (lowercase): {url_lower}")
        
        # Extended bank keyword mapping
        bank_keywords = {
            # ธนาคารกรุงเทพ
            'bangkokbank.com': 'ธนาคารกรุงเทพ',
            'bangkokbank': 'ธนาคารกรุงเทพ',
            'bbl.co.th': 'ธนาคารกรุงเทพ',
            'bbl': 'ธนาคารกรุงเทพ',
            
            # ธนาคารกสิกรไทย
            'kasikornbank': 'ธนาคารกสิกรไทย',
            'kasikorn': 'ธนาคารกสิกรไทย',
            'kbank': 'ธนาคารกสิกรไทย',
            
            # ธนาคารไทยพาณิชย์
            'scb': 'ธนาคารไทยพาณิชย์',
            'scb.co.th': 'ธนาคารไทยพาณิชย์',
            'siamcommercial': 'ธนาคารไทยพาณิชย์',
            
            # ธนาคารกรุงไทย
            'ktb': 'ธนาคารกรุงไทย',
            'krungthai': 'ธนาคารกรุงไทย',
            'ktb.co.th': 'ธนาคารกรุงไทย',
            
            # ธนาคารกรุงศรีอยุธยา
            'krungsri': 'ธนาคารกรุงศรีอยุธยา',
            'krungsri.com': 'ธนาคารกรุงศรีอยุธยา',
            'baya': 'ธนาคารกรุงศรีอยุธยา',
            
            # ธนาคารทหารไทยธนชาต
            'ttb': 'ธนาคารทหารไทยธนชาต',
            'tmbthanachart': 'ธนาคารทหารไทยธนชาต',
            
            # ธนาคารเกียรตินาคินภัทร
            'kiatnakin': 'ธนาคารเกียรตินาคินภัทร',
            'kkp': 'ธนาคารเกียรตินาคินภัทร',
            
            # ธนาคารธนชาต
            'thanachart': 'ธนาคารธนชาต',
            
            # ธนาคารทิสโก้
            'tisco': 'ธนาคารทิสโก้',
            
            # ธนาคารไอซีบีซี (ไทย)
            'icbc': 'ธนาคารไอซีบีซี (ไทย)',
            
            # ธนาคารซีไอเอ็มบี ไทย
            'cimb': 'ธนาคารซีไอเอ็มบี ไทย',
        }
        
        # First > try ตรวจจับจาก URL: วนลูปใน Dictionary หากพบ KeywordในURLจะreturnชื่อธนาคารทันที
        for keyword, bank_name in bank_keywords.items():
            if keyword in url_lower:
                logging.info(f"✅ MATCH FOUND!")
                logging.info(f"   Keyword: '{keyword}'")
                logging.info(f"   Bank: {bank_name}")
                logging.info(f"{'='*80}\n")
                return bank_name
        
        logging.warning(f"⚠️ No bank detected from URL!")
        logging.warning(f"⚠️ Checking page content...\n")

        # Second > try ตรวจจับจากเนื้อหา HTML: 
        # หากไม่พบจาก URL จะใช้ BeautifulSoup วิเคราะห์ Title Tag, Meta Tags, และ ชื่อไทยในข้อความหน้าเว็บ

        if html_content:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Check title tag
            title = soup.find('title')
            if title:
                title_text = title.get_text().lower()
                for keyword, bank_name in bank_keywords.items():
                    if keyword in title_text:
                        logging.info(f"🏦 Bank detected from page title: {bank_name}")
                        return bank_name
            
            # Check meta tags
            meta_tags = soup.find_all('meta', attrs={'name': ['description', 'title', 'og:title']})
            for meta in meta_tags:
                content = meta.get('content', '').lower()
                for keyword, bank_name in bank_keywords.items():
                    if keyword in content:
                        logging.info(f"🏦 Bank detected from meta tags: {bank_name}")
                        return bank_name
            
            # Check for Thai bank names in page content
            page_text = soup.get_text()
            
            thai_bank_exact = {
                'ธนาคารกรุงเทพ': 'ธนาคารกรุงเทพ',
                'กรุงเทพ': 'ธนาคารกรุงเทพ',
                'Bangkok Bank': 'ธนาคารกรุงเทพ', # blocked

                'ธนาคารกสิกรไทย': 'ธนาคารกสิกรไทย',
                'กสิกรไทย': 'ธนาคารกสิกรไทย',
                'KASIKORNBANK': 'ธนาคารกสิกรไทย', # test already

                'ธนาคารไทยพาณิชย์': 'ธนาคารไทยพาณิชย์',
                'ไทยพาณิชย์': 'ธนาคารไทยพาณิชย์', # test already

                'ธนาคารกรุงไทย': 'ธนาคารกรุงไทย',
                'กรุงไทย': 'ธนาคารกรุงไทย', # blocked

                'ธนาคารออมสิน': 'ธนาคารออมสิน',
                'ออมสิน': 'ธนาคารออมสิน',
                'gsb': 'ธนาคารออมสิน', # test already

                'ธนาคารกรุงศรีอยุธยา': 'ธนาคารกรุงศรีอยุธยา',
                'กรุงศรีอยุธยา': 'ธนาคารกรุงศรีอยุธยา', # test already

            }
            
            for thai_keyword, bank_name in thai_bank_exact.items():
                if thai_keyword in page_text:
                    logging.info(f"✅ Bank detected from page Thai text '{thai_keyword}': {bank_name}")
                    return bank_name
        
        # If still not detected, try to extract from domain
        domain_match = re.search(r'https?://(?:www\.)?([^/]+)', url)
        if domain_match:
            domain = domain_match.group(1)
            logging.warning(f"⚠️ Could not auto-detect bank. Domain: {domain}")
            return f"ธนาคาร ({domain})"
        
        return "ธนาคารไม่ระบุ"


    def setup_driver(self) -> bool:

        """ฟังก์ชันตั้งค่าและเริ่มต้น Selenium WebDriver (Chrome)
        ตั้งค่า ChromeOptions เพื่อให้ทำงานแบบ Headless (ไม่มีหน้าต่างเบราว์เซอร์) 
        และเพิ่ม Argument เพื่อหลีกเลี่ยงการถูกตรวจจับว่าเป็น Bot 
        (เช่น user-agent, excludeSwitches, useAutomationExtension)
        """

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
            self.driver.set_page_load_timeout(60) # 60 seconds timeout

            # เรียกใช้ JavaScript เพื่อซ่อน Property navigator.webdriver ซึ่งเป็นอีกวิธีที่เว็บไซต์ใช้ตรวจจับ Selenium
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            logging.info("✅ WebDriver setup completed")
            return True
            
        except Exception as e:
            logging.error(f"❌ Error setting up WebDriver: {e}")
            return False


    def fetch_page_content(self, url: str, retries: int = 3) -> Optional[str]:
        """ฟังก์ชันสำหรับดึงเนื้อหาหน้าเว็บด้วย Selenium"""
        if self.driver is None:
            if not self.setup_driver():
                return None
                
        for attempt in range(retries):
            try:
                time.sleep(random.uniform(2, 4)) # wait before each attempt to avoid detection
                logging.info(f"🌐 Navigating to {url} (attempt {attempt+1}/{retries})")
                self.driver.get(url)
                
                # รอจนกว่า Element <body/> จะปรากฏ (สูงสุด 20 วินาที) 
                # เพื่อให้แน่ใจว่าหน้าเว็บเริ่มต้นโหลดแล้ว
                WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                logging.info("⏳ Waiting for content to load...")
                time.sleep(5)
                
                # วนลูปสั่งให้เบราว์เซอร์ Scroll ไปยังตำแหน่งต่าง ๆ ของหน้าเว็บ 
                # เพื่อกระตุ้นการโหลดเนื้อหาแบบ Lazy Loading
                logging.info("📜 Scrolling to load dynamic content...")
                for scroll_pct in [0.25, 0.5, 0.75, 1.0]:
                    self.driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * {scroll_pct});")
                    time.sleep(2)
                
                self.driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(2)
                
                page_source = self.driver.page_source # ดึงเนื้อหา HTML ทั้งหมดของหน้าเว็บที่โหลดเสร็จแล้ว
                
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
        """ฟังก์ชันตรวจสอบว่าข้อความเป็นชื่อผู้บริหารไทยที่ถูกต้องหรือไม่"""
        if not text or len(text) < 6 or len(text) > 90:
            return False
        
        # ตรวจสอบว่าไม่มีมากกว่า 10 คำ (ป้องกันการติดกันของหลายชื่อ)
        words = text.split()
        if len(words) > 10:
            return False
        
        # ตรวจสอบว่ามีตัวเลขพุทธศักราช
        if re.search(r'25\d{2}', text):
            return False
        
        # ตรวจสอบว่าไม่มีอีเมลหรือ URL
        if '@' in text or 'http' in text.lower() or '.com' in text.lower() or '.th' in text.lower():
            return False
        
        # คำที่ไม่ใช่ชื่อคน
        invalid_keywords = [
            'สงวนลิขสิทธิ์', 'ลิขสิทธิ์', 'copyright', '©', 'all rights reserved',
            'บริษัท', 'บมจ', 'จำกัด', 'มหาชน', 'limited', 'public', 'company',
            'เว็บไซต์', 'website', 'www.', 'http', '.com', '.th', '.co',
            'โทร', 'โทรศัพท์', 'telephone', 'tel:', 'email', 'e-mail',
            'ติดต่อ', 'contact', 'สอบถาม', 'information', 'ข้อมูล',
            'แผนก', 'ฝ่าย', 'department', 'division', 'section',
            'เลขที่', 'address', 'ที่อยู่', 'location', 'สถานที่',
            'วันที่', 'date', 'เวลา', 'time', 'ปี', 'year',
            'พ.ศ.', 'ค.ศ.', 'a.d.', 'b.e.',
            'เมนู', 'menu', 'หน้าหลัก', 'home', 'กลับ', 'back',
            'ค้นหา', 'search', 'ภาษา', 'language', 'ไทย', 'english',
            'ดาวน์โหลด', 'download', 'pdf', 'print', 'พิมพ์',
            'เพิ่มเติม', 'more', 'อ่านเพิ่มเติม', 'read more',
            'ประกาศ', 'announcement', 'ข่าว', 'news',
            'นโยบาย', 'policy', 'เงื่อนไข', 'terms', 'conditions',
            'ความเป็นส่วนตัว', 'privacy', 'คุกกี้', 'cookie',
        ]
        
        text_lower = text.lower()
        for keyword in invalid_keywords:
            if keyword.lower() in text_lower:
                return False
        
        # ตรวจสอบอักขระพิเศษ
        special_char_count = sum(1 for char in text if char in '©®™@#$%^&*()_+=[]{}|\\:;"<>,.?/')
        if special_char_count > 2:
            return False
        
        # ต้องมีอักษรไทยอย่างน้อย 5 ตัว
        thai_char_count = sum(1 for char in text if 0x0E00 <= ord(char) <= 0x0E7F)
        if thai_char_count < 5:
            return False
        
        # ต้องมีคำนำหน้าชื่อที่ถูกต้อง
        thai_titles = ['นาย', 'นาง', 'นางสาว', 'ดร.', 'ดร', 'ศ.', 'รศ.', 'ผศ.', 'พันตรี', 'พล.', 'พลเอก', 'พลโท', 'พลตรี']
        if not any(text.startswith(title) for title in thai_titles):
            if not any(title in text for title in thai_titles):
                return False
        
        # ต้องมีอย่างน้อย 2 คำ (คำนำหน้า + ชื่อ หรือ ชื่อ + นามสกุล)
        if len(words) < 2:
            return False
        
        return True


    def _is_valid_position(self, text: str) -> bool:
        """Check if text is a valid position title"""
        if not text or len(text) < 2:
            return False
        
        valid_keywords = [
            'ผู้จัดการ', 'กรรมการ', 'ผู้บริหาร', 'ผู้อำนวยการ', 
            'ประธาน', 'รองประธาน', 'ผู้ช่วย', 'หัวหน้า', 'ผู้ตรวจสอบ',
            'CEO', 'CFO', 'CTO', 'COO', 'President', 'Vice',
            'Executive', 'Director', 'Manager', 'Chief', 'Officer',
            'Assistant', 'Deputy', 'Senior', 'Head', 'Business',
            'ที่ปรึกษา', 'เลขานุการ', 'คณะกรรมการ', 'บริษัท',
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
        """ฟังก์ชันสำหรับแยกชื่อและตำแหน่งออกจากเนื้อหา HTML"""
        soup = BeautifulSoup(html_content, 'html.parser')
        executives = []
        
        logging.info("\n🔍 Extracting executives from HTML...")
        
        # Save HTML for debugging
        with open("debug_page.html", "w", encoding="utf-8") as f:
            f.write(html_content)
        logging.info("💾 Saved page content to debug_page.html for inspection")

        # ลบ Element ที่ไม่ต้องการออกก่อน
        for script in soup(["script", "style", "noscript"]):
            script.decompose()
        
        # METHOD 1.1: Check tables first: วนลูปหาข้อมูลใน Element <table> 
        # และจับคู่ข้อความที่ผ่าน _is_valid_thai_name กับข้อความใน Cell 
        # ข้างเคียงที่ผ่าน _is_valid_position
        
        tables = soup.find_all('table')
        logging.info(f"📊 Found {len(tables)} tables")
        
        # วนลูปตรวจสอบตารางแต่ละตารางที่พบ
        for table_idx, table in enumerate(tables):
            rows = table.find_all('tr') # ดึงแถวทั้งหมดในตาราง

            # วนลูปตรวจสอบแต่ละแถว
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    # Try to find name and position pairs
                    cell_texts = [cell.get_text(strip=True) for cell in cells]
                    # ลบช่องว่างส่วนเกินในข้อความ
                    cell_texts = [re.sub(r'\s+', ' ', text) for text in cell_texts if text]
                    
                    # วนลูปตรวจสอบแต่ละ Cell ในแถว
                    for i in range(len(cell_texts)):
                        text = cell_texts[i]
                        if self._is_valid_thai_name(text):
                            name = text
                            position = "ไม่ระบุ"
                            
                            # Look for position in adjacent cells
                            for j in range(len(cell_texts)):
                                # ข้าม Cell ที่เป็นชื่อเพราะต้องการหาตำแหน่ง
                                if i != j and self._is_valid_position(cell_texts[j]):
                                    position = cell_texts[j]
                                    break
                            
                            # ตรวจสอบว่าชื่อนี้ยังไม่เคยถูกบันทึกไว้ใน executives หรือไม่ (ป้องกันชื่อซ้ำ)
                            if not any(name == existing_name for existing_name, _ in executives):
                                executives.append((name, position))
                                logging.info(f"✅ Table: {name} | {position}")

        # METHOD 1.2: Check div/section containers: วนลูปหา Element Container
        # (div/section/article) ที่มี Class Name ที่บ่งบอกว่าเป็นข้อมูลผู้บริหาร
        # (เช่น executive, management, board) และพยายามจับคู่ข้อความที่อยู่ติดกัน
        containers = soup.find_all(['div', 'section', 'article'], 
                                   class_=re.compile(r'(executive|management|board|director|team|profile|member|card)', re.I))
        
        logging.info(f"📦 Found {len(containers)} potential executive containers")
        
        for container in containers:
            # Extract text from immediate children only (not nested deeply)
            texts = []
            # วนลูปตรวจสอบ Element ลูกโดยตรงของ Container
            for child in container.find_all(recursive=False):
                child_text = child.get_text(strip=True)
                if child_text and len(child_text) > 5:
                    texts.append(re.sub(r'\s+', ' ', child_text))
            
            # Also check direct text content
            for element in container.find_all(['p', 'span', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'div']):
                text = element.get_text(strip=True)
                if text and len(text) > 5 and len(text) < 150:  # Limit length
                    text = re.sub(r'\s+', ' ', text)
                    if text not in texts:
                        texts.append(text)
            
            # Process texts
            i = 0
            # วนลูปตรวจสอบข้อความที่ดึงมา
            while i < len(texts):
                text = texts[i]
                # ตรวจสอบว่าข้อความเป็นชื่อผู้บริหารหรือไม่
                if self._is_valid_thai_name(text):
                    name = text
                    position = "ไม่ระบุ"
                    
                    # ตรวจสอบข้อความถัดไปเพื่อหาตำแหน่ง
                    for j in range(i+1, min(i+4, len(texts))):
                        # หากข้อความถัดไปเป็นตำแหน่งที่ถูกต้องและไม่เป็นชื่อคน ให้กำหนดให้เป็น position
                        if self._is_valid_position(texts[j]) and not self._is_valid_thai_name(texts[j]):
                            position = texts[j]
                            break
                        
            
                    # ตรวจสอบว่าชื่อนี้ยังไม่เคยถูกบันทึกไว้ใน executives หรือไม่ (ป้องกันชื่อซ้ำ)
                    if not any(name == existing_name for existing_name, _ in executives):
                        executives.append((name, position)) # เพิ่มชื่อและตำแหน่งลงในรายชื่อผู้บริหาร
                        logging.info(f"✅ Container: {name} | {position}") 
                    
                    # ขยับไปยังข้อความถัดไป
                    i += 1
                else: # หากไม่ใช่ชื่อผู้บริหาร ให้ขยับไปยังข้อความถัดไป
                    i += 1
        
        # METHOD 1.3: List items: วนลูปหาข้อมูลใน Element <li> หรือ <dt> 
        # (รายการ List) และใช้วิธีคล้ายกันในการแยกชื่อและตำแหน่ง
        list_items = soup.find_all(['li', 'dt'])
        logging.info(f"📋 Found {len(list_items)} list items")
        
        # วนลูปตรวจสอบแต่ละรายการใน List
        for item in list_items:
            item_text = item.get_text(strip=True) # ดึงข้อความจากรายการ
            item_text = re.sub(r'\s+', ' ', item_text) # ลบช่องว่างส่วนเกิน
            
            # แยกข้อความออกจากกันด้วยการขึ้นบรรทัดใหม่ (\n หรือ \r) 
            # เพื่อจัดการกรณีที่ชื่อและตำแหน่งอยู่ใน Element เดียวกันแต่คั่นด้วยบรรทัดใหม่
            parts = re.split(r'[\n\r]+', item_text)
            parts = [p.strip() for p in parts if p.strip()]

            # วนลูปตรวจสอบข้อความที่แยกออกมา
            for part in parts:
                if self._is_valid_thai_name(part): # ตรวจสอบว่าข้อความเป็นชื่อผู้บริหารหรือไม่
                    name = part
                    position = "ไม่ระบุ"
                    
                    # ค้นหาตำแหน่งจากข้อความอื่น ๆ ในรายการเดียวกัน
                    for other_part in parts:
                        # ข้ามข้อความที่เป็นชื่อเพราะต้องการหาตำแหน่ง
                        if other_part != name and self._is_valid_position(other_part):
                            position = other_part # กำหนดตำแหน่ง
                            break

                    # ตรวจสอบว่าชื่อนี้ยังไม่เคยถูกบันทึกไว้ใน executives หรือไม่ (ป้องกันชื่อซ้ำ)
                    if not any(name == existing_name for existing_name, _ in executives):
                        executives.append((name, position))
                        logging.info(f"✅ List: {name} | {position}")
        
        logging.info(f"\n📊 Total executives found: {len(executives)}")

        # แสดงผล 10 รายการแรก for debugging
        if executives:
            logging.info("\n🔍 First 10 executives found:")
            for i, (name, position) in enumerate(executives[:10]):
                logging.info(f"  {i+1}. {name} - {position}")
        
        return executives

    # ฟังก์ชันแยกองค์ประกอบชื่อ
    def _parse_name_components(self, full_name: str) -> Tuple[str, str, str, str]:
        """Parse name into prefix, first name, and surname"""
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
        
        prefix = ""
        name_without_prefix = full_name
        
        # วนลูปเพื่อตรวจจับและดึงคำนำหน้าออกจากชื่อเต็ม
        for thai_title, eng_title in sorted(title_map.items(), key=lambda x: len(x[0]), reverse=True):
            if full_name.startswith(thai_title):
                prefix = eng_title
                name_without_prefix = full_name[len(thai_title):].strip()
                break
        
        # กรณีที่คำนำหน้าไม่ได้อยู่ต้นชื่อ (เช่น "สมชาย นาย ใจดี")
        if not prefix and any(0x0E00 <= ord(char) <= 0x0E7F for char in full_name):
            for thai_title, eng_title in title_map.items():
                if thai_title in full_name and full_name.index(thai_title) < 10:
                    prefix = eng_title
                    name_without_prefix = full_name.replace(thai_title, '').strip()
                    break
        
        name_without_prefix = re.sub(r'\s+', ' ', name_without_prefix).strip()
        
        # แยกชื่อที่ไม่มีคำนำหน้าออกเป็นส่วนต่าง ๆ เพื่อหาชื่อจริง (parts[0]) และนามสกุล (parts[1:])
        parts = name_without_prefix.split()
        
        if len(parts) == 0:
            return prefix, full_name, "", ""
        elif len(parts) == 1:
            return prefix, full_name, parts[0], ""
        elif len(parts) == 2:
            return prefix, full_name, parts[0], parts[1]
        else:
            first_name = parts[0]
            surname = " ".join(parts[1:])
            return prefix, full_name, first_name, surname


    def create_executive_records(self, executives: List[Tuple[str, str]]) -> List[Dict]:
        """ฟังก์ชันสำหรับสร้าง List ของ Dictionary (Records) จากข้อมูล Tuple ที่ได้จากการ Scrape"""
        records = []
        seen_names = set()
        
        logging.info("\n🔍 Creating executive records...")
        
        #กันชื่อซ้ำ
        for name, position in executives:
            if name in seen_names:
                logging.debug(f"  ⚠️ Skipping duplicate: {name}")
                continue
            
            # Additional filtering for non-name texts
            name_lower = name.lower()
            skip_keywords = ['สงวนลิขสิทธิ์', 'ลิขสิทธิ์', 'บมจ', 'บริษัท', 'copyright', '©']
            if any(keyword in name_lower for keyword in skip_keywords):
                logging.debug(f"  ⚠️ Skipping non-name text: {name}")
                continue
            
            # Check for year
            if re.search(r'25\d{2}', name):
                logging.debug(f"  ⚠️ Skipping text with year: {name}")
                continue
            
            # เรียกใช้ฟังก์ชันแยกองค์ประกอบชื่อ
            prefix, full_name, first_name, surname = self._parse_name_components(name)
            
            if not first_name:
                logging.warning(f"  ⚠️ Could not parse name: {name}")
                continue
            
            # Validate name length
            if len(first_name) < 2 or len(first_name) > 50:
                logging.debug(f"  ⚠️ Invalid name length: {first_name}")
                continue
            
            # สร้าง Dictionary Record ที่มี Field ข้อมูลที่ต้องการ 7 Field
            record = {
                "BUSI_DT": self.busi_dt,
                "Prefixed_Name": prefix,
                "Full_Name": full_name,
                "First_Name": first_name,
                "Surname": surname,
                "Bank_Name": self.bank_name,
                "Position": position
            }
            
            records.append(record)
            seen_names.add(name)
            logging.info(f"  ✅ {prefix} | {first_name} {surname} | {position}")
        
        if records:
            logging.info("\n🔍 First 3 records structure:")
            for i, record in enumerate(records[:3]):
                logging.info(f"  {i+1}. Prefixed: '{record['Prefixed_Name']}' | First: '{record['First_Name']}' | Last: '{record['Surname']}'")
        
        return records


    # Main scraping function
    def intelligent_scrape(self, limit: int = 150) -> List[Dict]:
        logging.info("🚀 Starting scraping process...")
        
        # Fetch page
        html_content = self.fetch_page_content(self.base_url)
        if not html_content:
            logging.error("❌ Failed to fetch page content")
            return []
        
        # Detect bank
        self.bank_name = self.detect_bank_name(self.base_url, html_content)
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
        return records[:limit] # ส่งคืน Records ที่สร้างเสร็จแล้ว (จำกัดจำนวน Record ไม่เกิน 150)

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
        df = pd.DataFrame(data) # Create Pandas DataFrame from list of dictionaries
        

        # กำหนดและจัดเรียงลำดับ Column ให้ถูกต้อง
        column_order = ['BUSI_DT', 'Prefixed_Name', 'Full_Name', 
                       'First_Name', 'Surname', 'Bank_Name', 'Position']
        
        for col in column_order:
            if col not in df.columns:
                df[col] = ""
        
        df = df[column_order]
        
        # Clean bank name for filename
        bank_short = bank_name.replace('ธนาคาร', '').strip()
        
        """
        แปลงชื่อธนาคารภาษาไทยให้เป็นชื่อย่อภาษาอังกฤษ (เช่น 'ธนาคารกสิกรไทย' เป็น 'Kbank') 
        เพื่อใช้ในการตั้งชื่อไฟล์ CSV (เช่น Kbank_20251028.csv)
        """

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
        }
        
        file_bank_name = bank_short
        for thai_name, eng_name in bank_name_map.items():
            if thai_name in bank_short:
                file_bank_name = eng_name
                break
        
        date_str = busi_dt.replace('-', '')
        filename = f"{file_bank_name}_{date_str}.csv"
        
        # Create output directory
        os.makedirs('output', exist_ok=True)
        output_path = os.path.join('output', filename)
        
        # Save CSV with proper encoding
        df.to_csv(output_path, index=False, encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
        
        logging.info(f"\n✅ File saved: {output_path}")
        logging.info(f"📊 Total records: {len(df)}")
        
        print("\n" + "="*120)
        print(f"📊 RESULTS FOR {bank_name}")
        print(f"📅 Date: {busi_dt}")
        print(f"📁 File: {output_path}")
        print(f"📈 Records: {len(df)}")
        print("="*120)
        
        print("\n📋 SAMPLE DATA (first 5 records):")
        sample_df = df.head().copy()
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
    print("🤖 IMPROVED BANK EXECUTIVE SCRAPER v2.0")
    print("="*120)
    print("✅ Better HTML structure handling")
    print("✅ Improved name/position separation")
    print("✅ Prevents concatenated names issue")
    print("✅ Works with various website structures")
    print("="*120 + "\n")
    
    # List of URLs to scrape
    urls = [
        "https://www.bangkokbank.com/th-TH/About-Us/Board-Directors"
        ]
    
    print("📋 URLs to scrape:")
    for i, url in enumerate(urls, 1):
        print(f"  {i}. {url}")
    print()
    
    all_results = []
    
    """ 
    วนลูปเรียกใช้ Class FlexibleBankScraper และเรียกใช้ฟังก์ชัน 
    intelligent_scrape() เพื่อดึงข้อมูลสำหรับแต่ละ URL
    """
    for url in urls:
        print(f"\n{'='*120}")
        print(f"🌐 Processing: {url}")
        print(f"{'='*120}\n")
        
        scraper = None
        
        try:
            scraper = FlexibleBankScraper(url)
            
            print(f"🌐 Target URL: {url}")
            print(f"📅 Date: {scraper.busi_dt}")
            
            temp_bank = scraper.detect_bank_name(url, None)
            print(f"🏦 Initial bank detection: {temp_bank}\n")
            
            # Run scraping
            executives = scraper.intelligent_scrape()
            
            if executives:
                print(f"\n🏦 Final detected bank: {scraper.bank_name}")
                
                # Save to CSV
                if save_to_csv(executives, scraper.bank_name, scraper.busi_dt):
                    print(f"\n✅ SUCCESS: Extracted {len(executives)} executives from {scraper.bank_name}")
                    all_results.append({
                        'bank': scraper.bank_name,
                        'count': len(executives),
                        'url': url
                    })
                else:
                    print(f"\n⚠️ WARNING: Data extracted but failed to save CSV")
            else:
                print(f"\n❌ FAILED: No executives found for {url}")
            
        except KeyboardInterrupt:
            print("\n⚠️ Interrupted by user")
            break
        except Exception as e:
            logging.error(f"❌ Error processing {url}: {e}")
            import traceback
            traceback.print_exc()
        finally: # Ensure WebDriver is closed
            if scraper:
                try:
                    scraper.close()
                except:
                    pass
    
    # Summary
    print("\n" + "="*120)
    print("📋 SCRAPING SUMMARY")
    print("="*120)
    
    if all_results:
        print(f"\n✅ Successfully scraped {len(all_results)} bank(s):\n")
        for i, result in enumerate(all_results, 1):
            print(f"  {i}. {result['bank']}: {result['count']} executives")
            print(f"     URL: {result['url']}\n")
    else:
        print("\n❌ No banks were successfully scraped")
    
    print("="*120)
    print("\n💡 TIP: Check the 'output' folder for generated CSV files")
    print("💡 TIP: Check 'debug_page.html' if you need to inspect the page structure")
    print("💡 TIP: The improved version prevents name concatenation issues")
    print("="*120 + "\n")


if __name__ == "__main__":
    main()
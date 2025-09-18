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
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class OllamaAI:
    """Class to interact with Ollama API for AI-powered analysis"""

    def __init__(self, base_url="http://localhost:11434", model="llama3.2"):
        self.base_url = base_url
        self.model = model
        
    def generate_response(self, prompt: str, system_prompt: str = "") -> str:
        """
        Generate response using Ollama API
        
        Parameters:
            prompt (str): User prompt
            system_prompt (str): System prompt for context
            
        Returns:
            str: AI response
        """
        try:
            url = f"{self.base_url}/api/generate"
            
            payload = {
                "model": self.model,
                "prompt": prompt,
                "system": system_prompt,
                "stream": False,
                "options": {
                    "temperature": 0.1,  # Low temperature for consistent parsing
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
            response = requests.get(f"{self.base_url}/api/tags", timeout=10)
            return response.status_code == 200
        except:
            return False


class IntelligentKBankScraper:
    """AI-powered adaptive web scraper for Kasikorn Bank executives"""
    
    def __init__(self, base_url="https://www.kasikornbank.com/th/about/Pages/executives.aspx"):
        self.base_url = base_url
        self.driver = None
        self.ai = OllamaAI()
        
        # Check if Ollama is running
        if not self.ai.test_connection():
            logging.warning("Ollama not accessible. Make sure Ollama is running on localhost:11434")
            
    def setup_driver(self) -> bool:
        """Setup Selenium WebDriver with stealth options"""
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-notifications")
            chrome_options.add_argument("--disable-popup-blocking")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Random user agent
            user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0"
            ]
            chrome_options.add_argument(f"user-agent={random.choice(user_agents)}")
            
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(60)
            
            # Execute script to hide automation indicators
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            logging.info("Selenium WebDriver setup completed successfully")
            return True
            
        except Exception as e:
            logging.error(f"Error setting up Selenium WebDriver: {e}")
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
                
                # Wait for page load
                time.sleep(random.uniform(3, 6))
                
                # Wait for body element
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                page_source = self.driver.page_source
                if len(page_source) > 500:
                    logging.info(f"Successfully fetched page (length: {len(page_source)} characters)")
                    return page_source
                else:
                    logging.warning(f"Page source too short ({len(page_source)} chars)")
                
            except Exception as e:
                logging.warning(f"Error on attempt {attempt+1}: {e}")
                
            if attempt < retries - 1:
                backoff_time = 5 * (attempt + 1) + random.uniform(1, 3)
                logging.info(f"Retrying in {backoff_time:.2f} seconds...")
                time.sleep(backoff_time)
        
        logging.error(f"Failed to fetch page after {retries} attempts")
        return None

    def analyze_page_structure(self, html_content: str) -> Dict:
        """Use AI to analyze the page structure and identify executive data"""
        
        system_prompt = """You are an expert web scraper analyst. Your task is to analyze HTML content and identify where executive/leadership information is located on a webpage.

You should look for:
1. Tables containing executive names and positions/titles
2. Div sections with executive profiles
3. Lists of leadership team members with their roles
4. Any structured data about company executives and their job titles

Focus on extracting:
- Full names (with titles like นาย, นาง, ดร.)
- Detailed position titles (ประธาน, ผู้ช่วยผู้จัดการ, ผู้จัดการ, ผู้บริหารกลุ่มธุรกิจ, etc.)

Return your analysis in this exact JSON format:
{
    "data_location": "description of where the data is found",
    "html_selectors": ["list", "of", "css", "selectors", "to", "try"],
    "data_structure": "description of how the data is organized",
    "extraction_strategy": "recommended approach for extracting names and positions"
}

Be specific and practical in your recommendations."""

        # Truncate HTML to avoid token limits
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()
            
        # Get text content with some structure
        simplified_html = str(soup)[:8000]  # Limit to first 8000 chars
        
        prompt = f"""Analyze this HTML content from Kasikorn Bank's executive page and identify where executive information is located:

HTML Content:
{simplified_html}

Provide analysis in the requested JSON format."""

        response = self.ai.generate_response(prompt, system_prompt)
        
        try:
            # Try to extract JSON from response
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
        except:
            pass
            
        # Fallback structure analysis
        return {
            "data_location": "Tables and div elements",
            "html_selectors": [
                "table tr td", 
                ".ms-rteTable-default tr td",
                "div.executive-profile",
                "div.profile-card",
                "table.executive-table tr td"
            ],
            "data_structure": "Likely in table format with columns for name, position, dates",
            "extraction_strategy": "Extract from table rows or structured div elements"
        }

    def extract_executives_with_ai(self, html_content: str, structure_info: Dict) -> List[Dict]:
        """Use AI to extract executive information from HTML"""
        
        soup = BeautifulSoup(html_content, 'html.parser')
        executives = []
        
        # Try the suggested selectors first
        selectors = structure_info.get("html_selectors", [])
        
        # Add specific selectors for image-based profiles
        image_based_selectors = [
            "div.ms-rtestate-field",
            "div.profile-container", 
            "div.person-card",
            "div.staff-card",
            "div.member-card",
            "div[class*='executive']",
            "div[class*='profile']",
            "div[class*='member']",
            "div[class*='staff']",
            "div[class*='person']"
        ]
        
        all_selectors = selectors + image_based_selectors
        
        for selector in all_selectors:
            try:
                elements = soup.select(selector)
                if elements:
                    logging.info(f"Found {len(elements)} elements with selector: {selector}")
                    
                    # Get sample content for AI analysis
                    sample_content = ""
                    for i, elem in enumerate(elements[:15]):  # Take first 15 for analysis
                        sample_content += f"Element {i+1}: {elem.get_text(strip=True)}\n"
                    
                    # Ask AI to extract structured data focusing on positions
                    extraction_prompt = f"""Extract executive information from this content. Focus on getting detailed position titles in Thai.

Look for executives with positions like:
- ประธาน (President/Chairman)
- ผู้ช่วยผู้จัดการ (Assistant Manager)
- ผู้จัดการ (Manager)  
- ผู้บริหารกลุ่มธุรกิจ (Business Group Executive)
- กรรมการ (Director)
- รองประธาน (Vice President)
- ผู้อำนวยการ (Executive Director)

Content to analyze:
{sample_content}

Return the data in this exact JSON format (array of objects):
[
  {{
    "full_name": "complete name with title (นาย, นาง, ดร., etc.)",
    "position": "detailed job title/position in Thai"
  }}
]

Focus on accurate position titles. Ignore dates. If no clear executive data is found, return an empty array []."""

                    response = self.ai.generate_response(extraction_prompt)
                    
                    try:
                        # Extract JSON from response
                        json_match = re.search(r'\[.*\]', response, re.DOTALL)
                        if json_match:
                            ai_extracted = json.loads(json_match.group())
                            if ai_extracted:
                                executives.extend(self.process_ai_extracted_data(ai_extracted))
                                if len(executives) > 0:  # Continue looking for more data
                                    continue
                    except Exception as e:
                        logging.warning(f"Error parsing AI response: {e}")
                        
            except Exception as e:
                logging.warning(f"Error with selector {selector}: {e}")
        
        # Also try to extract image-based profiles specifically
        image_executives = self.extract_image_based_profiles(soup)
        if image_executives:
            executives.extend(image_executives)
        
        return executives

    def extract_top_image_profiles(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract executives from the top image-based profiles specifically"""
        executives = []
        
        try:
            # ใช้ selector ที่เฉพาะเจาะจงกับส่วนภาพด้านบน
            top_profile_selectors = [
                "div.leader-profile", 
                "div.executive-header",
                "div.top-executives",
                "div[class*='banner'] div.profile",
                "div.main-content div.profile-container",
                # เพิ่ม selector ตามโครงสร้างที่เห็นใน screenshot
                "div:has(> img) + div",  # div ที่ตามหลัง img โดยตรง
                "figure + div",  # div ที่ตามหลัง figure
            ]
            
            for selector in top_profile_selectors:
                try:
                    elements = soup.select(selector)
                    if elements:
                        logging.info(f"Found {len(elements)} top image profiles with selector: {selector}")
                        
                        for elem in elements:
                            text_content = elem.get_text(separator='\n', strip=True)
                            if text_content and len(text_content) > 10:
                                exec_data = self.analyze_profile_text_with_ai(text_content)
                                if exec_data:
                                    executives.append(exec_data)
                except Exception as e:
                    logging.warning(f"Error with top profile selector {selector}: {e}")
                    continue
            
            # หากยังไม่พบ ลองหา img และดู parent/sibling elements
            images = soup.find_all('img', src=True)
            for img in images:
                try:
                    # ดูเฉพาะภาพที่อาจเป็นภาพโปรไฟล์
                    img_src = img['src'].lower()
                    if any(keyword in img_src for keyword in ['executive', 'profile', 'leader', 'management', 'director']):
                        # ดู parent container
                        parent = img.find_parent()
                        if parent:
                            parent_text = parent.get_text(separator='\n', strip=True)
                            if parent_text and len(parent_text) > 20:
                                exec_data = self.analyze_profile_text_with_ai(parent_text)
                                if exec_data:
                                    executives.append(exec_data)
                except:
                    continue
                    
        except Exception as e:
            logging.error(f"Error extracting top image profiles: {e}")
        
        return executives

    def extract_from_text_patterns(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract executives from specific text patterns found in screenshot"""
        executives = []
        
        try:
            # หา text ที่มีรูปแบบเหมือนใน screenshot
            text_patterns = [
                r"นายสาวยัตติยา อินทรวิธัย[\s\S]*?ประธานเจ้าหน้าที่บริหาร",
                r"ดร\. พิพัฒน์พงศ์ โปษยานนท์[\s\S]*?ผู้จัดการใหญ่",
                r"นายจงรัก รัตนเพียร[\s\S]*?ผู้จัดการใหญ่", 
                r"นายรุ่งเรือง สุขเกิดกิจพิบูลย์[\s\S]*?ผู้จัดการใหญ่"
            ]
            
            all_text = soup.get_text()
            
            for pattern in text_patterns:
                matches = re.finditer(pattern, all_text, re.IGNORECASE)
                for match in matches:
                    text_block = match.group(0)
                    exec_data = self.analyze_profile_text_with_ai(text_block)
                    if exec_data:
                        executives.append(exec_data)
                        
        except Exception as e:
            logging.error(f"Error extracting from text patterns: {e}")
        
        return executives

    def extract_image_based_profiles(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract executives from image-based profile layouts"""
        executives = []
        
        # เรียกฟังก์ชันสำหรับส่วนภาพด้านบน
        top_executives = self.extract_top_image_profiles(soup)
        if top_executives:
            executives.extend(top_executives)
        
        # เรียกฟังก์ชันสำหรับรูปแบบ text จาก screenshot
        pattern_executives = self.extract_from_text_patterns(soup)
        if pattern_executives:
            executives.extend(pattern_executives)
        
        try:
            # Look for common image-based profile patterns
            profile_patterns = [
                # Divs that might contain image + text combinations
                "div img ~ div",  # Div that follows an image
                "div img + div",  # Div immediately after an image
                "figure + figcaption",  # Figure captions
                "div.profile img ~ *",  # Any element after image in profile div
                "div[class*='card'] img ~ *",  # Card-based layouts
                "div[class*='member'] img ~ *",  # Member layouts
                # Find all divs that contain both img and text
                "div:has(img)",
            ]
            
            for pattern in profile_patterns:
                try:
                    elements = soup.select(pattern)
                    if elements:
                        logging.info(f"Found {len(elements)} image-based profiles with pattern: {pattern}")
                        
                        for elem in elements:
                            # Get all text content from the element and its children
                            text_content = elem.get_text(separator='\n', strip=True)
                            
                            if text_content and len(text_content) > 10:
                                # Use AI to analyze this text for executive information
                                exec_data = self.analyze_profile_text_with_ai(text_content)
                                if exec_data:
                                    executives.append(exec_data)
                                    
                except Exception as e:
                    logging.warning(f"Error with image pattern {pattern}: {e}")
                    continue
            
            # Also try finding parent containers of images
            images = soup.find_all('img')
            for img in images:
                try:
                    # Look at parent containers of images
                    parent = img.find_parent()
                    if parent:
                        # Get siblings or nearby text
                        text_elements = []
                        
                        # Check siblings
                        for sibling in parent.find_next_siblings():
                            sibling_text = sibling.get_text(strip=True)
                            if sibling_text and len(sibling_text) > 3:
                                text_elements.append(sibling_text)
                        
                        # Check parent's text content
                        parent_text = parent.get_text(separator='\n', strip=True)
                        if parent_text and len(parent_text) > 10:
                            text_elements.append(parent_text)
                        
                        # Analyze collected text
                        if text_elements:
                            combined_text = '\n'.join(text_elements[:3])  # Limit to avoid noise
                            exec_data = self.analyze_profile_text_with_ai(combined_text)
                            if exec_data and exec_data not in executives:
                                executives.append(exec_data)
                                
                except Exception as e:
                    continue
                    
        except Exception as e:
            logging.error(f"Error in image-based extraction: {e}")
        
        # Remove duplicates
        unique_executives = []
        seen_names = set()
        for exec_data in executives:
            name_key = exec_data.get('Full_Name', '').lower().strip()
            if name_key and name_key not in seen_names:
                seen_names.add(name_key)
                unique_executives.append(exec_data)
        
        logging.info(f"Extracted {len(unique_executives)} unique image-based profiles")
        return unique_executives

    def analyze_profile_text_with_ai(self, text_content: str) -> Optional[Dict]:
        """Use AI to analyze text content from image-based profiles"""
        
        if not text_content or len(text_content.strip()) < 5:
            return None
            
        prompt = f"""Analyze this text content from a bank executive profile and extract the person's information:

Text Content:
{text_content}

Look for:
1. Full name (with Thai titles like นาย, นาง, ดร., etc.)
2. Job position/title in Thai (like ประธาน, ผู้จัดการ, กรรมการ, etc.)

This text might contain:
- Name and position in separate lines
- Position first, then name
- Name first, then position
- Mixed content with other information

Return in this exact JSON format:
{{
  "full_name": "complete name with title",
  "position": "detailed job title in Thai"
}}

Only return data if you can clearly identify both a person's name and their executive position. If the text doesn't contain clear executive information, return empty object {{}}."""

        response = self.ai.generate_response(prompt)
        
        try:
            # Extract JSON from response
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                data = json.loads(json_match.group())
                
                full_name = data.get("full_name", "").strip()
                position = data.get("position", "").strip()
                
                if full_name and position and len(full_name) > 3:
                    # Process the extracted data
                    prefix = self.extract_prefix(full_name)
                    first_name, surname = self.parse_name(full_name)
                    enhanced_position = self.enhance_position_with_ai(position)
                    
                    return {
                        "Prefixed_Name": prefix,
                        "Full_Name": full_name,
                        "First_Name": first_name,
                        "Surname": surname,
                        "Position": enhanced_position
                    }
                    
        except Exception as e:
            logging.warning(f"Error parsing profile text AI response: {e}")
            
        return None

    def process_ai_extracted_data(self, ai_data: List[Dict]) -> List[Dict]:
        """Process and normalize AI-extracted data"""
        processed = []
        
        for item in ai_data:
            try:
                full_name = item.get("full_name", "").strip()
                position = item.get("position", "").strip()
                
                if not full_name:
                    continue
                
                # Extract prefix
                prefix = self.extract_prefix(full_name)
                
                # Parse name
                first_name, surname = self.parse_name(full_name)
                
                # Clean and enhance position with AI
                enhanced_position = self.enhance_position_with_ai(position)
                
                processed.append({
                    "Prefixed_Name": prefix,
                    "Full_Name": full_name,
                    "First_Name": first_name,
                    "Surname": surname,
                    "Position": enhanced_position
                })
                
            except Exception as e:
                logging.error(f"Error processing AI data item: {e}")
                
        return processed

    def extract_prefix(self, full_name: str) -> str:
        """Extract prefix from full name using AI assistance"""
        if not full_name:
            return ""
            
        prompt = f"""Extract only the title/prefix from this name: "{full_name}"

Common titles include: Mr, Mrs, Ms, Dr, Prof, etc. (in Thai or English)

Return only the title/prefix, nothing else. If no title is found, return empty string."""

        response = self.ai.generate_response(prompt)
        
        # Clean the response
        prefix = response.strip().strip('"').strip("'")
        
        # Fallback to manual extraction if AI response is unclear
        if len(prefix) > 10 or not prefix:
            return self._manual_extract_prefix(full_name)
            
        return prefix

    def _manual_extract_prefix(self, full_name: str) -> str:
        """Fallback manual prefix extraction"""
        titles = {
            "นาย": "Mr", "นาง": "Mrs", "นางสาว": "Ms",
            "ดร.": "Dr", "ศ.": "Prof", "รศ.": "Assoc Prof",
            "Mr.": "Mr", "Mrs.": "Mrs", "Miss": "Ms", "Dr.": "Dr"
        }
        
        for title in sorted(titles.keys(), key=len, reverse=True):
            if full_name.startswith(title):
                return titles[title]
        return ""

    def parse_name(self, full_name: str) -> tuple:
        """Parse name into first name and surname using AI"""
        if not full_name:
            return "", ""
            
        prompt = f"""Parse this Thai name into first name and surname: "{full_name}"

Remove any title/prefix (Mr, Mrs, Dr, นาย, นาง, etc.) and separate into:
- First name (given name)
- Surname (family name)

Return in this exact format:
First Name: [first name]
Surname: [surname]

If only one name part exists, put it as first name and leave surname empty."""

        response = self.ai.generate_response(prompt)
        
        try:
            # Extract first name and surname from AI response
            first_match = re.search(r'First Name:\s*(.+)', response)
            surname_match = re.search(r'Surname:\s*(.+)', response)
            
            first_name = first_match.group(1).strip() if first_match else ""
            surname = surname_match.group(1).strip() if surname_match else ""
            
            # Clean up responses
            first_name = first_name.strip('"').strip("'").strip()
            surname = surname.strip('"').strip("'").strip()
            
            return first_name, surname
            
        except:
            # Fallback to manual parsing
            return self._manual_parse_name(full_name)

    def _manual_parse_name(self, full_name: str) -> tuple:
        """Fallback manual name parsing"""
        # Remove common titles
        titles = ["นาย", "นาง", "นางสาว", "ดร.", "ศ.", "รศ.", "ผศ.", 
                 "Mr.", "Mrs.", "Miss", "Dr.", "Prof."]
        
        name = full_name
        for title in sorted(titles, key=len, reverse=True):
            if name.startswith(title):
                name = name[len(title):].strip()
                break
        
        parts = name.split()
        if len(parts) == 1:
            return parts[0], ""
        elif len(parts) >= 2:
            return parts[0], " ".join(parts[1:])
        
        return "", ""

    def enhance_position_with_ai(self, position: str) -> str:
        """Use AI to clean and enhance position titles"""
        if not position:
            return ""
            
        prompt = f"""Clean and standardize this Thai job position title: "{position}"

Common executive positions include:
- ประธาน (President/Chairman)
- รองประธาน (Vice President)  
- ผู้จัดการ (Manager)
- ผู้ช่วยผู้จัดการ (Assistant Manager)
- ผู้บริหารกลุ่มธุรกิจ (Business Group Executive)
- ผู้อำนวยการ (Executive Director)
- กรรมการ (Director)
- กรรมการผู้จัดการ (Managing Director)
- ผู้บริหารสูงสุด (Chief Executive)

Return only the cleaned position title in Thai. Remove any extra text, dates, or unnecessary words."""

        response = self.ai.generate_response(prompt)
        
        # Clean the response
        cleaned_position = response.strip().strip('"').strip("'")
        
        # If AI response is too long or seems wrong, return original
        if len(cleaned_position) > 100 or not cleaned_position:
            return position
            
        return cleaned_position

    def normalize_date(self, date_str: str) -> str:
        """This function is now deprecated - dates are no longer used"""
        return ""

    def intelligent_scrape(self, limit: int = 100) -> List[Dict]:
        """Main intelligent scraping function"""
        logging.info("Starting intelligent scraping of Kasikorn Bank executives")
        
        # Step 1: Fetch page content
        html_content = self.fetch_page_content(self.base_url)
        if not html_content:
            logging.error("Failed to fetch page content")
            return []
        
        # Step 2: Analyze page structure with AI
        logging.info("Analyzing page structure with AI...")
        structure_info = self.analyze_page_structure(html_content)
        logging.info(f"AI Analysis: {structure_info}")
        
        # Step 3: Extract executives using AI guidance
        logging.info("Extracting executive data with AI assistance...")
        executives = self.extract_executives_with_ai(html_content, structure_info)
        
        # Step 3.5: Extract specifically from image-based profiles
        soup = BeautifulSoup(html_content, 'html.parser')
        image_executives = self.extract_image_based_profiles(soup)
        if image_executives:
            executives.extend(image_executives)
        
        # Step 4: If AI extraction fails, fall back to traditional scraping
        if not executives:
            logging.info("AI extraction failed, falling back to traditional scraping...")
            executives = self.fallback_scrape(html_content)
        
        # Step 5: AI validation and enhancement
        if executives:
            executives = self.ai_validate_and_enhance(executives)
        
        logging.info(f"Successfully extracted {len(executives)} executives")
        return executives[:limit]

    def fallback_scrape(self, html_content: str) -> List[Dict]:
        """Fallback to traditional scraping methods"""
        soup = BeautifulSoup(html_content, 'html.parser')
        executives = []
        
        # Try multiple selector strategies
        selectors = [
            "table tr td",
            ".ms-rteTable-default tr",
            "div.executive-profile",
            "div.profile-card",
            ".executive-item",
            "table.executive-table tr"
        ]
        
        for selector in selectors:
            try:
                elements = soup.select(selector)
                if elements:
                    logging.info(f"Fallback: Found {len(elements)} elements with {selector}")
                    
                    for elem in elements:
                        text = elem.get_text(strip=True)
                        if text and len(text) > 5:
                            # Simple heuristic: if text contains Thai characters and looks like a name
                            if re.search(r'[ก-ฮ]', text) and len(text.split()) >= 2:
                                lines = text.split('\n')
                                if lines:
                                    full_name = lines[0].strip()
                                    position = lines[1].strip() if len(lines) > 1 else ""
                                    
                                    # Enhance position with AI
                                    enhanced_position = self.enhance_position_with_ai(position)
                                    
                                    prefix = self._manual_extract_prefix(full_name)
                                    first_name, surname = self._manual_parse_name(full_name)
                                    
                                    executives.append({
                                        "Prefixed_Name": prefix,
                                        "Full_Name": full_name,
                                        "First_Name": first_name,
                                        "Surname": surname,
                                        "Position": enhanced_position
                                    })
                    
                    if executives:
                        break
                        
            except Exception as e:
                logging.warning(f"Error with fallback selector {selector}: {e}")
        
        return executives

    def ai_validate_and_enhance(self, executives: List[Dict]) -> List[Dict]:
        """Use AI to validate and enhance extracted data"""
        
        enhanced_executives = []
        
        for exec_data in executives:
            try:
                # Ask AI to validate and clean the data, focusing on position titles
                validation_prompt = f"""Validate and clean this executive data from Kasikorn Bank:

Full Name: {exec_data['Full_Name']}
Position: {exec_data['Position']}

Tasks:
1. Check if the full name looks like a real person's name
2. Clean and standardize the position title in Thai
3. Make sure the position title is appropriate for a bank executive

Common Thai executive positions:
- ประธาน, รองประธาน
- กรรมการ, กรรมการผู้จัดการ 
- ผู้จัดการ, ผู้ช่วยผู้จัดการ
- ผู้บริหารกลุ่มธุรกิจ
- ผู้อำนวยการ
- หัวหน้าฝ่าย

Return in this format:
Valid: Yes/No
Cleaned Full Name: [name]
Cleaned Position: [detailed position title in Thai]

Only return data for valid executives with proper banking positions."""

                response = self.ai.generate_response(validation_prompt)
                
                # Parse AI response
                if "Valid: Yes" in response:
                    # Extract cleaned data
                    name_match = re.search(r'Cleaned Full Name:\s*(.+)', response)
                    pos_match = re.search(r'Cleaned Position:\s*(.+)', response)
                    
                    if name_match:
                        cleaned_name = name_match.group(1).strip()
                        cleaned_position = pos_match.group(1).strip() if pos_match else exec_data['Position']
                        
                        # Update the executive data
                        prefix = self.extract_prefix(cleaned_name)
                        first_name, surname = self.parse_name(cleaned_name)
                        
                        enhanced_executives.append({
                            "Prefixed_Name": prefix,
                            "Full_Name": cleaned_name,
                            "First_Name": first_name,
                            "Surname": surname,
                            "Position": cleaned_position
                        })
                        
                else:
                    # If AI says invalid, still include but log warning
                    logging.warning(f"AI marked as invalid: {exec_data['Full_Name']}")
                    enhanced_executives.append({
                        "Prefixed_Name": exec_data['Prefixed_Name'],
                        "Full_Name": exec_data['Full_Name'],
                        "First_Name": exec_data['First_Name'],
                        "Surname": exec_data['Surname'],
                        "Position": exec_data['Position']
                    })
                    
            except Exception as e:
                logging.error(f"Error in AI validation: {e}")
                enhanced_executives.append({
                    "Prefixed_Name": exec_data['Prefixed_Name'],
                    "Full_Name": exec_data['Full_Name'],
                    "First_Name": exec_data['First_Name'],
                    "Surname": exec_data['Surname'],
                    "Position": exec_data['Position']
                })
        
        return enhanced_executives

    def adaptive_retry_with_ai(self, html_content: str) -> List[Dict]:
        """Use AI to suggest new scraping strategies when initial attempts fail"""
        
        system_prompt = """You are a web scraping expert. Analyze this HTML and suggest alternative scraping strategies when standard methods fail.

Look for:
1. Alternative CSS selectors that might contain executive data
2. Different HTML structures that could hold the information
3. JavaScript-generated content indicators
4. Alternative data organization patterns

Provide practical, specific suggestions."""

        prompt = f"""The standard scraping methods failed on this webpage. Suggest alternative approaches:

HTML Sample:
{html_content[:5000]}

What alternative selectors or strategies should I try to find executive information?"""

        response = self.ai.generate_response(prompt, system_prompt)
        
        # Extract suggested selectors from AI response
        suggested_selectors = re.findall(r'["\']([^"\']+)["\']', response)
        
        if suggested_selectors:
            logging.info(f"AI suggested alternative selectors: {suggested_selectors}")
            
            soup = BeautifulSoup(html_content, 'html.parser')
            executives = []
            
            for selector in suggested_selectors:
                try:
                    elements = soup.select(selector)
                    if elements:
                        # Process with AI
                        content = "\n".join([elem.get_text(strip=True) for elem in elements[:20]])
                        if content:
                            executives = self.extract_executives_with_ai(html_content, {
                                "html_selectors": [selector]
                            })
                            if executives:
                                break
                except:
                    continue
            
            return executives
        
        return []

    def close(self):
        """Close the WebDriver"""
        try:
            if self.driver:
                self.driver.quit()
                logging.info("WebDriver closed successfully")
        except Exception as e:
            logging.error(f"Error closing WebDriver: {e}")


def save_to_csv(data: List[Dict], filename: str = "ai_kasikorn_executives.csv") -> bool:
    """Save executive data to CSV file"""
    if not data:
        logging.warning("No data to save")
        return False

    try:
        df = pd.DataFrame(data)
        
        # Create output directory
        os.makedirs('output', exist_ok=True)
        output_path = os.path.join('output', filename)
        
        # Save to CSV with Thai language support
        df.to_csv(
            output_path,
            index=False,
            encoding='utf-8-sig',
            quoting=csv.QUOTE_ALL
        )
        
        logging.info(f"Successfully saved {len(data)} executives to {output_path}")
        
        # Print preview
        print("\nPreview of extracted data:")
        print(df.head().to_string(index=False))
        
        return True
        
    except Exception as e:
        logging.error(f"Error saving to CSV: {e}")
        return False


def main():
    """Main execution function"""
    print("🤖 AI-Powered Kasikorn Bank Executive Scraper")
    print("=" * 50)
    
    try:
        # Initialize the intelligent scraper
        scraper = IntelligentKBankScraper()
        
        # Test Ollama connection
        if not scraper.ai.test_connection():
            print("⚠️  Warning: Ollama not accessible. Some AI features may not work.")
            print("   Make sure Ollama is running: 'ollama serve'")
            print("   And you have a model installed: 'ollama pull llama3.2'")
        else:
            print("✅ Ollama connection successful")
        
        print("\n🔍 Starting intelligent scraping...")
        
        # Perform intelligent scraping
        executives = scraper.intelligent_scrape(limit=100)
        
        if executives:
            print(f"\n✅ Successfully extracted {len(executives)} executives")
            
            # Save to CSV
            if save_to_csv(executives):
                print("💾 Data saved successfully to output/ai_kasikorn_executives.csv")
            else:
                print("❌ Failed to save data")
        else:
            print("❌ No executive data could be extracted")
            
    except KeyboardInterrupt:
        print("\n⏹️  Scraping interrupted by user")
    except Exception as e:
        logging.error(f"Error in main execution: {e}")
        print(f"❌ Error: {e}")
    finally:
        # Cleanup
        if 'scraper' in locals():
            scraper.close()
        print("\n🏁 Scraping completed")


if __name__ == "__main__":
    main()

#ตำแหน่งยังไม่แสดง
#ชื่อที่มีภาพด้านบนที่ไม่ใช่ตรงตารางไม่ขึ้น
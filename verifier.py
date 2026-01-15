import requests
import json
import logging
from typing import List, Dict, Optional
from bs4 import BeautifulSoup 

logger = logging.getLogger(__name__)

# กำหนด URL และ Model
OLLAMA_API_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "llama3.2"

class Verifier:
    """
    เครื่องมือตรวจสอบความถูกต้องของข้อมูลที่ Scrape มาเทียบกับ Live Content
    - เพิ่มการแปลง HTML เป็น Text ล้วน เพื่อให้อ่านข้อมูลได้ครบทั้งหน้า
    """
    def __init__(self, ollama_url: str = OLLAMA_API_URL, model: str = OLLAMA_MODEL):
        self.ollama_url = ollama_url
        self.model = model
        logger.info(f"🧠 Verifier initialized: Model={self.model}, API={self.ollama_url}")

    def _format_scraped_data(self, scraped_data: List[Dict]) -> str:
        """จัดรูปแบบข้อมูลที่ Scrape มาให้ LLM อ่านง่าย"""
        if not scraped_data:
            return "No records were scraped."
        return "\n".join([f"- {r['Full_Name']} | {r['Position']}" for r in scraped_data])

    def _create_prompt(self, scraped_records_string: str, live_html_content: str, bank_name: str) -> str:
        """
        - เพิ่มการแปลง HTML เป็น Text ล้วน เพื่อให้ AI อ่านข้อมูลได้ครบทั้งหน้า
        """
        # 1. ใช้ BeautifulSoup แปลง HTML เป็น Text
        soup = BeautifulSoup(live_html_content, 'html.parser')
        
        # ลบส่วนที่ไม่จำเป็นออก
        for element in soup(["script", "style", "header", "footer", "nav", "noscript", "meta"]):
            element.decompose()
            
        # ดึงเฉพาะข้อความ และจัดระเบียบเว้นวรรค
        text_content = soup.get_text(" ", strip=True)
        # ลดช่องว่างที่ซ้ำซ้อนให้เหลือช่องเดียว
        text_content = " ".join(text_content.split())
        
        # 2. เพิ่ม Limit เป็น 15,000 ตัวอักษร
        live_snippet = text_content[:50000]

        return f"""
คุณคือผู้เชี่ยวชาญด้านการตรวจสอบข้อมูลองค์กร ที่มีความเข้มงวดสูงในการตรวจสอบความถูกต้องของข้อมูล

**งานของคุณ:**
ตรวจสอบความถูกต้องและครบถ้วนของรายชื่อผู้บริหารของ {bank_name} ที่ถูกดึงมาจากหน้าเว็บ

**### รายชื่อที่ถูก Scrape มา (Scraped List)**
{scraped_records_string}

**### เนื้อหาบนหน้าเว็บ (Page Content)**
{live_snippet}

**คำสั่งที่ต้องปฏิบัติอย่างเคร่งครัด:**

1. **ค้นหาผู้บริหารทั้งหมด** ในหน้าเว็บที่มี:
   - คำนำหน้าชื่อ: นาย/นาง/นางสาว/ดร./ศ./รศ./ผศ./พล./พัน/คุณหญิง/คุณ
   - ตำแหน่งสำคัญ: ประธาน/กรรมการ/ผู้บริหาร/ผู้จัดการ/Chief/President/Director/Manager

2. **เปรียบเทียบรายการ Live กับ Scraped List** อย่างละเอียด (ไม่สนใจลำดับ)

3. **ระบุรายชื่อที่ตกหล่น (Missing Names)** พร้อม:
   - ชื่อเต็ม (รวมคำนำหน้า)
   - ตำแหน่งงานที่ระบุในหน้าเว็บ (ต้องระบุให้ครบถ้วน)
   - ตรวจสอบให้แน่ใจว่าชื่อและตำแหน่งนี้มีอยู่จริงในหน้าเว็บ

4. **ระบุรายชื่อที่ไม่ถูกต้อง (Extra Names)** - ชื่อที่ดึงมาแต่ไม่มีใน Live Content

5. **ข้อกำหนดสำคัญสำหรับ missing_names:**
   - แต่ละรายการต้องเป็น object ที่มี 3 fields: full_name, position, confidence
   - full_name: ต้องเป็นชื่อเต็ม รวมคำนำหน้า (เช่น "นางขัตติยา อินทรวิชัย")
   - position: ต้องเป็นตำแหน่งเต็มที่ปรากฏในหน้าเว็บ (เช่น "กรรมการผู้จัดการใหญ่")
   - confidence: ระดับความมั่นใจ 0.0-1.0 (ต้อง >= 0.85 เท่านั้น)

**ตัวอย่าง JSON Output ที่ถูกต้อง:**
{{
  "is_complete": false,
  "missing_names": [
    {{
      "full_name": "นางขัตติยา อินทรวิชัย",
      "position": "ประธานเจ้าหน้าที่บริหาร",
      "confidence": 0.95
    }}
  ],
  "extra_names": [""]
}}

**หลักเกณฑ์การตัดสินใจ:**
- ถ้าข้อมูลไม่แน่ชัด confidence ต่ำกว่า 0.85 → อย่าใส่ในรายการ
- ถ้าชื่อใน Scraped List ใกล้เคียงกับชื่อใน Live (เช่น พิมพ์ผิดเล็กน้อย) → ไม่ถือว่า missing
- ถ้าตำแหน่งไม่ระบุชัดเจนในหน้าเว็บ → อย่าใส่ในรายการ

**คืนค่าเป็น JSON เท่านั้น โดยมี fields:**
- is_complete: boolean (true = ข้อมูล Scraped ครบถ้วน)
- missing_names: array of objects (ชื่อที่หายไปพร้อมตำแหน่งและ confidence)
- extra_names: array of strings (ชื่อที่ดึงมาเกิน)
        """

    def verify(self, scraped_data: List[Dict], live_html_content: str, bank_name: str) -> Dict:
        """
        ส่งข้อมูลไปให้ Ollama ตรวจสอบและรับผลลัพธ์ JSON
        """
        scraped_records_string = self._format_scraped_data(scraped_data)
        user_prompt = self._create_prompt(scraped_records_string, live_html_content, bank_name)

        payload = {
            "model": self.model,
            "prompt": user_prompt,
            "system": "You are a strict data validation expert. Extract missing executive names with their EXACT positions from the source content. Return ONLY valid JSON with full_name, position, and confidence (0.0-1.0) for each missing person. Only include entries with confidence >= 0.85.",
            "stream": False,
            "format": "json",
            "temperature": 0.1,

            "options": {
                "num_ctx": 16384  # รองรับ Token ได้เยอะขึ้นมาก (ประมาณ 60,000 chars)
            }
        }

        logger.info("⏳ Sending data to Ollama for verification...")
        
        try:
            response = requests.post(self.ollama_url, json=payload, timeout=180)
            response.raise_for_status()
            
            result_text = response.json().get('response', '{}')
            result = json.loads(result_text)
            result = self._validate_and_clean_result(result)
            
            logger.info("✅ Ollama verification completed.")
            return result

        except Exception as e:
            logger.error(f"❌ Ollama API Error: {e}")
            return {"error": str(e), "is_complete": False, "missing_names": [], "extra_names": []}

    def _validate_and_clean_result(self, result: Dict) -> Dict:
        """
        ตรวจสอบและทำความสะอาดผลลัพธ์จาก LLM พร้อมกรองชื่อสมมติ
        """
        if not isinstance(result, dict):
            return {"is_complete": False, "missing_names": [], "extra_names": []}
        
        missing_names = result.get('missing_names', [])
        cleaned_missing = []
        
        # Blacklist ชื่อตัวอย่างที่ชอบหลุดมา
        blacklisted_names = ["สมชาย ใจดี", "นายสมชาย ใจดี", "somchai jaidee"]
        
        for item in missing_names:
            if isinstance(item, dict) and 'full_name' in item and 'position' in item:
                full_name = str(item['full_name']).strip()
                
                if any(fake in full_name for fake in blacklisted_names):
                    continue

                confidence = item.get('confidence', 0.0)
                if confidence >= 0.80:
                    cleaned_item = {
                        'full_name': full_name,
                        'position': str(item['position']).strip(),
                        'confidence': float(confidence)
                    }
                    if cleaned_item['full_name'] and cleaned_item['position']:
                        cleaned_missing.append(cleaned_item)
        
        extra_names = result.get('extra_names', [])
        if not isinstance(extra_names, list): extra_names = []
        cleaned_extra = [str(name).strip() for name in extra_names if name]
        
        return {
            'is_complete': bool(result.get('is_complete', False)),
            'missing_names': cleaned_missing,
            'extra_names': cleaned_extra
        }

    def extract_missing_names_details(self, missing_names_data: List[Dict]) -> List[Dict]:
        extracted = []
        for item in missing_names_data:
            if isinstance(item, dict) and 'full_name' in item and 'position' in item:
                extracted.append({
                    'Full_Name': item['full_name'],
                    'Position': item['position'],
                    'confidence': item.get('confidence', 1.0)
                })
        return extracted
    
    def _check_name_exists_in_source(self, name: str, source_text: str) -> bool:
        """
        [Smart Check] ตรวจสอบว่าชื่อนี้มีอยู่จริงใน source text หรือไม่
        โดยไม่สนใจเว้นวรรคและคำนำหน้า (เพื่อความยืดหยุ่นแต่แม่นยำ)
        """
        if not name or not source_text:
            return False
            
        # 1. ทำความสะอาดชื่อที่ AI ส่งมา (ลบคำนำหน้า + ลบเว้นวรรค)
        clean_name = name.replace("นาย", "").replace("นางสาว", "").replace("นาง", "").replace("ดร.", "").strip()
        clean_name_nospace = clean_name.replace(" ", "")
        
        # 2. ทำความสะอาด Source Text (ลบเว้นวรรค เพื่อให้หาง่ายขึ้น)
        source_nospace = source_text.replace(" ", "")
        
        # 3. เช็คว่าชื่อ(แบบไม่มีเว้นวรรค) ปรากฏอยู่ใน Source หรือไม่
        # ต้องมีความยาวพอสมควรป้องกันการไป Match กับเศษคำ
        if len(clean_name_nospace) > 4 and clean_name_nospace in source_nospace:
            return True
            
        return False
import requests
import json
import logging
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

# กำหนด URL และ Model
OLLAMA_API_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "llama3.2" # ใช้ LLM ที่มีความสามารถในการประมวลผล JSON

class Verifier:
    """
    เครื่องมือตรวจสอบความถูกต้องของข้อมูลที่ Scrape มาเทียบกับ Live Content
    โดยใช้ Ollama LLM ในการวิเคราะห์และเปรียบเทียบ
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
        """สร้าง Prompt สำหรับ Ollama"""
        
        # ตัด HTML Content ให้เหลือขนาดที่เหมาะสม เพื่อไม่ให้ Token เกิน
        # 5000 chars น่าจะเพียงพอสำหรับหาชื่อที่ตกหล่น
        live_snippet = live_html_content[:5000]

        return f"""
        คุณคือผู้เชี่ยวชาญด้านการตรวจสอบข้อมูลองค์กร หน้าที่ของคุณคือ **ตรวจสอบความถูกต้องและครบถ้วนของรายชื่อผู้บริหารของ {bank_name}** ที่ถูกดึงมาจากหน้าเว็บ
        
        **### รายชื่อที่ถูก Scrape มา (Scraped List)**
        {scraped_records_string}
        
        **### เนื้อหาหน้าเว็บฉบับเต็ม (Live Content Snippet)**
        {live_snippet}
        
        **โปรดดำเนินการดังนี้:**
        1. ค้นหารายชื่อผู้บริหารทั้งหมด (ที่มีคำนำหน้า นาย/นาง/นางสาว/ดร./ศ./คุณ และตำแหน่งสำคัญ) ที่ปรากฏใน **Live Content Snippet**
        2. เปรียบเทียบรายการ Live กับ Scraped List (โดยไม่ต้องสนใจลำดับ)
        3. ระบุรายชื่อที่ **"ตกหล่น/ขาดหายไป"** (Missing/Unscraped Names) จาก Scraped List
        4. ระบุรายชื่อที่ **"ไม่ถูกต้อง/เกินมา"** (Incorrect/Extra Names - ชื่อที่ดึงมาแต่ไม่มีใน Live Content)
        5. ให้สรุปผลการตรวจสอบในรูปแบบ JSON เท่านั้น โดยมี Field ดังนี้:
           - is_complete: (boolean) True หาก Scraped List ครบถ้วนตาม Live List
           - missing_names: (List[string]) รายชื่อผู้บริหารที่ขาดหายไป
           - extra_names: (List[string]) รายชื่อผู้บริหารที่ดึงมาเกินมา
        """

    def verify(self, scraped_data: List[Dict], live_html_content: str, bank_name: str) -> Dict:
        """ส่งข้อมูลไปให้ Ollama ตรวจสอบและรับผลลัพธ์ JSON"""
        
        scraped_records_string = self._format_scraped_data(scraped_data)
        user_prompt = self._create_prompt(scraped_records_string, live_html_content, bank_name)

        payload = {
            "model": self.model,
            "prompt": user_prompt,
            "system": "You are a data validation expert. Your task is to compare two lists and provide missing/extra items in JSON format.",
            "stream": False,
            "format": "json" 
        }

        logger.info("⏳ Sending data to Ollama for verification...")
        
        try:
            response = requests.post(self.ollama_url, json=payload, timeout=120)
            response.raise_for_status()
            
            result_text = response.json().get('response', '{}')
            
            # แปลง JSON String เป็น Python Dict
            result = json.loads(result_text)
            logger.info("✅ Ollama verification completed.")
            return result

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Ollama API Error: Could not connect or request timed out. Make sure Ollama is running on port 11434. Error: {e}")
            return {"error": str(e), "is_complete": False, "missing_names": [], "extra_names": []}
        except json.JSONDecodeError as e:
            logger.error(f"❌ Ollama returned invalid JSON. Error: {e}")
            logger.debug(f"Raw Ollama Response: {response.text}")
            return {"error": "Invalid JSON response from LLM", "is_complete": False, "missing_names": [], "extra_names": []}
        except Exception as e:
            logger.error(f"❌ An unexpected error occurred during verification: {e}")
            return {"error": str(e), "is_complete": False, "missing_names": [], "extra_names": []}
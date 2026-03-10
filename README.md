# Nitter Fetch

สคริปต์ Python สำหรับดึงข้อมูลสถิติผู้ใช้งานและโพสต์ล่าสุด (ผ่าน Nitter RSS) จาก Twitter/X แล้วบันทึกลง Google Sheets แบบอัตโนมัติ

## ไฟล์สคริปต์หลัก

1. **`index_profile.py`**: ดึงข้อมูลจำนวนสมาชิกใน Community และสถิติโปรไฟล์ผู้ใช้ (ยอดผู้ติดตาม, ยอดโพสต์) ผ่าน X API แล้วบันทึกผลลงในชีต `Twitter(X) User Stat`
2. **`index_post.py`**: ดึงโพสต์ล่าสุดจาก Nitter RSS (ค่าเริ่มต้นคือย้อนหลัง 30 วัน) จัดการ Error 429 (Rate Limit) และส่งลิงก์ที่อ่านง่ายกรณีเจอ 404 (Not Found) ข้อมูลจะถูกบันทึกลงในชีต `Migration`
3. **`common.py`**: ไฟล์ศูนย์รวมฟังก์ชันที่ใช้ร่วมกัน เช่น การยืนยันตัวตน Google Sheets, การตั้งค่า Session X API, ตัวจัดการ Rate Limit และระบบแจ้งเตือนผ่าน Telegram

## ฟีเจอร์เด่น

- **เชื่อมต่อ Google Sheets:** อ่านรายชื่อแอคเคาท์ต้นทางและเขียนข้อมูลกลับลงหน้าชีตอัตโนมัติ
- **แจ้งเตือน Telegram:** ส่งข้อความแจ้งเตือนเข้าแชท Telegram ทันทีเมื่อเจอ Error HTTP 403 (Forbidden) หรือ 404 (Not Found) พร้อมลิงก์ไปยังบัญชี/กลุ่มที่มีปัญหาเพื่อให้กดดูได้ง่าย
- **รองรับ GitHub Actions:** ตั้งค่าสคริปต์สำหรับรันอัตโนมัติตามตารางเวลาไว้แล้วในโฟลเดอร์ `.github/workflows`
- **ระบบทนทานต่อ Rate Limit:** มีระบบ Backoff รอเวลาและ Retries อัตโนมัติเมื่อชนลิมิต API

## การติดตั้ง

1. **ติดตั้ง Library ที่จำเป็น:**

   ```bash
   pip install requests gspread google-auth
   ```

2. **ตั้งค่า Environment Variables:** สร้างไฟล์ `.env` พร้อมใส่ค่าเหล่านี้:

   ```env
   X_BEARER="your_bearer_token_here"
   X_COOKIE_STRING="your_cookie_string_here"
   TELEGRAM_BOT_TOKEN="your_bot_token"
   TELEGRAM_CHAT_ID="your_chat_id"
   ```

3. **เตรียม Google Service Account:** สร้างไฟล์ `service-account.json` ไว้ในโฟลเดอร์หลัก หรือตั้งค่าผ่านตัวแปร `SERVICE_ACCOUNT`

## วิธีใช้งาน

รันสคริปต์ผ่าน Terminal ได้เลย:

```bash
python index_profile.py
python index_post.py
```

# คู่มือการใช้งาน index.py

สคริปต์นี้ใช้สำหรับดึงข้อมูลสถิติและโพสต์ล่าสุดจาก X (Twitter) ทั้งจากบัญชีผู้ใช้ทั่วไปและ Community โดยจะนำข้อมูลไปอัปเดตลงใน Google Spreadsheet อัตโนมัติ

## คุณสมบัติหลัก

1. **ดึงสถิติพื้นฐาน (Stats):** ดึงจำนวน Follower และจำนวนโพสต์ทั้งหมดของบัญชีผู้ใช้ หรือจำนวนสมาชิกของ Community
2. **ดึงโพสต์ล่าสุด (Recent Posts):** ดึงข้อความย้อนหลังตามจำนวนวันที่กำหนด (ค่าเริ่มต้นคือ 30 วัน)
3. **ระบบ Google Sheets Integration:** อัปเดตข้อมูลลงใน Spreadsheet โดยอัตโนมัติในคอลัมน์ที่กำหนด
4. **ระบบ Backoff & Rate Limit:** มีการจัดการ Error และการรอเมื่อติด Rate Limit ของ X API

## สิ่งที่ต้องเตรียม (Prerequisites)

- **Python 3.x**
- **Libraries:** `gspread`, `google-auth`, `requests`, `zoneinfo`
- **Google Service Account:** ต้องมีไฟล์ JSON ของ Service Account และแชร์ Spreadsheet ให้ Email ของ Service Account นั้น
- **X API Credentials:** ต้องมี Bearer Token หรือ Cookie (หากต้องการดึงข้อมูลที่ละเอียดขึ้น)

## การตั้งค่า (Configuration)

ตั้งค่าผ่าน Environment Variables หรือไฟล์ `.env`:

- `X_BEARER`: Bearer Token จาก X Developer Portal
- `X_COOKIE_STRING`: (ตัวเลือก) Cookie สำหรับ User Auth
- `X_AUTH_TOKEN` / `X_CT0`: (ตัวเลือก) สำหรับการยืนยันตัวตนแบบ User

**Google Sheets ID:**
แก้ไข `SPREADSHEET_ID` ในโค้ดให้ตรงกับ ID ของ Google Sheet ที่คุณใช้งาน

## วิธีการใช้งาน

1. ติดตั้ง Dependencies:
   ```bash
   pip install gspread google-auth requests
   ```
2. รันสคริปต์:
   ```bash
   python index.py
   ```

## รายละเอียดการทำงานของโค้ด

- **`get_twitter_user_stats()`**:
  - อ่านลิงก์จากคอลัมน์ A ใน Sheet "Migration"
  - ตรวจสอบว่าเป็น User หรือ Community
  - ดึงข้อมูลแล้วเขียนลงในคอลัมน์ B, C, D
- **`get_twitter_user_recent_posts(days)`**:
  - ดึงโพสต์ย้อนหลังตามจำนวนวันที่ระบุ (ใน `__main__` ตั้งไว้ที่ 30 วัน)
  - เขียนเนื้อหาโพสต์ลงในคอลัมน์ E เป็นต้นไป

## หมายเหตุ

- สคริปต์รองรับทั้งลิงก์รูปแบบ `https://x.com/username` และ `https://x.com/i/communities/id`
- มีระบบ Retry อัตโนมัติหากเกิดข้อผิดพลาดในการเชื่อมต่อเครือข่าย

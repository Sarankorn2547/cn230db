import requests
import sqlite3
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time
import csv

# โหลด .env
load_dotenv()
API_KEY = os.getenv("MARKETSTACK_API_KEY")

if not API_KEY:
    raise ValueError("❌ ไม่พบ API KEY ใน .env")

# 🔄 โหลดหุ้นจาก CSV
def load_symbols_from_csv(filepath):
    with open(filepath, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        return [row['symbol'].strip() for row in reader if row['symbol'].strip()]

NASDAQ_100 = load_symbols_from_csv("nasdaq100.csv")

# วันที่ย้อนหลัง 30 วัน
end_date = datetime.today()
start_date = end_date - timedelta(days=30)

# ตั้งค่า SQLite
conn = sqlite3.connect("nasdaq100_prices.db")
cursor = conn.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_history (
        symbol TEXT,
        date TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER,
        PRIMARY KEY (symbol, date)
    )
""")

# ดึงข้อมูลย้อนหลัง 30 วัน
for symbol in NASDAQ_100:
    print(f"📦 ดึง: {symbol}")
    url = "http://api.marketstack.com/v1/eod"
    params = {
        "access_key": API_KEY,
        "symbols": symbol,
        "date_from": start_date.strftime('%Y-%m-%d'),
        "date_to": end_date.strftime('%Y-%m-%d'),
        "limit": 1000
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json().get("data", [])
        for day in data:
            cursor.execute("""
                INSERT OR REPLACE INTO stock_history
                (symbol, date, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                day["symbol"],
                day["date"][:10],
                day["open"],
                day["high"],
                day["low"],
                day["close"],
                day["volume"]
            ))
        print(f"✅ {symbol}: บันทึก {len(data)} รายการ")
    else:
        print(f"❌ {symbol}: {response.status_code} - {response.text}")

    time.sleep(1)  # กัน rate limit

conn.commit()
conn.close()
print("🎉 เสร็จสิ้น: เก็บข้อมูลย้อนหลัง 30 วันเรียบร้อย")

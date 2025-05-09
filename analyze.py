import sqlite3
from datetime import datetime, timedelta

# กำหนดช่วงเวลา 30 วัน
end_date = datetime.today()
start_date = end_date - timedelta(days=30)

conn = sqlite3.connect("nasdaq100_prices.db")
cursor = conn.cursor()

# 1. ผลตอบแทนสูงสุด
print("\n📈 ผลตอบแทนย้อนหลัง 30 วันสูงสุด:")
cursor.execute("""
    SELECT a.symbol,
           ((b.close - a.close) / a.close) * 100 AS pct_return
    FROM stock_history a
    JOIN stock_history b
      ON a.symbol = b.symbol
    WHERE a.date = (
              SELECT MIN(date)
              FROM stock_history
              WHERE symbol = a.symbol AND date >= ?
          )
      AND b.date = (
              SELECT MAX(date)
              FROM stock_history
              WHERE symbol = b.symbol AND date >= ?
          )
    ORDER BY pct_return DESC
    LIMIT 10
""", (start_date.strftime('%Y-%m-%d'), start_date.strftime('%Y-%m-%d')))
for sym, pct in cursor.fetchall():
    print(f"{sym: <8} +{pct:.2f}%")

# 2. ผลตอบแทนต่ำสุด
print("\n📉 ผลตอบแทนย้อนหลัง 30 วันต่ำสุด:")
cursor.execute("""
    SELECT a.symbol,
           ((b.close - a.close) / a.close) * 100 AS pct_return
    FROM stock_history a
    JOIN stock_history b
      ON a.symbol = b.symbol
    WHERE a.date = (
              SELECT MIN(date)
              FROM stock_history
              WHERE symbol = a.symbol AND date >= ?
          )
      AND b.date = (
              SELECT MAX(date)
              FROM stock_history
              WHERE symbol = b.symbol AND date >= ?
          )
    ORDER BY pct_return ASC
    LIMIT 10
""", (start_date.strftime('%Y-%m-%d'), start_date.strftime('%Y-%m-%d')))
for sym, pct in cursor.fetchall():
    print(f"{sym: <8} {pct:.2f}%")

# 3. Volume spike
print("\n🚨 หุ้นที่มี Volume Spike:")
cursor.execute("""
    WITH latest_date AS (
        SELECT symbol, MAX(date) as max_date
        FROM stock_history
        WHERE date >= ?
        GROUP BY symbol
    ),
    volume_avg5 AS (
        SELECT s.symbol,
               AVG(s.volume) AS avg_volume
        FROM stock_history s
        JOIN latest_date l ON s.symbol = l.symbol
        WHERE s.date < l.max_date
        GROUP BY s.symbol
    )
    SELECT l.symbol, s.volume, v.avg_volume
    FROM latest_date l
    JOIN stock_history s
      ON l.symbol = s.symbol AND l.max_date = s.date
    JOIN volume_avg5 v
      ON l.symbol = v.symbol
    WHERE s.volume > 2 * v.avg_volume
    ORDER BY s.volume DESC
""", (start_date.strftime('%Y-%m-%d'),))
for sym, vol, avg in cursor.fetchall():
    print(f"{sym: <8} volume = {vol:,.0f}  > avg = {avg:,.0f}")

# 4. ปิดบวกเยอะ
print("\n✅ หุ้นที่มีวันปิดบวกมากที่สุด:")
cursor.execute("""
    SELECT symbol, COUNT(*) as up_days
    FROM stock_history
    WHERE date >= ?
      AND close > open
    GROUP BY symbol
    ORDER BY up_days DESC
    LIMIT 10
""", (start_date.strftime('%Y-%m-%d'),))
for sym, count in cursor.fetchall():
    print(f"{sym: <8} up_days = {count}")

# 5. ความผันผวนเฉลี่ย
print("\n📊 หุ้นที่ผันผวนเฉลี่ยสูงสุด (high - low):")
cursor.execute("""
    SELECT symbol, AVG(high - low) AS avg_range
    FROM stock_history
    WHERE date >= ?
    GROUP BY symbol
    ORDER BY avg_range DESC
    LIMIT 10
""", (start_date.strftime('%Y-%m-%d'),))
for sym, rng in cursor.fetchall():
    print(f"{sym: <8} avg_range = {rng:.2f}")

cursor.execute("""
    SELECT symbol, AVG(volume) AS avg_volume
    FROM stock_history
    WHERE date >= ?
    GROUP BY symbol
    ORDER BY avg_volume DESC
    LIMIT 10
""", (start_date.strftime('%Y-%m-%d'),))
print("\n📦 หุ้นที่มี volume เฉลี่ยสูงสุด:")
for row in cursor.fetchall():
    print(f"{row[0]: <8} avg_volume = {row[1]:,.0f}")

cursor.execute("""
    SELECT symbol, date, close
    FROM stock_history
    WHERE (symbol, date) IN (
        SELECT symbol, MAX(date)
        FROM stock_history
        GROUP BY symbol
    )
    ORDER BY close DESC
    LIMIT 10
""")
print("\n💰 หุ้นที่ราคาปิดล่าสุดสูงสุด:")
for row in cursor.fetchall():
    print(f"{row[0]: <8} date = {row[1]}, close = {row[2]:.2f}")

cursor.execute("""
    SELECT s.symbol, s.date, s.close
    FROM stock_history s
    INNER JOIN (
        SELECT symbol, MAX(date) AS max_date
        FROM stock_history
        GROUP BY symbol
    ) latest
    ON s.symbol = latest.symbol AND s.date = latest.max_date
    ORDER BY s.close ASC
    LIMIT 10
""")
print("\n💸 หุ้นที่ราคาปิดล่าสุดต่ำสุด:")
for row in cursor.fetchall():
    print(f"{row[0]: <8} date = {row[1]}, close = {row[2]:.2f}")

conn.close()

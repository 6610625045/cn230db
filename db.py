import requests
import sqlite3
import json

# --- อ่านข้อมูลจากแหล่งข้อมูล API (จำลอง) ---
def fetch_data_from_api():
    """จำลองการดึงข้อมูลจาก API ของ Alpha Vantage."""
    # ข้อมูลสมมติในรูปแบบ JSON
    mock_data = {
        "Time Series (Daily)": {
            "2025-05-09": {"open": 155.50, "high": 157.00, "low": 154.80, "close": 156.50, "volume": 120000},
            "2025-05-08": {"open": 152.00, "high": 153.50, "low": 151.70, "close": 153.00, "volume": 95000},
            "2025-05-07": {"open": 150.20, "high": 152.50, "low": 149.80, "close": 152.00, "volume": 100000},
            "2025-05-06": {"open": 149.50, "high": 150.80, "low": 148.90, "close": 150.10, "volume": 85000},
            "2025-05-05": {"open": 148.00, "high": 149.90, "low": 147.50, "close": 149.20, "volume": 92000},
        },
        "Meta Data": {
            "Symbol": "XYZ",
            "Last Refreshed": "2025-05-09"
        }
    }
    return mock_data

# ---  นำข้อมูลเก็บใน DBMS โดยใช้ sqlite3 ---
def store_data_to_sqlite(data, db_name="stock_data.db"):
    """เก็บข้อมูลจาก API ลงในฐานข้อมูล SQLite."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

   
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_stock_data (
            date TEXT PRIMARY KEY,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER
        )
    """)

    time_series_data = data.get("Time Series (Daily)", {})
    for date, values in time_series_data.items():
        cursor.execute("""
            INSERT OR REPLACE INTO daily_stock_data (date, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (date, values.get("open"), values.get("high"), values.get("low"), values.get("close"), values.get("volume")))

    conn.commit()
    conn.close()
    print(f"ข้อมูลถูกจัดเก็บลงในฐานข้อมูล '{db_name}' แล้ว.")

# ---  ใช้ SQL ทำ data analytics จากข้อมูล ---
def analyze_data_with_sql(db_name="stock_data.db"):
    """วิเคราะห์ข้อมูลด้วย SQL และแสดงผลลัพธ์."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    print("\n--- Data Analytics ---")

    # 1. แสดงค่าเฉลี่ยของราคาเปิด, ราคาสูงสุด, ราคาต่ำสุด และราคาปิดของหุ้น
    cursor.execute("""
        SELECT AVG(open) AS avg_open,
               AVG(high) AS avg_high,
               AVG(low) AS avg_low,
               AVG(close) AS avg_close
        FROM daily_stock_data
    """)
    average_prices = cursor.fetchone()
    print(f"1.ค่าเฉลี่ยของราคาหุ้น:")
    print(f"  ราคาเปิด: {average_prices[0]:.2f}")
    print(f"  ราคาสูงสุด: {average_prices[1]:.2f}")
    print(f"  ราคาต่ำสุด: {average_prices[2]:.2f}")
    print(f"  ราคาปิด: {average_prices[3]:.2f}")

    # 2. แสดงวันที่ที่มีปริมาณการซื้อขายของหุ้นสูงสุด 3 อันดับแรก
    cursor.execute("""
        SELECT date, volume
        FROM daily_stock_data
        ORDER BY volume DESC
        LIMIT 3
    """)
    top_volume_dates = cursor.fetchall()
    print("\n2.วันที่ที่มีปริมาณการซื้อขายหุ้นสูงสุด 3 อันดับแรก:")
    for date, volume in top_volume_dates:
        print(f"  วันที่: {date}, ปริมาณ: {volume}")

    # 3. แสดงวันที่ที่มีช่วงราคาสูงสุดของหุ้น (high - low) 3 อันดับแรก
    cursor.execute("""
        SELECT date, (high - low) AS price_range
        FROM daily_stock_data
        ORDER BY price_range DESC
        LIMIT 3
    """)
    top_price_range_dates = cursor.fetchall()
    print("\n3.วันที่ที่มีช่วงราคาหุ้นสูงสุด 3 อันดับแรก:")
    for date, price_range in top_price_range_dates:
        print(f"  วันที่: {date}, ช่วงราคา: {price_range:.2f}")

    conn.close()

def analyze_data_with_sql_extended(db_name="stock_data.db"):
    """วิเคราะห์ข้อมูลเพิ่มเติมด้วย SQL และแสดงผลลัพธ์."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()


    # 4. แสดงวันที่ที่มีราคาปิดของหุ้นสูงสุด
    cursor.execute("""
        SELECT date, close
        FROM daily_stock_data
        ORDER BY close DESC
        LIMIT 1
    """)
    highest_close = cursor.fetchone()
    print(f"\n4.วันที่ที่มีราคาหุ้นปิดสูงสุด: {highest_close[0]}, ราคา: {highest_close[1]:.2f}")

    # 5. แสดงวันที่ที่มีราคาปิดของหุ้นต่ำสุด
    cursor.execute("""
        SELECT date, close
        FROM daily_stock_data
        ORDER BY close ASC
        LIMIT 1
    """)
    lowest_close = cursor.fetchone()
    print(f"5.วันที่ที่มีราคาหุ้นปิดต่ำสุด: {lowest_close[0]}, ราคา: {lowest_close[1]:.2f}")

    # 6. แสดงค่าเฉลี่ยของปริมาณการซื้อขายของหุ้น
    cursor.execute("""
        SELECT AVG(volume) AS avg_volume
        FROM daily_stock_data
    """)
    average_volume = cursor.fetchone()[0]
    print(f"\n6.ค่าเฉลี่ยของปริมาณการซื้อขายหุ้น: {average_volume:.0f}")


    # 7. แสดงจำนวนวันที่ที่มีราคาปิดของหุ้นสูงกว่าราคาเปิด
    cursor.execute("""
        SELECT COUNT(*)
        FROM daily_stock_data
        WHERE close > open
    """)
    days_close_higher_than_open = cursor.fetchone()[0]
    print(f"\n7.จำนวนวันที่ราคาหุ้นปิดสูงกว่าราคาเปิด: {days_close_higher_than_open} วัน")

    # 8. แสดงจำนวนวันที่ที่มีราคาปิดของหุ้นต่ำกว่าราคาเปิด
    cursor.execute("""
        SELECT COUNT(*)
        FROM daily_stock_data
        WHERE close < open
    """)
    days_close_lower_than_open = cursor.fetchone()[0]
    print(f"8.จำนวนวันที่ราคาหุ้นปิดต่ำกว่าราคาเปิด: {days_close_lower_than_open} วัน")

    # 9. แสดงวันที่ที่มีอัตราส่วนของราคาสูงสุดต่อราคาต่ำสุดสูงสุดของหุ้น (แสดงถึงความผันผวนระหว่างวันมาก)
    cursor.execute("""
        SELECT date, (high / low) AS volatility_ratio
        FROM daily_stock_data
        ORDER BY volatility_ratio DESC
        LIMIT 1
    """)
    most_volatile_day = cursor.fetchone()
    print(f"\n9.วันที่ที่มีความผันผวนของราคาหุ้นสูงสุด (high/low): {most_volatile_day[0]}, อัตราส่วน: {most_volatile_day[1]:.2f}")

    conn.close()

if __name__ == "__main__":
    api_data = fetch_data_from_api()
    store_data_to_sqlite(api_data)
    analyze_data_with_sql()
    analyze_data_with_sql_extended()

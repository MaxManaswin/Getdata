from flask import Flask, request, jsonify
from pymongo import MongoClient
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import time

app = Flask(__name__)

# --- 1. Configuration (แก้ไขตามค่าจริงของคุณ) ---
MONGO_URI = "mongodb://localhost:27017"
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "w1iwVOXxAtimRLKKyoOXz_jIK3ru8arflF-wmgCiebkdmwkD5jlxbOv8uwLAQ69qCGO8KdKFdZ9mZCxshbolPA=="  # วาง Token ที่ Copy มา
INFLUX_ORG = "Ioterra*"
INFLUX_BUCKET = "Ioterra*"

# --- 2. Database Connections ---
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["iot_system"]
devices_col = db["devices"]

influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

# ตัวแปรสำหรับ Buffer ข้อมูล MongoDB (5 นาที)
latest_data = {} 
LAST_SAVE_TIME = time.time()

# --- 3. Route: Authentication (สำหรับ EMQX Access Control) ---
@app.route('/auth', methods=['POST'])
def mqtt_auth():
    auth_data = request.json
    clientid = auth_data.get("clientid")
    username = auth_data.get("username")
    password = auth_data.get("password")

    # ค้นหาใน MongoDB (เช็ค 3 อย่าง)
    device = devices_col.find_one({
        "id": clientid,
        "mqtt_username": username,
        "mqtt_password": password
    })

    print(username, clientid, password)

    if device:
        print(f"Auth Success: {clientid}")
        return jsonify({"result": "allow"}), 200
    else:
        print(f"Auth Failed: {clientid}")
        return jsonify({"result": "deny"}), 200

# --- 4. Route: Data Integration (รับข้อมูลจาก EMQX Rule Engine) ---
@app.route('/data', methods=['POST'])
def receive_data():
    global LAST_SAVE_TIME, latest_data
    payload = request.json 
    
    dev_id = payload.get("dev_id")
    
    # ดึงค่าจาก Payload (อ้างอิงตาม Register Address ในคู่มือ) 
    # หมายเหตุ: ค่าบางตัวเช่น Temp/Hum/Noise/Press ต้องหาร 10 ตามสเปค 
    data = {
        "device_id": dev_id,
        "temp": payload.get("temperature"),   # หน่วย 0.1℃ 
        "hum": payload.get("humidity"),      # หน่วย 0.1%RH 
        "noise": payload.get("noise"),        # หน่วย 0.1dB 
        "pm25": payload.get("pm25"),          # หน่วย 1μg/m³ 
        "pm10": payload.get("pm10"),          # หน่วย 1μg/m³ 
        "press": payload.get("pressure"),     # หน่วย 0.1kPa 
        "lux": payload.get("lux"),            # หน่วย 1Lux 
        "timestamp": time.time()
    }

    # A. ส่งลง InfluxDB (Real-time)
    point = Point("weather_station").tag("device_id", dev_id)
    for key, value in data.items():
        if key not in ["device_id", "timestamp"] and value is not None:
            point.field(key, float(value))
    write_api.write(bucket=INFLUX_BUCKET, record=point)

    # B. เก็บค่าล่าสุดไว้ใน Dictionary เพื่อรอทำ Snapshot ลง MongoDB
    latest_data[dev_id] = data

    # เช็คว่าครบ 5 นาที (300 วินาที) หรือยัง
    if time.time() - LAST_SAVE_TIME >= 300:
        if latest_data:
            data_to_save = list(latest_data.values())
            db["device_history"].insert_many(data_to_save)
            
            print(f"Saved snapshot of {len(data_to_save)} devices to MongoDB")
            
            latest_data = {} # ล้างค่าเพื่อรอบถัดไป
            LAST_SAVE_TIME = time.time()
            
    return {"status": "success"}, 200
if __name__ == '__main__':
    # รันที่พอร์ต 5000 (อย่าลืมเปิดพอร์ตนี้ใน Firewall ด้วย)
    app.run(host='0.0.0.0', port=5001)
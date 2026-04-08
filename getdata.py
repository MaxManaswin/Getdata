from flask import Flask, request, jsonify
from pymongo import MongoClient
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import time
import datetime

app = Flask(__name__)

# --- 1. Configuration ---
MONGO_URI = "mongodb://localhost:27017"
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "w1iwVOXxAtimRLKKyoOXz_jIK3ru8arflF-wmgCiebkdmwkD5jlxbOv8uwLAQ69qCGO8KdKFdZ9mZCxshbolPA==" 
INFLUX_ORG = "Ioterra*"
INFLUX_BUCKET = "Ioterra*"

# --- ตั้งค่าเกณฑ์ความปลอดภัย (Thresholds) ---
# ปรับตัวเลขตามความเหมาะสมของหน้างานจริง
SENSOR_LIMITS = {
    "temperature": {"min": -20, "max": 90}, # หน่วย 0.1C (คือ -20.0 ถึง 90.0)
    "humidity": {"min": 0, "max": 1000},     # หน่วย 0.1%
    "pm25": {"min": 0, "max": 1000},
    "lux": {"min": 0, "max": 200000}
}

# --- 2. Database Connections ---
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["iot_system"]
devices_col = db["devices"]
incident_col = db["incident_logs"] # เพิ่มตารางเก็บ Log ความผิดปกติ

influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

latest_data = {} 
LAST_SAVE_TIME = time.time()

# --- 3. Route: Authentication ---
@app.route('/auth', methods=['POST'])
def mqtt_auth():
    auth_data = request.json
    clientid = auth_data.get("clientid")
    username = auth_data.get("username")
    password = auth_data.get("password")

    device = devices_col.find_one({
        "id": clientid,
        "mqtt_username": username,
        "mqtt_password": password
    })

    if device:
        return jsonify({"result": "allow"}), 200
    else:
        return jsonify({"result": "deny"}), 200

# --- 4. Route: Data Integration (พร้อมระบบตรวจสอบ) ---
@app.route('/data', methods=['POST'])
def receive_data():
    global LAST_SAVE_TIME, latest_data
    payload = request.json 
    dev_id = payload.get("dev_id")
    
    # 1. กำหนดค่าที่ต้องการดึงมาจาก Payload
    raw_data = {
        "temp": payload.get("temperature"),
        "hum": payload.get("humidity"),
        "noise": payload.get("noise"),
        "pm25": payload.get("pm25"),
        "pm10": payload.get("pm10"),
        "press": payload.get("pressure"),
        "lux": payload.get("lux")
    }

    valid_fields = {} # เก็บค่าที่ปกติเพื่อส่ง Influx
    error_fields = {} # เก็บค่าที่ผิดปกติเพื่อลง Log

    # 2. Loop ตรวจสอบทีละ Field
    for key, value in raw_data.items():
        if value is None:
            continue
            
        # ตรวจสอบตามเงื่อนไข SENSOR_LIMITS (ถ้ามีระบุไว้)
        # mapping ชื่อ key ใน raw_data ให้ตรงกับ SENSOR_LIMITS
        limit_key = {
            "temp": "temperature",
            "hum": "humidity",
            "pm25": "pm25",
            "lux": "lux"
        }.get(key)

        if limit_key and limit_key in SENSOR_LIMITS:
            limits = SENSOR_LIMITS[limit_key]
            if value < limits["min"] or value > limits["max"]:
                # 🚨 ค่าผิดปกติ: เก็บลง error_fields
                error_fields[key] = value
                continue # ข้ามไปตัวถัดไป ไม่เอาลง valid_fields
        
        # ✅ ค่าปกติ หรือ ค่าที่ไม่ได้ตั้ง Limit ไว้: เก็บลง valid_fields
        valid_fields[key] = value

    # 3. บันทึกค่าที่ผิดปกติลง MongoDB (ถ้ามี)
    if error_fields:
        print(f"⚠️ [ALERT] {dev_id} abnormal data: {error_fields}")
        incident_col.insert_one({
            "dev_id": dev_id,
            "timestamp": datetime.datetime.now(),
            "invalid_data": error_fields,
            "raw_payload": payload
        })

    # 4. บันทึกค่าที่ "ปกติเท่านั้น" ลง InfluxDB
    if valid_fields:
        point = Point("weather_station").tag("device_id", dev_id)
        for key, value in valid_fields.items():
            point.field(key, float(value))
        write_api.write(bucket=INFLUX_BUCKET, record=point)

        # 5. เก็บลง MongoDB Snapshot (ทุก 5 นาที) เฉพาะค่าที่ปกติ
        latest_data[dev_id] = {
            "device_id": dev_id,
            **valid_fields, # กระจายค่าที่ปกติลงไป
            "timestamp": time.time()
        }

    # เช็คเงื่อนไขบันทึก Snapshot ลง MongoDB ตามเดิม
    if time.time() - LAST_SAVE_TIME >= 300:
        if latest_data:
            db["device_history"].insert_many(list(latest_data.values()))
            latest_data = {}
            LAST_SAVE_TIME = time.time()
            
    return {"status": "success"}, 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
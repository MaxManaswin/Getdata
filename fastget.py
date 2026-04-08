from fastapi import FastAPI, Request, BackgroundTasks
from pymongo import MongoClient
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import time
import datetime

app = FastAPI()

# --- 1. Configuration ---
MONGO_URI = "mongodb://localhost:27017"
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "w1iwVOXxAtimRLKKyoOXz_jIK3ru8arflF-wmgCiebkdmwkD5jlxbOv8uwLAQ69qCGO8KdKFdZ9mZCxshbolPA==" 
INFLUX_ORG = "Ioterra*"
INFLUX_BUCKET = "Ioterra*"

SENSOR_LIMITS = {
    "temperature": {"min": -20, "max": 90},
    "humidity": {"min": 0, "max": 1000},
    "pm25": {"min": 0, "max": 1000},
    "lux": {"min": 0, "max": 200000}
}

# --- 2. Database Connections ---
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["iot_system"]
devices_col = db["devices"]
incident_col = db["incident_logs"]

influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

latest_data = {} 
LAST_SAVE_TIME = time.time()

# --- 3. Endpoint: Authentication ---
@app.post("/auth")
async def mqtt_auth(request: Request):
    auth_data = await request.json()
    clientid = auth_data.get("clientid")
    username = auth_data.get("username")
    password = auth_data.get("password")

    device = devices_col.find_one({
        "id": clientid,
        "mqtt_username": username,
        "mqtt_password": password
    })

    if device:
        return {"result": "allow"}
    return {"result": "deny"}

# --- 4. Endpoint: Data Integration ---
@app.post("/data")
async def receive_data(request: Request):
    global LAST_SAVE_TIME, latest_data
    payload = await request.json()
    dev_id = payload.get("dev_id", "STATION_UNKNOWN")
    
    raw_data = {
        "temp": payload.get("temperature"),
        "hum": payload.get("humidity"),
        "noise": payload.get("noise"),
        "pm25": payload.get("pm25"),
        "pm10": payload.get("pm10"),
        "press": payload.get("pressure"),
        "lux": payload.get("lux")
    }

    valid_fields = {}
    error_details = []

    for key, value in raw_data.items():
        if value is None: continue
            
        # จับคู่ชื่อ Key กับ SENSOR_LIMITS
        limit_key = {
            "temp": "temperature", "hum": "humidity", 
            "pm25": "pm25", "lux": "lux"
        }.get(key)

        if limit_key and limit_key in SENSOR_LIMITS:
            limits = SENSOR_LIMITS[limit_key]
            # ตรวจสอบค่าว่าอยู่นอกช่วงหรือไม่
            if value < limits["min"] or value > limits["max"]:
                error_details.append({
                    "sensor": key,
                    "actual_value": value,
                    "threshold": limits,
                    "status": "TOO HIGH" if value > limits["max"] else "TOO LOW"
                })
                continue # ข้ามค่านี้ไป ไม่ส่งลง Influx
        
        valid_fields[key] = value

    # --- บันทึกลง MongoDB: กรณีมีค่าผิดปกติ ---
    if error_details:
        print(f"⚠️ [{dev_id}] Found {len(error_details)} invalid fields!")
        incident_col.insert_one({
            "dev_id": dev_id,
            "timestamp": datetime.datetime.now(),
            "invalid_fields": error_details,  # บอกว่าตัวไหนพัง พังเพราะอะไร
            "raw_payload": payload
        })

    # --- บันทึกลง InfluxDB: เฉพาะค่าที่ปกติ ---
    if valid_fields:
        try:
            point = Point("weather_station").tag("device_id", dev_id)
            for key, value in valid_fields.items():
                point.field(key, float(value))
            write_api.write(bucket=INFLUX_BUCKET, record=point)

            # เก็บไว้ทำ Snapshot 5 นาที
            latest_data[dev_id] = {
                "device_id": dev_id,
                **valid_fields,
                "timestamp": datetime.datetime.now()
            }
        except Exception as e:
            print(f"❌ InfluxDB Write Error: {e}")

    # --- บันทึก Snapshot ลง MongoDB ทุกๆ 5 นาที ---
    if time.time() - LAST_SAVE_TIME >= 300:
        if latest_data:
            history_col.insert_many(list(latest_data.values()))
            print("💾 5-Minute Snapshot Saved to MongoDB.")
            latest_data.clear()
            LAST_SAVE_TIME = time.time()
            
    return {"status": "success", "processed_at": str(datetime.datetime.now())}

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=3000)
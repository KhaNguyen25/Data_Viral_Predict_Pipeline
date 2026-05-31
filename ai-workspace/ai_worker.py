import json
import os
import xgboost as xgb
import redis
import pandas as pd
import numpy as np
import time  
from kafka import KafkaConsumer

# ==========================================
# 1. CẤU HÌNH HỆ THỐNG VÀ ĐƯỜNG DẪN
# ==========================================
KAFKA_BROKER = 'kafka:29092'
KAFKA_TOPIC = 'cdn_features_stream'
REDIS_HOST = 'redis'
REDIS_PORT = 6379

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "../"))
MODEL_PATH = os.path.join(ROOT_DIR, "models", "xgboost_cache_model.json")

# Đồng bộ Threshold xác suất với file retrain
CUSTOM_THRESHOLD = 0.95

FEATURE_COLS = [
    "hour_of_day", "hour_sin", "hour_cos", 
    "active_edges_count", "req_count_current", 
    "req_count_previous", "growth_rate"
]

def start_ai_worker():
    print(f"🚀 Đang khởi động AI Worker (Ngưỡng kích hoạt Viral: {CUSTOM_THRESHOLD})...")

    # ==========================================
    # 2. KHỞI TẠO
    # ==========================================
    if not os.path.exists(MODEL_PATH):
        print(f"❌ Lỗi: Không tìm thấy model tại {MODEL_PATH}.")
        return

    model = xgb.XGBClassifier()
    model.load_model(MODEL_PATH)
    print("✅ Đã nạp thành công XGBoost Model vào RAM.")
    
    last_model_mtime = os.path.getmtime(MODEL_PATH)

    try:
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        redis_client.ping()
        print("✅ Kết nối Redis thành công.")
    except Exception as e:
        print(f"❌ Lỗi kết nối Redis: {e}")
        return

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        group_id='ai_cache_predictor_group',
        auto_offset_reset='latest', 
        enable_auto_commit=False, 
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    print(f"🎧 AI Worker đang lắng nghe luồng dữ liệu từ topic: '{KAFKA_TOPIC}'...\n")

    # ==========================================
    # 3. VÒNG LẶP SỰ KIỆN (EVENT LOOP)
    # ==========================================
    try:
        while True:
            try:
                # KIỂM TRA HOT-RELOAD
                try:
                    current_mtime = os.path.getmtime(MODEL_PATH)
                    if current_mtime > last_model_mtime:
                        print("\n🔄 [HOT-RELOAD] Phát hiện Model XGBoost mới!")
                        try:
                            model.load_model(MODEL_PATH)
                            last_model_mtime = current_mtime
                            print("✅ Đã cập nhật Model mới thành công.\n")
                        except Exception as load_err:
                            print(f"⚠️ Chưa thể nạp model mới (đang bị lock ghi): {load_err}")
                except OSError:
                    pass

                # ĐỌC MESSAGE TỪ KAFKA
                msg_pack = consumer.poll(timeout_ms=1000, max_records=500)
                if not msg_pack:
                    continue
                
                batch_data = []
                batch_obj_ids = []
                
                for tp, messages in msg_pack.items():
                    for message in messages:
                        data = message.value
                        obj_id = data.pop('curr_object_id', None) 
                        if obj_id is not None:
                            batch_data.append(data)
                            batch_obj_ids.append(obj_id)

                # DỰ ĐOÁN VÀ ĐẨY VÀO REDIS
                if batch_data:
                    df_features = pd.DataFrame(batch_data, columns=FEATURE_COLS)
                    
                    # [ĐÃ SỬA]: Dự đoán theo xác suất và đối chiếu với CUSTOM_THRESHOLD
                    probabilities = model.predict_proba(df_features)[:, 1]
                    predictions = (probabilities >= CUSTOM_THRESHOLD).astype(int)

                    pipeline = redis_client.pipeline()
                    viral_count = 0
                    
                    for obj_id, pred in zip(batch_obj_ids, predictions):
                        if pred == 1:
                            pipeline.set(f"viral:{obj_id}", "TRUE", ex=900)
                            viral_count += 1
                    
                    if viral_count > 0:
                        pipeline.execute()
                        print(f"🔥 [BÁO ĐỘNG] Lô {len(batch_data)} logs -> Bắt được {viral_count} file Viral (>= {CUSTOM_THRESHOLD*100}%). Đã ghi Redis.")
                
                consumer.commit()

            except Exception as batch_error:
                print(f"⚠️ Lỗi xử lý batch: {batch_error}")
                time.sleep(2) 
                
    except KeyboardInterrupt:
        print("\n🛑 Nhận lệnh dừng. Đang đóng kết nối an toàn...")
    finally:
        consumer.close()
        redis_client.close()
        print("👋 AI Worker đã tắt.")

if __name__ == "__main__":
    start_ai_worker()
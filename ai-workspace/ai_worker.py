import json
import os
import xgboost as xgb
import redis
import pandas as pd
from kafka import KafkaConsumer

# ==========================================
# 1. CẤU HÌNH HỆ THỐNG VÀ ĐƯỜNG DẪN
# ==========================================
KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPIC = 'cdn_features_stream'
REDIS_HOST = 'redis'
REDIS_PORT = 6379

# [SỬA LẠI KHÚC NÀY] Lùi 1 cấp giống hệt ai_retrain.py
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "../"))
MODEL_PATH = os.path.join(ROOT_DIR, "models", "xgboost_cache_model.json")

# Đảm bảo thứ tự cột phải KHỚP TUYỆT ĐỐI với lúc bạn train AI
FEATURE_COLS = [
    "hour_of_day", "hour_sin", "hour_cos", 
    "active_edges_count", "req_count_current", 
    "req_count_previous", "growth_rate"
]

def start_ai_worker():
    print("🚀 Đang khởi động AI Worker...")

    # ==========================================
    # 2. KHỞI TẠO (CHỈ CHẠY 1 LẦN DUY NHẤT)
    # ==========================================
    
    # 2.1. Nạp Model vào RAM
    if not os.path.exists(MODEL_PATH):
        print(f"❌ Lỗi: Không tìm thấy model tại {MODEL_PATH}. Vui lòng chạy Batch Pipeline để train model trước!")
        return

    model = xgb.XGBClassifier()
    model.load_model(MODEL_PATH)
    print("✅ Đã nạp thành công XGBoost Model vào RAM.")

    # 2.2. Kết nối Redis
    try:
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        redis_client.ping()
        print("✅ Kết nối Redis thành công.")
    except Exception as e:
        print(f"❌ Lỗi kết nối Redis: {e}")
        return

    # 2.3. Kết nối Kafka Consumer
    # value_deserializer giúp tự động dịch chuỗi Byte của Kafka thành Dictionary (JSON) của Python
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='latest', # Bỏ qua data cũ lúc tắt máy, chỉ lấy luồng live
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    print(f"🎧 AI Worker đang lắng nghe trực tiếp luồng dữ liệu từ topic: '{KAFKA_TOPIC}'...\n")

    # ==========================================
    # 3. VÒNG LẶP SỰ KIỆN (EVENT LOOP)
    # ==========================================
    try:
        # Vòng lặp này tự động "ngủ" khi không có tin nhắn, không làm hao CPU
        for message in consumer:
            data = message.value
            
            # 3.1. Tách ID ra khỏi cục Data (Vì ID không được đưa vào dự đoán)
            # Dùng .pop() lấy value ra và xóa key đó khỏi dictionary
            obj_id = data.pop('curr_object_id', None) 
            
            if not obj_id:
                continue

            # 3.2. Ép cục Data còn lại thành Pandas DataFrame với 1 dòng duy nhất
            # Chú ý: Ta truyền cột FEATURE_COLS để ép Pandas xếp đúng thứ tự cột cho XGBoost
            df_features = pd.DataFrame([data], columns=FEATURE_COLS)
            
            # 3.3. AI Dự đoán (Siêu tốc)
            prediction = model.predict(df_features)[0]

            # 3.4. Bắn Cache lên Redis nếu có tín hiệu Viral
            if prediction == 1:
                # set key với TTL = 3600 giây (1 tiếng)
                redis_client.set(f"viral:{obj_id}", "true", ex=3600)
                print(f"🔥 [BÁO ĐỘNG] File {obj_id} sắp Viral! Đã đẩy lệnh Cache (TTL: 1h).")
                
    except KeyboardInterrupt:
        print("\n🛑 Nhận lệnh dừng. Đang đóng kết nối an toàn...")
    finally:
        consumer.close()
        redis_client.close()
        print("👋 AI Worker đã tắt.")

if __name__ == "__main__":
    start_ai_worker()
import json
import os
import xgboost as xgb
import redis
import pandas as pd
import time  # Thêm thư viện time để xử lý delay khi gặp lỗi
from kafka import KafkaConsumer

# ==========================================
# 1. CẤU HÌNH HỆ THỐNG VÀ ĐƯỜNG DẪN
# ==========================================
KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPIC = 'cdn_features_stream'
REDIS_HOST = 'redis'
REDIS_PORT = 6379

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
    print("🚀 Đang khởi động AI Worker (Chế độ tối ưu Micro-batching)...")

    # ==========================================
    # 2. KHỞI TẠO (CHỈ CHẠY 1 LẦN DUY NHẤT)
    # ==========================================
    
    if not os.path.exists(MODEL_PATH):
        print(f"❌ Lỗi: Không tìm thấy model tại {MODEL_PATH}. Vui lòng chạy Batch Pipeline để train model trước!")
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

    # [SỬA LỖI 2]: Tắt auto-commit để chuyển sang chế độ commit thủ công (At-least-once)
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        group_id='ai_cache_predictor_group',
        auto_offset_reset='latest', 
        enable_auto_commit=False, 
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    print(f"🎧 AI Worker đang lắng nghe trực tiếp luồng dữ liệu từ topic: '{KAFKA_TOPIC}'...\n")

    # ==========================================
    # 3. VÒNG LẶP SỰ KIỆN (EVENT LOOP) - XỬ LÝ HÀNG LOẠT
    # ==========================================
    try:
        while True:
            try:
                # ---------------------------------------------------------
                # BƯỚC 3.1: KIỂM TRA VÀ CẬP NHẬT MODEL TỰ ĐỘNG (HOT-RELOAD)
                # ---------------------------------------------------------
                try:
                    current_mtime = os.path.getmtime(MODEL_PATH)
                    if current_mtime > last_model_mtime:
                        print("\n🔄 [HOT-RELOAD] Phát hiện Model XGBoost mới từ quá trình Retrain!")
                        print("⏳ Đang nạp lại Model vào RAM...")
                        
                        # [SỬA LỖI 1]: Bọc Exception tại bước load_model để tránh crash khi file model đang ghi dở
                        try:
                            model.load_model(MODEL_PATH)
                            last_model_mtime = current_mtime
                            print("✅ Đã cập nhật Model mới thành công. Tiếp tục dự đoán...\n")
                        except Exception as load_err:
                            print(f"⚠️ Chưa thể nạp model mới (có thể file đang bị ghi): {load_err}")
                            
                except OSError:
                    pass
                # ---------------------------------------------------------

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

                if batch_data:
                    df_features = pd.DataFrame(batch_data, columns=FEATURE_COLS)
                    predictions = model.predict(df_features)

                    pipeline = redis_client.pipeline()
                    viral_count = 0
                    
                    for obj_id, pred in zip(batch_obj_ids, predictions):
                        if pred == 1:
                            pipeline.set(f"viral:{obj_id}", "TRUE", ex=900)
                            viral_count += 1
                    
                    if viral_count > 0:
                        pipeline.execute()
                        print(f"🔥 [BÁO ĐỘNG] Đã nhận lô {len(batch_data)} logs -> Phát hiện {viral_count} file sắp Viral! Đã ghi Cache.")
                
                # [SỬA LỖI 2]: Commit offset thủ công sau khi đảm bảo batch đã được xử lý và ghi Redis an toàn
                consumer.commit()

            except Exception as batch_error:
                print(f"⚠️ [CẢNH BÁO CỤC BỘ] Lỗi trong lúc xử lý batch hiện tại: {batch_error}")
                print("➡️ Do chưa commit offset, hệ thống sẽ tự động thử lại toàn bộ batch này ở vòng lặp sau...")
                time.sleep(2) 
                
    except KeyboardInterrupt:
        print("\n🛑 Nhận lệnh dừng. Đang đóng kết nối an toàn...")
    finally:
        consumer.close()
        redis_client.close()
        print("👋 AI Worker đã tắt.")

if __name__ == "__main__":
    start_ai_worker()
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
    
    # 2.1. Nạp Model vào RAM lần đầu
    if not os.path.exists(MODEL_PATH):
        print(f"❌ Lỗi: Không tìm thấy model tại {MODEL_PATH}. Vui lòng chạy Batch Pipeline để train model trước!")
        return

    model = xgb.XGBClassifier()
    model.load_model(MODEL_PATH)
    print("✅ Đã nạp thành công XGBoost Model vào RAM.")
    
    # Lưu lại thời điểm file model được tạo/sửa lần cuối
    last_model_mtime = os.path.getmtime(MODEL_PATH)

    # 2.2. Kết nối Redis
    try:
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        redis_client.ping()
        print("✅ Kết nối Redis thành công.")
    except Exception as e:
        print(f"❌ Lỗi kết nối Redis: {e}")
        return

    # 2.3. Kết nối Kafka Consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='latest', 
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    print(f"🎧 AI Worker đang lắng nghe trực tiếp luồng dữ liệu từ topic: '{KAFKA_TOPIC}'...\n")

    # ==========================================
    # 3. VÒNG LẶP SỰ KIỆN (EVENT LOOP) - XỬ LÝ HÀNG LOẠT
    # ==========================================
    try:
        while True:
            # ---------------------------------------------------------
            # BƯỚC MỚI: KIỂM TRA VÀ CẬP NHẬT MODEL TỰ ĐỘNG (HOT-RELOAD)
            # ---------------------------------------------------------
            try:
                current_mtime = os.path.getmtime(MODEL_PATH)
                if current_mtime > last_model_mtime:
                    print("\n🔄 [HOT-RELOAD] Phát hiện Model XGBoost mới từ quá trình Retrain!")
                    print("⏳ Đang nạp lại Model vào RAM...")
                    model.load_model(MODEL_PATH)
                    last_model_mtime = current_mtime
                    print("✅ Đã cập nhật Model mới thành công. Tiếp tục dự đoán...\n")
            except OSError:
                # Bỏ qua lỗi trong khoảnh khắc file model đang bị ghi đè (xóa và tạo mới)
                pass
            # ---------------------------------------------------------

            # Lấy tối đa 500 tin nhắn một lúc. Chờ tối đa 1 giây (1000ms).
            # Nếu Spark chưa xả batch hoặc ít traffic, Worker sẽ tự động ngủ trong 1s này
            msg_pack = consumer.poll(timeout_ms=1000, max_records=500)
            
            if not msg_pack:
                continue
            
            batch_data = []
            batch_obj_ids = []
            
            # Duyệt qua các phân vùng (partitions) và tin nhắn (messages) trong lô vừa bốc được
            for tp, messages in msg_pack.items():
                for message in messages:
                    data = message.value
                    
                    # Tách ID ra khỏi dữ liệu
                    obj_id = data.pop('curr_object_id', None) 
                    
                    if obj_id is not None:
                        batch_data.append(data)
                        batch_obj_ids.append(obj_id)

            # Bắt đầu xử lý nếu lô dữ liệu có chứa thông tin
            if batch_data:
                # 3.1. Gom tất cả thành 1 DataFrame duy nhất (chuẩn hóa thứ tự cột)
                df_features = pd.DataFrame(batch_data, columns=FEATURE_COLS)
                
                # 3.2. AI Dự đoán HÀNG LOẠT (Nhanh hơn hàng trăm lần so với chạy for từng dòng)
                predictions = model.predict(df_features)

                # 3.3. Sử dụng Redis Pipeline để gom lệnh ghi
                pipeline = redis_client.pipeline()
                viral_count = 0
                
                for obj_id, pred in zip(batch_obj_ids, predictions):
                    if pred == 1:
                        pipeline.set(f"viral:{obj_id}", "TRUE", ex=1800) # TTL: 30 phút
                        viral_count += 1
                
                # 3.4. Bắn toàn bộ lệnh lên Redis trong 1 lần duy nhất để tiết kiệm kết nối mạng
                if viral_count > 0:
                    pipeline.execute()
                    print(f"🔥 [BÁO ĐỘNG] Đã nhận lô {len(batch_data)} logs -> Phát hiện {viral_count} file sắp Viral! Đã ghi Cache.")
                    
    except KeyboardInterrupt:
        print("\n🛑 Nhận lệnh dừng. Đang đóng kết nối an toàn...")
    finally:
        consumer.close()
        redis_client.close()
        print("👋 AI Worker đã tắt.")

if __name__ == "__main__":
    start_ai_worker()
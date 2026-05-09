import os
import json
import time
from confluent_kafka import Producer

# ==========================================
# ⚙️ CẤU HÌNH THÔNG SỐ CƠ BẢN
# ==========================================
NODE_ID_INT = int(os.getenv("NODE_ID", "1"))
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
SIM_SPEED = float(os.getenv("SIM_SPEED", "1.0"))

# Đường dẫn trỏ tới file JSONL đã được chia nhỏ
MY_DATASET_PATH = f"/app/data/split_logs/node_{NODE_ID_INT}.jsonl"

print(f"🚀 Khởi động Node số {NODE_ID_INT} - Sẵn sàng phục vụ!")

# Cấu hình Kafka Producer
conf = {
    'bootstrap.servers': KAFKA_BROKER, 
    'client.id': f'producer_node_{NODE_ID_INT}',
    'acks': '1',
    'linger.ms': 5,
    'batch.size': 16384
}
producer = Producer(conf)

def run_edge_node():
    if not os.path.exists(MY_DATASET_PATH):
        print(f"❌ NODE {NODE_ID_INT}: Không tìm thấy file data ({MY_DATASET_PATH}).")
        return

    # Lấy mốc thời gian thực lúc hệ thống Edge bắt đầu "Replay" dữ liệu
    sim_start_wall_clock = time.time()

    with open(MY_DATASET_PATH, 'r', encoding='utf-8') as file:
        print(f"▶️ NODE {NODE_ID_INT} bắt đầu mô phỏng bắn dữ liệu với tốc độ x{SIM_SPEED}...")
        
        count = 0
        for line in file:
            line = line.strip()
            if not line: continue
            
            # Đọc dòng JSON thành Dictionary
            row = json.loads(line)
            
            # Xác nhận: timestamp gốc là số nguyên và đơn vị là Giây
            log_ts = int(row['timestamp'])
            
            # --- CƠ CHẾ KIỂM SOÁT NHỊP ĐỘ (ĐỘ TRỄ) ---
            # Ví dụ: log_ts = 15 giây, SIM_SPEED = 1.0 -> Nghĩa là đúng 15 giây sau khi chạy, log này mới được gửi
            target_wall_time = sim_start_wall_clock + (log_ts / SIM_SPEED)
            
            while True:
                # Lấy thời gian hiện tại đang chạy hệ thống từ .time() sau đó so sánh với 
                # target_wall_time để quyết định có nên gửi log này đi hay chưa
                current_wall_time = time.time()
                if current_wall_time >= target_wall_time:
                    break
                
                # Nếu thời gian chờ còn dài hơn 10ms, cho CPU nghỉ ngơi
                diff = target_wall_time - current_wall_time
                if diff > 0.01:
                    time.sleep(diff)

            # --- GÓI HÀNG PAYLOAD ---
            log_payload = {
                "timestamp_rel": log_ts, # Lưu lại số Giây gốc để dễ debug
                "timestamp_abs": int(target_wall_time * 1000), # Ép sang Mili-giây (Mốc thời gian thực) để dọn cỗ cho Spark
                "key": row['key'],
                "node_id": f"NODE_{NODE_ID_INT}",
                "operation": row['operation']
            }
            
            # Bắn lên Kafka Broker
            producer.produce(
                topic='edge_node_req_logs',
                key=str(row['key']),
                value=json.dumps(log_payload)
            )
            
            producer.poll(0)
            
            count += 1
            if count % 5000 == 0:
                print(f"📤 NODE {NODE_ID_INT}: Đã gửi {count} requests (Giây thứ: {log_ts}s)")

    print(f"✅ NODE {NODE_ID_INT} ĐÃ HOÀN TẤT CHIẾN DỊCH!")
    producer.flush()

if __name__ == "__main__":
    run_edge_node()
import os
import csv
import json  # Thêm thư viện json
import requests
import zstandard as zstd
import io

# ==========================================
# ⚙️ CẤU HÌNH THÔNG SỐ CƠ BẢN
# ==========================================
SNIA_SCRIPT_PATH = "/app/data/download_memcached_traces.sh"
OUTPUT_DIR = "/app/data/split_logs/"
TOTAL_NODES = 30

START_ID = 35729       # ID của Part 000
TOTAL_PARTS = 102      # Tổng số Parts cần tải
STEP = 3               # Bước nhảy ID

# ==========================================
# 🛠️ CÁC HÀM XỬ LÝ CHÍNH
# ==========================================

def extract_cookies_from_script(script_path):
    cookies = {}
    print(f"Đang trích xuất Cookies bảo mật từ {script_path}...")
    try:
        with open(script_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.startswith('.iotta'):
                    parts = line.strip().split('\t')
                    if len(parts) >= 7:
                        cookies[parts[5]] = parts[6]
        return cookies
    except FileNotFoundError:
        return None

def get_all_part_ids(start_id, total_parts, step):    
    print(f"Đang tạo danh sách {total_parts} Parts, bắt đầu từ {start_id}, bước nhảy {step}...")
    trace_ids = []
    for i in range(total_parts):
        current_id = start_id + (i * step)
        trace_ids.append(str(current_id))
    print(f"Đã nạp đạn xong {len(trace_ids)} ID để chuẩn bị tải!")
    return trace_ids

def run_ultimate_pipeline():
    # 1. KIỂM TRA & LẤY COOKIES
    cookies = extract_cookies_from_script(SNIA_SCRIPT_PATH)
    if not cookies:
        print(f"Lỗi: Không tìm thấy file {SNIA_SCRIPT_PATH} hoặc không trích xuất được Cookies. Dừng chương trình.")
        return

    # 2. TẠO DANH SÁCH TẢI DỮ LIỆU
    trace_ids = get_all_part_ids(start_id=START_ID, total_parts=TOTAL_PARTS, step=STEP)

    # 3. MỞ 30 VAN NƯỚC (GHI RA JSONL)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    file_handlers = {}
    header = ['timestamp', 'key', 'key_size', 'value_size', 'client_id', 'operation', 'ttl']
    
    print("Khởi tạo 30 luồng ghi dữ liệu JSONL...")
    for i in range(1, TOTAL_NODES + 1):
        # Đổi đuôi file thành .jsonl và chỉ mở file thông thường (không dùng csv_writer nữa)
        f = open(os.path.join(OUTPUT_DIR, f"node_{i}.jsonl"), 'w', encoding='utf-8')
        file_handlers[i] = f

    # 4. TIẾN HÀNH TẢI, GIẢI NÉN VÀ CHIA NHỎ (ZERO-DISK)
    dctx = zstd.ZstdDecompressor()
    total_rows = 0

    for idx, trace_id in enumerate(trace_ids):
        print(f"\nĐang xử lý Part {idx:03d} (ID: {trace_id})")
        download_url = f"http://iotta.unist.ac.kr/traces/key-value/{trace_id}/download?type=file&mirror_chosen=true&sType=curl"
        
        try:
            with requests.get(download_url, cookies=cookies, stream=True) as response:
                response.raise_for_status()
                
                # Giải nén trực tiếp luồng tải trên RAM
                with dctx.stream_reader(response.raw) as reader:
                    text_stream = io.TextIOWrapper(reader, encoding='utf-8')
                    # Vẫn dùng csv.DictReader để mổ xẻ luồng text gốc dễ dàng hơn
                    csv_reader = csv.DictReader(text_stream, fieldnames=header)
                    
                    row_count = 0
                    for row in csv_reader:
                        # Bỏ qua dòng tiêu đề
                        if row['timestamp'] == 'timestamp': continue
                        
                        try:
                            client_id_num = int(row['client_id'])
                        except ValueError:
                            continue

                        # Thuật toán Sharding
                        target_node = (client_id_num % TOTAL_NODES) + 1
                        
                        # --- ĐIỂM THAY ĐỔI CHÍNH ---
                        # Convert dictionary (row) thành chuỗi JSON và nối thêm ký tự xuống dòng (\n)
                        json_line = json.dumps(row)
                        file_handlers[target_node].write(json_line + '\n')
                        # ---------------------------
                        
                        total_rows += 1
                        row_count += 1
                        
                        if row_count % 500000 == 0:
                            print(f"   🔄 Đã chia nhỏ {row_count} dòng của Part này...")
                            
            print(f"Xong Part {idx:03d}! Chuyển sang Part tiếp theo...")
            
        except requests.exceptions.HTTPError as err:
             print(f"LỖI MẠNG TẠI PART {idx:03d}: {err}")
             print("Gợi ý: Có thể file .sh của bạn đã hết hạn hoặc sai IP. Hãy tải lại file mới từ web.")
             break
        except Exception as e:
            print(f"LỖI KHÔNG XÁC ĐỊNH TẠI PART {idx:03d}: {e}")
            break

    # 5. ĐÓNG VAN AN TOÀN
    for f in file_handlers.values():
        f.close()
        
    print(f"\nTổng cộng {total_rows} dòng log dạng JSONL đã được chia ra 30 Nodes.")

if __name__ == "__main__":
    run_ultimate_pipeline()
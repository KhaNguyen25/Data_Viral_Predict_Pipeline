import os
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import glob
import time

# ==========================================
# 1. CẤU HÌNH ĐƯỜNG DẪN
# ==========================================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "../"))

# Trỏ thẳng vào thư mục chứa file Parquet của lớp Gold (Delta Lake bản chất là Parquet)
GOLD_ML_DATASET_PATH = os.path.join(ROOT_DIR, "data", "gold_ml_dataset")
MODEL_SAVE_PATH = os.path.join(ROOT_DIR, "models", "xgboost_cache_model.json")

# Đảm bảo thư mục models tồn tại
os.makedirs(os.path.dirname(MODEL_SAVE_PATH), exist_ok=True)

# Khớp tuyệt đối với luồng Real-time
FEATURE_COLS = [
    "hour_of_day", "hour_sin", "hour_cos", 
    "active_edges_count", "req_count_current", 
    "req_count_previous", "growth_rate"
]
TARGET_COL = "is_viral_next_window"

def train_and_save_model():
    start_time = time.time()
    print("🚀 [1/4] Bắt đầu tiến trình Retrain AI Model...")

    # ==========================================
    # 2. ĐỌC DỮ LIỆU TỪ LỚP GOLD (DELTA/PARQUET)
    # ==========================================
    # Delta Lake lưu dữ liệu thành nhiều file .parquet nhỏ trong thư mục
    # Ta dùng glob để tìm tất cả các file này
    parquet_files = glob.glob(os.path.join(GOLD_ML_DATASET_PATH, "*.parquet"))
    
    if not parquet_files:
        print(f"❌ Lỗi: Không tìm thấy file dữ liệu nào tại {GOLD_ML_DATASET_PATH}. Bạn đã chạy Batch Pipeline chưa?")
        return

    print(f"⏳ [2/4] Đang nạp {len(parquet_files)} file dữ liệu vào RAM...")
    
    # Nối tất cả các file thành 1 bảng Pandas duy nhất
    df_list = [pd.read_parquet(file) for file in parquet_files]
    df = pd.concat(df_list, ignore_index=True)
    
    print(f"✅ Tổng số dòng dữ liệu huấn luyện: {len(df)}")

    # Loại bỏ các dòng có giá trị NaN (đề phòng có lỗi trong lúc join)
    df = df.dropna(subset=FEATURE_COLS + [TARGET_COL])

    # ==========================================
    # 3. CHUẨN BỊ DỮ LIỆU & CHIA TẬP TRAIN/TEST
    # ==========================================
    X = df[FEATURE_COLS]
    y = df[TARGET_COL]

    # Kiểm tra xem có đủ cả 2 nhãn 0 và 1 không
    if len(y.unique()) < 2:
        print("⚠️ CẢNH BÁO: Tập dữ liệu hôm nay chỉ có 1 loại nhãn (Toàn 0 hoặc toàn 1). Không thể train. Giữ nguyên Model cũ.")
        return

    # Chia tỷ lệ 80% học - 20% thi thử để đo lường độ chính xác
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # ==========================================
    # 4. HUẤN LUYỆN MODEL (XGBOOST)
    # ==========================================
    print("⏳ [3/4] Đang huấn luyện (Train) mô hình XGBoost...")
    
    # Cấu hình siêu tham số (Hyperparameters) - Có thể tuning thêm sau này
    # scale_pos_weight: Rất quan trọng vì số lượng file KHÔNG viral luôn nhiều gấp vạn lần file Viral (Imbalanced Data)
    viral_ratio = len(y_train[y_train == 0]) / max(len(y_train[y_train == 1]), 1)
    
    model = xgb.XGBClassifier(
        n_estimators=100,           # Số lượng cây quyết định
        max_depth=6,                # Độ sâu của cây
        learning_rate=0.1,          # Tốc độ học
        scale_pos_weight=viral_ratio, # Xử lý mất cân bằng dữ liệu
        eval_metric='auc',
        random_state=42,
        n_jobs=-1                   # Dùng toàn bộ lõi CPU để train cho lẹ
    )

    # Bắt đầu học
    model.fit(X_train, y_train)

    # ==========================================
    # 5. ĐÁNH GIÁ VÀ LƯU MODEL
    # ==========================================
    print("⏳ [4/4] Đang làm bài thi thử (Testing) và Lưu Model...")
    
    # Cho model làm bài thi trên 20% dữ liệu đã giấu
    y_pred = model.predict(X_test)

    # Đo lường kết quả
    acc = accuracy_score(y_test, y_pred)
    prec = precision_score(y_test, y_pred, zero_division=0)
    rec = recall_score(y_test, y_pred, zero_division=0)
    f1 = f1_score(y_test, y_pred, zero_division=0)

    print("\n📊 --- KẾT QUẢ BÀI THI ---")
    print(f"🎯 Độ chính xác tổng (Accuracy) : {acc:.2%}")
    print(f"🎯 Độ chuẩn xác (Precision)     : {prec:.2%} (Khi đoán Viral, đúng bao nhiêu %)")
    print(f"🎯 Độ bao phủ (Recall)          : {rec:.2%} (Bắt được bao nhiêu % tổng số file Viral thực tế)")
    print(f"🎯 Điểm tổng hợp (F1-Score)     : {f1:.4f}")
    print("--------------------------\n")

    # Chỉ ghi đè model mới nếu nó đủ tốt (Tránh việc data hôm nay quá rác làm model bị ngu đi)
    # Ví dụ: Mình quy định F1-score phải >= 0.5 mới được lưu
    if f1 >= 0.5:
        # Xóa file cũ (nếu có) để tránh xung đột
        if os.path.exists(MODEL_SAVE_PATH):
            os.remove(MODEL_SAVE_PATH)
            
        model.save_model(MODEL_SAVE_PATH)
        print(f"🎉 ĐÃ LƯU MODEL MỚI TẠI: {MODEL_SAVE_PATH}")
    else:
        print("⚠️ Model mới học quá tệ (F1 < 0.5). Hủy lưu file. Hệ thống Real-time sẽ tiếp tục dùng Model cũ của hôm qua.")

    print(f"⏱️ Tổng thời gian chạy: {round(time.time() - start_time, 2)} giây.")

if __name__ == "__main__":
    train_and_save_model()
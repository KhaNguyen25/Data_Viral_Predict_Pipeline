import os
import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import time
from deltalake import DeltaTable

# ==========================================
# 1. CẤU HÌNH ĐƯỜNG DẪN & THAM SỐ
# ==========================================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "../"))

GOLD_ML_DATASET_PATH = os.path.join(ROOT_DIR, "data", "gold_ml_dataset")
MODEL_SAVE_PATH = os.path.join(ROOT_DIR, "models", "xgboost_cache_model.json")

os.makedirs(os.path.dirname(MODEL_SAVE_PATH), exist_ok=True)

FEATURE_COLS = [
    "hour_of_day", "hour_sin", "hour_cos", 
    "active_edges_count", "req_count_current", 
    "req_count_previous", "growth_rate"
]
TARGET_COL = "is_viral_next_window"

# Bổ sung ngưỡng xác suất giống như lúc Train trên Notebook
CUSTOM_THRESHOLD = 0.95

def train_and_save_model():
    start_time = time.time()
    print("🚀 [1/4] Bắt đầu tiến trình Retrain AI Model...")

    # ==========================================
    # 2. ĐỌC DỮ LIỆU TỪ LỚP GOLD BẰNG DELTA LAKE ENGINE
    # ==========================================
    print(f"⏳ [2/4] Đang nạp bảng Delta Table từ: {GOLD_ML_DATASET_PATH}...")
    try:
        dt = DeltaTable(GOLD_ML_DATASET_PATH)
        df = dt.to_pandas()
    except Exception as e:
        print(f"❌ Lỗi: Không thể đọc Delta Table. Chi tiết lỗi: {e}")
        return
    
    if df.empty:
        print("❌ Lỗi: Bảng Delta Table trống. Không có dữ liệu để huấn luyện.")
        return

    print(f"✅ Tổng số dòng dữ liệu thực tế: {len(df)}")
    df = df.dropna(subset=FEATURE_COLS + [TARGET_COL])

    # ==========================================
    # 3. CHUẨN BỊ DỮ LIỆU & CHIA TẬP TRAIN/TEST
    # ==========================================
    X = df[FEATURE_COLS]
    y = df[TARGET_COL]

    if len(y.unique()) < 2:
        print("⚠️ CẢNH BÁO: Tập dữ liệu hôm nay chỉ có 1 loại nhãn. Không thể train. Giữ nguyên Model cũ.")
        return

    # [ĐÃ SỬA]: Thêm shuffle=False để tránh lỗi Data Leakage trong dữ liệu Time-Series
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

    # ==========================================
    # 4. HUẤN LUYỆN MODEL (XGBOOST)
    # ==========================================
    print("⏳ [3/4] Đang huấn luyện (Train) mô hình XGBoost...")
    
    viral_ratio = len(y_train[y_train == 0]) / max(len(y_train[y_train == 1]), 1)
    
    # [ĐÃ SỬA]: Đồng bộ các tham số tốt nhất từ Notebook
    model = xgb.XGBClassifier(
        n_estimators=200,           
        max_depth=5,                
        learning_rate=0.1,          
        scale_pos_weight=viral_ratio * 0.7, # Nhân 0.7 để cân bằng lại
        eval_metric='logloss',
        random_state=42,
        n_jobs=-1                   
    )

    model.fit(X_train, y_train)

    # ==========================================
    # 5. ĐÁNH GIÁ VÀ LƯU MODEL
    # ==========================================
    print(f"⏳ [4/4] Đang đánh giá với Custom Threshold = {CUSTOM_THRESHOLD}...")
    
    # [ĐÃ SỬA]: Sử dụng predict_proba thay vì predict mặc định
    y_prob = model.predict_proba(X_test)[:, 1]
    y_pred = (y_prob >= CUSTOM_THRESHOLD).astype(int)

    acc = accuracy_score(y_test, y_pred)
    prec = precision_score(y_test, y_pred, zero_division=0)
    rec = recall_score(y_test, y_pred, zero_division=0)
    f1 = f1_score(y_test, y_pred, zero_division=0)

    print("\n📊 --- KẾT QUẢ BÀI THI ---")
    print(f"🎯 Độ chính xác tổng (Accuracy) : {acc:.2%}")
    print(f"🎯 Độ chuẩn xác (Precision)     : {prec:.2%}")
    print(f"🎯 Độ bao phủ (Recall)          : {rec:.2%}")
    print(f"🎯 Điểm tổng hợp (F1-Score)     : {f1:.4f}")
    print("--------------------------\n")

    # Giảm điều kiện lưu model xuống một chút vì F1 sẽ thấp hơn khi dùng Threshold khắt khe (0.85)
    # Nhưng bù lại Precision cực cao.
    if f1 >= 0.4:
        # Xóa file cũ trước khi lưu model mới (Tránh lỗi ghi đè đồng thời trên Docker)
        temp_path = MODEL_SAVE_PATH + ".tmp"
        model.save_model(temp_path)
        os.replace(temp_path, MODEL_SAVE_PATH)
        print(f"🎉 ĐÃ LƯU MODEL MỚI TẠI: {MODEL_SAVE_PATH}")
    else:
        print("⚠️ Model mới học quá tệ (F1 < 0.4). Giữ nguyên Model cũ.")

    print(f"⏱️ Tổng thời gian chạy: {round(time.time() - start_time, 2)} giây.")

if __name__ == "__main__":
    train_and_save_model()
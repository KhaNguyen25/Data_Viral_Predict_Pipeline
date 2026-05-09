from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lit, expr, max as spark_max
import xgboost as xgb
import pandas as pd
import os
import time

# --- CẤU HÌNH ĐƯỜNG DẪN & THAM SỐ ---
SILVER_FEATURE_STORE = "/app/data/silver_features/" 
MODEL_PATH = "/app/models/xgboost_cache_model.json"

# Cấu hình Cửa sổ Huấn luyện Trượt (Sliding Training Window)
LOOKBACK_DAYS = 1
WINDOWS_PER_DAY = 24 * 4 # 1 giờ có 4 window (15 phút/window), 1 ngày có 96 windows

def run_retraining_pipeline():
    start_time = time.time()
    
    # ==========================================
    # 0. KHỞI TẠO SPARK VỚI DELTA LAKE
    # ==========================================
    spark = SparkSession.builder \
        .appName("Retrain_Batch_Pipeline") \
        .config("spark.driver.memory", "4g") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # ==========================================
    # 1. ĐỌC DỮ LIỆU TỪ SILVER LAYER (VỚI LOOKBACK WINDOW)
    # ==========================================
    print(f"⏳ [1/4] Đọc dữ liệu Feature từ Delta Lake: {SILVER_FEATURE_STORE}...")
    try:
        df_feat = spark.read.format("delta").load(SILVER_FEATURE_STORE)
    except Exception as e:
        print(f"❌ Chưa có dữ liệu trong Delta Lake hoặc đường dẫn sai: {e}")
        return

    # 1A. Lấy window mới nhất hiện tại (Window đang chạy dở)
    max_win = df_feat.select(spark_max("window_index")).collect()[0][0]
    
    # --- BỔ SUNG LỚP BẢO VỆ COLD START ---
    if max_win is None:
        print("⚠️ Bảng Silver Features hiện đang rỗng. Hủy Retrain.")
        return
        
    # Đếm xem có bao nhiêu window riêng biệt đã hoàn thành
    completed_windows_count = df_feat.filter(col("window_index") < max_win) \
                                     .select("window_index").distinct().count()
    if completed_windows_count < 2:
        print("⚠️ Chưa có đủ ít nhất 2 Window (để so sánh quá khứ-tương lai). Hủy Retrain.")
        return
    
    # 1B. Tính toán điểm bắt đầu (Cắt bỏ dữ liệu quá cũ)
    min_win = max_win - (LOOKBACK_DAYS * WINDOWS_PER_DAY)

    # 1C. Lọc dữ liệu: Chỉ lấy trong khoảng [min_win, max_win)
    # Vừa cắt bỏ quá khứ (>= min_win), vừa cắt bỏ window dở dang hiện tại (< max_win)
    df_feat_filtered = df_feat.filter(
        (col("window_index") >= min_win) & 
        (col("window_index") < max_win)
    )

    print(f"✅ Đã lọc dữ liệu của {LOOKBACK_DAYS} ngày gần nhất (Từ window {min_win} đến {max_win-1})")

    # ==========================================
    # 2. TÍNH P95 VÀ GÁN NHÃN VIRAL (GLOBAL LEVEL)
    # ==========================================
    print("⏳ [2/4] Tính toán ngưỡng P95 và gán nhãn Hiện tại...")
    
    # Lấy ngưỡng 75% toàn hệ thống làm sàn (để loại bỏ các object quá ít view)
    global_floor = df_feat_filtered.approxQuantile("req_count_current", [0.75], 0.05)[0]

    # Tính P95 cho từng Window bằng Window Function
    win_spec = Window.partitionBy("window_index")
    df_labeled = df_feat_filtered.withColumn(
        "p95_limit", expr("percentile_approx(req_count_current, 0.95)").over(win_spec)
    ).withColumn(
        "is_viral_now", 
        ((col("req_count_current") >= col("p95_limit")) & (col("req_count_current") >= lit(global_floor))).cast("int")
    ).drop("p95_limit")

    # ==========================================
    # 3. DỊCH NHÃN TƯƠNG LAI (LABEL SHIFTING)
    # ==========================================
    print("⏳ [3/4] Dịch nhãn về tương lai (Để model học quá khứ -> đoán tương lai)...")
    
    # Tạo một bảng nhãn tương lai bằng cách lùi window_index đi 1
    df_future = df_labeled.select(
        (col("window_index") - 1).alias("fut_win"),
        col("object_id").alias("fut_obj"),
        col("is_viral_now").alias("is_viral_next_window")
    )

    # Join lại vào bảng chính
    df_final = df_labeled.join(df_future,
        (col("window_index") == col("fut_win")) & (col("object_id") == col("fut_obj")),
        "left"
    ).fillna({"is_viral_next_window": 0}).drop("fut_win", "fut_obj", "is_viral_now")

    # Bỏ window cuối cùng sau khi join (vì không có nhãn tương lai của nó)
    max_win_final = df_final.select(spark_max("window_index")).collect()[0][0]
    df_final = df_final.filter(col("window_index") < max_win_final)

    # ==========================================
    # 4. HUẤN LUYỆN MODEL XGBOOST VÀ LƯU TRỮ
    # ==========================================
    print("⏳ [4/4] Tải dữ liệu về Pandas và Train XGBoost...")
    
    FEATURE_COLS = [
        "hour_of_day", "hour_sin", "hour_cos", 
        "active_edges_count", "req_count_current", 
        "req_count_previous", "growth_rate"
    ]
    
    # Collect data về Master Node để train ML
    pd_train = df_final.select(FEATURE_COLS + ["is_viral_next_window"]).toPandas()

    X = pd_train[FEATURE_COLS]
    y = pd_train['is_viral_next_window']

    model = xgb.XGBClassifier(
        n_estimators=100, 
        max_depth=6, 
        learning_rate=0.1, 
        use_label_encoder=False, 
        eval_metric='logloss', 
        n_jobs=-1
    )
    
    model.fit(X, y)

    # Lưu đè file .json (Worker trong Streaming sẽ tự động nạp lại ở lần dự đoán tiếp theo)
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    model.save_model(MODEL_PATH)

    print("===================================================")
    print(f"🎉 HOÀN TẤT RETRAIN MẤT: {round(time.time() - start_time, 2)} GIÂY.")
    print(f"📁 Dữ liệu huấn luyện: {len(pd_train)} bản ghi (trong {LOOKBACK_DAYS} ngày).")
    print(f"🧠 Model XGBoost cập nhật tại: {MODEL_PATH}")

if __name__ == "__main__":
    run_retraining_pipeline()
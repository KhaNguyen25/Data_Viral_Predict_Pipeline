from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lit, expr, max as spark_max
import os
import time

# --- CẤU HÌNH ĐƯỜNG DẪN TỰ ĐỘNG ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "../../"))

SILVER_FEATURE_STORE = os.path.join(ROOT_DIR, "data", "silver_features")
GOLD_ML_DATASET = os.path.join(ROOT_DIR, "data", "gold_ml_dataset")

LOOKBACK_DAYS = 1
WINDOWS_PER_DAY = int(24 * 60 / 15) # 1 ngày có 96 windows (15 phút/window)

# KHOẢNG CÁCH DỰ ĐOÁN (PREDICTION HORIZON)
# Vì mỗi index = 1 phút. Dịch 15 index nghĩa là dạy model dự đoán 15 phút vào tương lai.
PREDICT_AHEAD_WINDOWS = 1 # Trượt 1 index tương đương với 15 phút tương lai (1 x 15 = 15) 

def run_labeling_pipeline():
    start_time = time.time()
    
    # ==========================================
    # 0. KHỞI TẠO SPARK VỚI DELTA LAKE
    # ==========================================
    spark = SparkSession.builder \
        .appName("Gold_Layer_Labeling_Pipeline") \
        .config("spark.driver.memory", "4g") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # ==========================================
    # 1. ĐỌC DỮ LIỆU TỪ SILVER LAYER
    # ==========================================
    print(f"⏳ [1/4] Đọc dữ liệu Feature từ Delta Lake Silver: {SILVER_FEATURE_STORE}...")
    try:
        df_feat = spark.read.format("delta").load(SILVER_FEATURE_STORE)
    except Exception as e:
        print(f"❌ Lỗi đọc Delta Lake: {e}")
        return

    max_win_row_df = df_feat.select(spark_max("window_index")).collect()
    max_win_row = max_win_row_df[0][0] if max_win_row_df else None

    if max_win_row is None:
        print("⚠️ Bảng Silver Features hiện đang rỗng. Hủy tiến trình.")
        return
        
    completed_windows_count = df_feat.filter(col("window_index") < max_win_row) \
                                     .select("window_index").distinct().count()
    
    # Cần ít nhất khoảng thời gian lớn hơn PREDICT_AHEAD_WINDOWS để có thể dịch nhãn
    if completed_windows_count <= PREDICT_AHEAD_WINDOWS:
        print(f"⚠️ Chưa có đủ dữ liệu (cần > {PREDICT_AHEAD_WINDOWS} Windows) để shift label. Hủy tiến trình.")
        return
    
    min_win = max_win_row - (LOOKBACK_DAYS * WINDOWS_PER_DAY)

    df_feat_filtered = df_feat.filter(
        (col("window_index") >= min_win) & 
        (col("window_index") < max_win_row)
    )

    # ==========================================
    # 2. TÍNH TOÁN NGƯỠNG P95 & GÁN NHÃN
    # ==========================================
    print("⏳ [2/4] Tính toán ngưỡng P95 và gán nhãn Hiện tại...")
    
    # Tính sàn (Floor) để loại bỏ các file lèo tèo vài request
    global_floor = df_feat_filtered.approxQuantile("req_count_current", [0.75], 0.05)[0]

    win_spec = Window.partitionBy("window_index")
    df_labeled = df_feat_filtered.withColumn(
        "p95_limit", expr("percentile_approx(req_count_current, 0.95)").over(win_spec)
    ).withColumn(
        "is_viral_now", 
        ((col("req_count_current") >= col("p95_limit")) & (col("req_count_current") >= lit(global_floor))).cast("int")
    ).drop("p95_limit")

    # ==========================================
    # 3. DỊCH NHÃN 15 PHÚT TƯƠNG LAI (LABEL SHIFTING)
    # ==========================================
    print(f"⏳ [3/4] Dịch nhãn về tương lai {PREDICT_AHEAD_WINDOWS} cửa sổ...")
    
    # Bắt file của hiện tại lấy nhãn của 15 phút sau
    df_future = df_labeled.select(
        (col("window_index") - PREDICT_AHEAD_WINDOWS).alias("fut_win"),
        col("curr_object_id").alias("fut_obj"),
        col("is_viral_now").alias("is_viral_next_window")
    )

    df_final = df_labeled.join(df_future,
        (col("window_index") == col("fut_win")) & (col("curr_object_id") == col("fut_obj")),
        "left"
    ).fillna({"is_viral_next_window": 0}).drop("fut_win", "fut_obj", "is_viral_now")

    # BƯỚC QUAN TRỌNG: Cắt bỏ 15 cửa sổ cuối cùng
    # Vì 15 cửa sổ này chưa có dữ liệu tương lai để join, nếu không cắt sẽ bị fillna(0) làm nhiễu mô hình
    safe_max_window = max_win_row - PREDICT_AHEAD_WINDOWS
    df_final = df_final.filter(col("window_index") < safe_max_window)

    # ==============================================================
    # 4. GHI LỚP GOLD
    # ==============================================================
    print("⏳ [4/4] Ghi dữ liệu sạch đã gán nhãn xuống Lớp Gold...")
    df_final.write \
        .format("delta") \
        .mode("overwrite") \
        .save(GOLD_ML_DATASET)

    print("===================================================")
    print(f"🎉 HOÀN TẤT TẠO GOLD LAYER MẤT: {round(time.time() - start_time, 2)} GIÂY.")
    print(f"🧠 Dữ liệu đã sẵn sàng tại: {GOLD_ML_DATASET}")

if __name__ == "__main__":
    run_labeling_pipeline()
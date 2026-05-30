from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sin, cos, pi, from_json, window, expr, approx_count_distinct, hour,
    to_json, struct  # Thêm 2 hàm này để đóng gói dữ liệu cho Kafka
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
import os
from delta.tables import DeltaTable

# --- CẤU HÌNH ĐƯỜNG DẪN TỰ ĐỘNG ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "../../"))

BRONZE_LOGS_STORE = os.path.join(ROOT_DIR, "data", "bronze_logs")
SILVER_FEATURE_STORE = os.path.join(ROOT_DIR, "data", "silver_features")
CHECKPOINT_BRONZE = os.path.join(ROOT_DIR, "data", "checkpoints", "bronze")
CHECKPOINT_SILVER = os.path.join(ROOT_DIR, "data", "checkpoints", "silver")
# Đã xóa MODEL_PATH và các biến global vì Spark không còn ôm đồm AI nữa

def process_silver_micro_batch(df_batch, batch_id):
    """
    Hàm này xử lý Stream-Static Join: Đóng băng Stream, nối với lịch sử trên Delta Lake.
    """
    spark = df_batch.sparkSession

    # Lấy window_index nhỏ nhất trong micro-batch hiện tại
    min_win_df = df_batch.select(expr("min(window_index)")).collect()
    min_win_idx = min_win_df[0][0] if min_win_df else None

    # Nếu micro-batch rỗng, bỏ qua
    if min_win_idx is None:
        return

    # 1. ĐỌC DỮ LIỆU QUÁ KHỨ TỪ SILVER LAYER (TỐI ƯU HÓA ĐỌC)
    try:
        # CHỈ ĐỌC dữ liệu của 2 window gần nhất thay vì đọc toàn bộ bảng
        df_history = spark.read.format("delta").load(SILVER_FEATURE_STORE) \
            .filter(col("window_index") >= min_win_idx - 2)
            
        df_prev = df_history.select(
            col("window_index").alias("prev_win_idx"),
            col("curr_object_id").alias("prev_obj"),
            col("req_count_current").alias("req_count_previous")
        )
    except Exception:
        # Nếu chạy lần đầu tiên, thư mục Silver chưa tồn tại
        empty_schema = StructType([
            StructField("prev_win_idx", LongType(), True),
            # ĐỔI StringType() SANG IntegerType()
            StructField("prev_obj", LongType(), True), 
            StructField("req_count_previous", LongType(), True)
        ])
        df_prev = spark.createDataFrame([], empty_schema)

    # 2. TẠO CỘT TARGET ĐỂ JOIN LÙI VỀ 1 WINDOW
    df_curr = df_batch.withColumn("target_prev_idx", col("window_index") - 1)

    # 3. THỰC HIỆN STATIC JOIN VÀ TÍNH GROWTH RATE
    df_joined = df_curr.join(
        df_prev,
        (col("target_prev_idx") == col("prev_win_idx")) & (col("curr_object_id") == col("prev_obj")),
        "leftOuter"
    ).fillna({"req_count_previous": 0})

    df_final = df_joined.withColumn(
        "growth_rate", 
        (col("req_count_current") - col("req_count_previous")) / (col("req_count_previous") + 1)
    ).drop("target_prev_idx", "prev_win_idx", "prev_obj")

    # Cache lại df_final vào RAM để tối ưu
    df_final.persist()

    # 4. GHI DỮ LIỆU FEATURE HOÀN CHỈNH XUỐNG SILVER LAYER (SỬA LỖI TRÙNG LẶP MERGE/UPSERT)
    if DeltaTable.isDeltaTable(spark, SILVER_FEATURE_STORE):
        deltaTable = DeltaTable.forPath(spark, SILVER_FEATURE_STORE)
        
        # Upsert (Merge) để chống ghi trùng dữ liệu khi Spark chạy lại micro-batch lỗi
        deltaTable.alias("target").merge(
            df_final.alias("source"),
            "target.window_index = source.window_index AND target.curr_object_id = source.curr_object_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    else:
        # Lần chạy đầu tiên (chưa có bảng Delta), tiến hành ghi append bình thường
        df_final.write \
            .format("delta") \
            .mode("append") \
            .save(SILVER_FEATURE_STORE)

    # ==============================================================
    # 5. MÔ HÌNH 3: BẮN KẾT QUẢ (FEATURES) LÊN KAFKA
    # ==============================================================
    # Ép DataFrame thành 2 cột: 'key' (mã file) và 'value' (chuỗi JSON chứa toàn bộ dữ liệu)
    df_kafka_out = df_final.selectExpr(
        "CAST(curr_object_id AS STRING) AS key",
        "to_json(struct(*)) AS value"
    )
    
    # Ra lệnh cho Spark làm Producer bắn dữ liệu vào Topic mới
    df_kafka_out.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("topic", "cdn_features_stream") \
        .save()
    # ==============================================================
    
    # Giải phóng RAM
    df_final.unpersist()

def start_streaming_pipeline(spark: SparkSession):
    json_schema = StructType([
        StructField("timestamp_rel", LongType(), True), 
        StructField("timestamp_abs", LongType(), True), 
        # ĐỔI StringType() SANG IntegerType()
        StructField("key", LongType(), True), 
        StructField("node_id", StringType(), True), 
        StructField("operation", StringType(), True) 
    ])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "edge_node_req_logs") \
        .option("startingOffsets", "latest") \
        .load()

    # 1. PARSE DỮ LIỆU THÔ
    df_parsed = kafka_df.filter(col("value").isNotNull()).select(
        from_json(col("value").cast("string"), json_schema).alias("data")
    ).select(
        (col("data.timestamp_abs") / 1000).cast("timestamp").alias("timestamp"), 
        col("data.key").alias("object_id"),
        col("data.node_id").alias("node_id"),
        col("data.operation").alias("operation") 
    )

    # ==============================================================
    # LUỒNG 1: GHI BRONZE LAYER (Lưu toàn bộ Raw Log)
    # ==============================================================
    query_bronze = df_parsed.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_BRONZE) \
        .trigger(processingTime="15 minutes") \
        .start(BRONZE_LOGS_STORE)

    # ==============================================================
    # LUỒNG 2: TÍNH TOÁN CƠ BẢN VÀ ĐẨY VÀO FOREACHBATCH
    # ==============================================================
    df_get_requests = df_parsed.filter(col("operation") == "get")

    df_agg = df_get_requests \
        .withWatermark("timestamp", "15 minutes") \
        .groupBy(
            window(col("timestamp"), "15 minutes").alias("time_window"), # Chỉ truyền 1 tham số
            col("object_id"),
        ).agg(
            count("*").alias("req_count_current"),
            approx_count_distinct(col("node_id")).alias("active_edges_count") 
        )
    
    # Chỉ tính các đặc trưng tĩnh ở luồng Stream (Giờ, chu kỳ)
    df_features = df_agg.withColumn(
        "hour_of_day", hour(col("time_window.start"))
    ).withColumn(
        "hour_sin", sin(2 * pi() * col("hour_of_day") / 24)
    ).withColumn(
        "hour_cos", cos(2 * pi() * col("hour_of_day") / 24)
    )

    # Cấu trúc đầu ra siêu gọn
    df_output = df_features.select(
        (col("time_window.start").cast("long") / 900).cast("long").alias("window_index"), # Đổi 300 thành 900
        "hour_of_day", "hour_sin", "hour_cos",
        col("object_id").alias("curr_object_id"), 
        "active_edges_count",
        "req_count_current"
    )

    # Đẩy toàn bộ dataframe tĩnh này vào hàm process_silver_micro_batch
    query_silver = df_output.writeStream \
        .outputMode("update") \
        .foreachBatch(process_silver_micro_batch) \
        .trigger(processingTime="15 minutes") \
        .option("checkpointLocation", CHECKPOINT_SILVER) \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("CDN_Realtime_Cache_Predictor") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    print("🚀 Đang kích hoạt luồng Real-time (Bronze + Silver với Stream-Static Join)...")
    start_streaming_pipeline(spark)
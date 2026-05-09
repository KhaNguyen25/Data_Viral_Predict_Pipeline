from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, regexp_extract, when, 
    sin, cos, pi, from_json, window, expr, approx_count_distinct, hour
)
from pyspark.sql.types import StructType, StructField, StringType, LongType
import os

# ==========================================
# 1. HÀM XỬ LÝ TRÊN WORKER (EMBEDDED INFERENCE)
# ==========================================
def predict_and_save_partition(partition):
    """
    Hàm này sẽ được Spark bốc và ném xuống chạy trên từng con Worker.
    Mỗi Worker sẽ tự load mô hình và tự ghi vào Redis.
    """
    import xgboost as xgb
    import redis
    import pandas as pd

    # Chuyển dữ liệu phân mảnh (partition) thành danh sách
    records = list(partition)
    if not records:
        return

    # Khởi tạo Redis Client (Trỏ tới container 'redis')
    redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

    # Khởi tạo và Load mô hình AI trực tiếp vào RAM của Worker
    model_path = "/app/models/xgboost_cache_model.json"
    if not os.path.exists(model_path):
        # Nếu chưa có model (chưa chạy Batch lần nào), tạm thời bỏ qua
        return

    model = xgb.XGBClassifier()
    model.load_model(model_path)

    # Chuyển đổi dữ liệu sang Pandas để đưa vào XGBoost
    df_pd = pd.DataFrame([r.asDict() for r in records])
    
    FEATURE_COLS = [
        "hour_of_day", "hour_sin", "hour_cos", 
        "active_edges_count", "req_count_current", 
        "req_count_previous", "growth_rate"
    ]
    
    X = df_pd[FEATURE_COLS]
    
    # AI Dự đoán (Real-time)
    predictions = model.predict(X)

    # Đẩy kết quả những file dự đoán là Viral (1) lên Redis
    for i, row in df_pd.iterrows():
        if predictions[i] == 1:
            object_id = row['object_id']
            # Cache sống trong 1 giờ (3600s)
            redis_client.set(f"viral:{object_id}", "true", ex=3600)

# ==========================================
# 2. HÀM ĐIỀU PHỐI TỪNG BATCH CỦA STREAMING
# ==========================================
def process_micro_batch(df_batch, batch_id):
    """
    Hàm này chạy mỗi khi Spark gom đủ 1 phút dữ liệu.
    """
    # BƯỚC A: Lưu kết quả đã gom nhóm (Silver Layer) xuống Delta Lake
    # Để luồng Batch ban đêm dùng lại, không phải tính từ đầu!
    df_batch.write \
        .format("delta") \
        .mode("append") \
        .save("/app/data/silver_features/")

    # BƯỚC B: Phân phát dữ liệu xuống các Worker để AI dự đoán
    df_batch.foreachPartition(predict_and_save_partition)

# ==========================================
# 3. PIPELINE XỬ LÝ CHÍNH
# ==========================================
def load_process_data_streaming(spark: SparkSession):
    # (Đoạn code bạn viết được giữ nguyên 100%)
    json_schema = StructType([
        StructField("timestamp_rel", LongType(), True), 
        StructField("timestamp_abs", LongType(), True), 
        StructField("key", StringType(), True), 
        StructField("node_id", StringType(), True), 
        StructField("operation", StringType(), True) 
    ])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "edge_node_req_logs") \
        .option("startingOffsets", "latest") \
        .load()

    df_base = kafka_df.filter(col("value").isNotNull()).select(
        from_json(col("value").cast("string"), json_schema).alias("data")
    ).select(
        (col("data.timestamp_abs") / 1000).cast("timestamp").alias("timestamp"), 
        col("data.key").alias("object_id"),
        col("data.node_id").alias("node_id"),
        col("data.operation").alias("operation") 
    ).filter(col("operation") == "get")

    # WATERMARK 1 MINUTE
    df_agg = df_base \
        .withWatermark("timestamp", "1 minutes") \
        .groupBy(
            window(col("timestamp"), "15 minutes").alias("time_window"),
            col("object_id"),
        ).agg(
            count("*").alias("req_count_current"),
            approx_count_distinct(col("node_id")).alias("active_edges_count") 
        )
    
    regex_pattern = "^([a-zA-Z0-9]+)[^a-zA-Z0-9]"
    df_agg = df_agg.withColumn(
        "namespace",
        when(col("object_id").rlike(regex_pattern), regexp_extract(col("object_id"), regex_pattern, 1))
        .otherwise("unknown")
    )

    df_features = df_agg.withColumn(
        "hour_of_day", hour(col("time_window.start"))
    ).withColumn(
        "hour_sin", sin(2 * pi() * col("hour_of_day") / 24)
    ).withColumn(
        "hour_cos", cos(2 * pi() * col("hour_of_day") / 24)
    )

    df_curr = df_features.alias("curr")
    
    df_prev = df_features.select(
        col("time_window.end").alias("prev_window_end"), 
        col("object_id").alias("prev_obj_id"),
        col("req_count_current").alias("req_count_previous")
    ).alias("prev")

    join_conditions = [
        col("curr.object_id") == col("prev.prev_obj_id"),
        col("curr.time_window.start") == col("prev.prev_window_end"),
        expr("curr.time_window.start >= prev.prev_window_end"),
        expr("curr.time_window.start <= prev.prev_window_end + interval 15 minutes")
    ]

    df_joined = df_curr.join(df_prev, join_conditions, "leftOuter") \
        .fillna({"req_count_previous": 0})

    df_final = df_joined.withColumn(
        "growth_rate", 
        (col("req_count_current") - col("req_count_previous")) / (col("req_count_previous") + 1)
    )

    df_output = df_final.select(
        (col("curr.time_window.start").cast("long") / 900).cast("long").alias("window_index"),
        "hour_of_day", "hour_sin", "hour_cos",
        "curr.object_id", "curr.namespace",
        "active_edges_count",
        "req_count_current", "req_count_previous", "growth_rate"
    )

    return df_output

# ==========================================
# 4. KHỞI TẠO VÀ CHẠY ỨNG DỤNG
# ==========================================
if __name__ == "__main__":
    # Nhúng cấu hình Delta Lake vào luồng Streaming
    spark = SparkSession.builder \
        .appName("CDN_Realtime_Cache_Predictor") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
        
    # Giảm mức độ log để dễ nhìn console
    spark.sparkContext.setLogLevel("WARN")

    print("🚀 Đang kích hoạt luồng Real-time...")
    final_df = load_process_data_streaming(spark)

    # Đẩy ra Output Sink
    query = final_df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_micro_batch) \
        .trigger(processingTime="1 minute") \
        .option("checkpointLocation", "/app/data/checkpoints/realtime_pipeline/") \
        .start()

    query.awaitTermination()
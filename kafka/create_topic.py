from confluent_kafka.admin import AdminClient, NewTopic

def create_log_topic():
    # Khởi tạo AdminClient trỏ vào cổng Kafka
    admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})
    topic_name = "edge_node_req_logs"
    
    # Thiết lập topic
    new_topic = NewTopic(
        topic=topic_name, 
        num_partitions=3, 
        replication_factor=1,
        config={
            'retention.ms': '86400000', 
            'cleanup.policy': 'delete'
        }
    )

    # Tạo topic
    fs = admin_client.create_topics([new_topic])
    
    for topic, future in fs.items():
        try:
            future.result()
            print(f"Thành công: Đã tạo topic '{topic}'")
        except Exception as e:
            print(f"Topic '{topic}' có thể đã tồn tại: {e}")
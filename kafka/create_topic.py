from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

def create_log_topic():
    print("Khởi tạo Admin Client kết nối tới Kafka...")
    admin_client = KafkaAdminClient(
        bootstrap_servers="kafka:9092",  # Đồng bộ gọi tên mạng Docker
        client_id='edge_node_setup'
    )

    topic_name = "edge_node_req_logs"
    
    # Thiết lập topic cho Raw Logs (Giữ trong 1 ngày = 86.400.000 ms)
    topic_list = [NewTopic(
        name=topic_name,
        num_partitions=3, 
        replication_factor=1,
        topic_configs={
            'retention.ms': '86400000', 
            'cleanup.policy': 'delete'
        }
    )]

    try:
        print(f"Đang tạo topic '{topic_name}'...")
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"✅ Đã tạo Topic '{topic_name}' thành công!")
    except TopicAlreadyExistsError:
        print(f"⚠️ Topic '{topic_name}' đã tồn tại từ trước.")
    except Exception as e:
        print(f"❌ Lỗi khi tạo topic: {e}")
    finally:
        admin_client.close()

if __name__ == "__main__":
    create_log_topic()
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

def create_kafka_topic():
    print("Khởi tạo Admin Client kết nối tới Kafka...")
    admin_client = KafkaAdminClient(
        bootstrap_servers="kafka:9092", 
        client_id='cdn_admin_setup'
    )

    topic_name = "cdn_features_stream"
    
    # Cấu hình chi tiết cho Topic
    topic_list = []
    topic_list.append(NewTopic(
        name=topic_name,
        num_partitions=3,       # Chia làm 3 luồng (Cho phép tối đa 3 AI Worker chạy song song)
        replication_factor=1,   # Lưu 1 bản copy (Nếu cụm Kafka của bạn có 3 node thì đổi thành 3)
        topic_configs={
            'retention.ms': '3600000',      # Chỉ giữ dữ liệu trên ổ cứng Kafka trong 1 giờ (3.600.000 ms)
            'cleanup.policy': 'delete'      # Quá 1 giờ tự động xóa bỏ để giải phóng ổ cứng
        }
    ))

    try:
        print(f"Đang tạo topic '{topic_name}'...")
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print("✅ Đã tạo Topic thành công với cấu hình chuẩn Production!")
    except TopicAlreadyExistsError:
        print(f"⚠️ Topic '{topic_name}' đã tồn tại từ trước.")
    except Exception as e:
        print(f"❌ Lỗi khi tạo topic: {e}")
    finally:
        admin_client.close()

if __name__ == "__main__":
    create_kafka_topic()
# Dùng bản Python nhẹ
FROM python:3.10-slim

# Đặt thư mục làm việc trong container
WORKDIR /app

# Copy và cài đặt thư viện
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy file code vào
COPY dataset_splitter.py .

# Khi container khởi động, chạy file dataset_splitter.py
CMD ["python", "dataset_splitter.py"]
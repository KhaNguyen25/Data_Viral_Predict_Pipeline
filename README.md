# Distributed Edge Deduplication & Viral Traffic Prediction Pipeline

## Project Overview
This repository contains a comprehensive, distributed system designed to process CDN logs, predict file virality at the edge, and perform data deduplication based on the Ripple paper architecture. By integrating Java-based edge nodes, Apache Spark processing, and Python-based AI workers, the pipeline optimizes network traffic and predicts content popularity in real-time. Detailed methodologies, system evaluations, and project outcomes are documented in the official report `BaoCao.pdf`.

## System Architecture & Components
The project is containerized using Docker (`docker-compose.yml`) and is divided into several highly decoupled microservices:

### 1. Edge Deduplication System (`Ripple-main/`)
A Java-based implementation handling distributed deduplication at the edge to minimize redundant traffic.
* **Core Logic:** Implements Edge Clients and Servers (`EdgeClient.java`, `EdgeServer.java`).
* **Probabilistic Data Structures:** Utilizes advanced filters such as Bloom Filters, Counting Bloom Filters, and Quotient Filters (`BloomFilter.java`, `QuotientFilter.java`) for efficient memory usage and fast membership querying.
* **Integrations:** Communicates with Redis (`RedisUtility.java`) for caching and Kafka (`KafkaLogConsumer.java`, `KafkaRequestProducer.java`) for message streaming.

### 2. AI Workspace (`ai-workspace/` & `models/`)
Python-based environment for predicting file virality using Machine Learning.
* **AI Workers:** `ai_worker.py` consumes streaming data to make real-time viral predictions at the edge.
* **Model Retraining:** `ai_retrain.py` automates the retraining pipeline as new CDN log patterns emerge.
* **Model Cache:** Utilizes an optimized XGBoost model (`xgboost_cache_model.json`) for fast and accurate classification.

### 3. Data Processing Pipeline (`spark/`)
Apache Spark jobs responsible for heavy data lifting, transformation, and feature extraction from CDN logs.
* **Real-time Processing:** `realtime_pipeline.py` processes streaming logs via Spark Structured Streaming.
* **Batch Processing:** `batch_pipeline.py` handles historical data aggregation and periodic ETL tasks.

### 4. Event Streaming (`kafka/`)
* Automation scripts (`create_topic.py`, `create_topic_for_predict.py`) to initialize Kafka topics that connect the edge nodes, Spark pipelines, and AI workers.

## Repository Structure
```text
Data_Viral_Predict_Pipeline/
│
├── .devcontainer/          # VS Code dev container settings
├── Ripple-main/            # Java source code for Edge Deduplication (Ripple architecture)
├── ai-workspace/           # Python AI workers and retraining scripts
├── kafka/                  # Kafka topic initialization scripts
├── models/                 # Pre-trained XGBoost models (JSON format)
├── spark/                  # PySpark scripts for batch and realtime log processing
├── BaoCao.pdf              # Official project report and documentation
├── Dockerfile.spark        # Docker image definition for the Spark environment
└── docker-compose.yml      # Multi-container orchestration (Kafka, Spark, Redis, etc.)

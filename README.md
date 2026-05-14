Spark Kafka Streaming Join Assignment

Docker로 독립 Kafka 서버를 실행하고, Spark Structured Streaming을 이용해 `impressions` 스트림과 `clicks` 스트림을 Stream-Stream Join하는 과제입니다.

실행 환경

- Docker Desktop
- Kafka: Docker Compose
- Spark: Docker Container `apache/spark:4.0.0`
- Python producer

파일 구성

```text
spark-kafka-streaming/
├─ docker-compose.yml
├─ producer.py
└─ stream_join.py

실행 방법
1. Kafka 실행
docker compose up -d

2. Spark 컨테이너 실행
docker run --rm -it --network spark-kafka-streaming_default -v "${PWD}:/app" -w /app apache/spark:4.0.0 bash

3. Stream-Join 실행
export HOME=/tmp && mkdir -p /tmp/.ivy2 && /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 stream_join.py

4. Producer 실행
새 터미널에서 프로젝트 폴더로 이동 후 실행합니다.
python producer.py

실행 결과
stream_join.py 실행 터미널에서 Stream-Stream Join 결과가 Batch 단위로 출력됩니다.
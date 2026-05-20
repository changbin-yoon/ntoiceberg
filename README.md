# kafka-to-iceberg

[![Scala](https://img.shields.io/badge/Scala-2.12.18-red?logo=scala)](https://scala-lang.org)
[![Spark](https://img.shields.io/badge/Spark-3.5.6-orange?logo=apachespark)](https://spark.apache.org)
[![Iceberg](https://img.shields.io/badge/Iceberg-1.8.0-blue)](https://iceberg.apache.org)
[![Java](https://img.shields.io/badge/Java-17-007396?logo=openjdk)](https://adoptium.net)

Kafka 토픽의 메시지를 읽어 Apache Iceberg 테이블에 적재하는 Spark 배치 파이프라인입니다.  
Airflow 에서 10분 간격으로 스케줄링되며, Kubernetes 환경(Spark Operator)에서 실행됩니다.

---

## Architecture

```
Kafka Topic
    │
    │  startingTimestamp ~ endingTimestamp (10분 윈도우)
    ▼
Spark Batch Job (Scala)
    │  - JSON 메시지 파싱 (동적 스키마)
    │  - timestamp 변환 / null 제거 / 중복 제거
    │  - event_date 파티션 컬럼 추가
    ▼
Apache Iceberg Table
    │  Catalog : Hive Metastore (HMS)
    │  Format  : Parquet + Snappy
    │  Storage : S3 / MinIO
    ▼
Airflow DAG (10분 스케줄)
    SparkKubernetesOperator → SparkKubernetesSensor
```

---

## Tech Stack

| 역할 | 기술                          |
|---|-----------------------------|
| 언어 | Scala 2.12.18               |
| 실행 엔진 | Apache Spark 3.5.6          |
| 테이블 포맷 | Apache Iceberg 1.8.0        |
| 카탈로그 | Hive Metastore (HMS)        |
| 메시지 큐 | Apache Kafka                |
| 스케줄러 | Apache Airflow              |
| 실행 환경 | Kubernetes + Spark Operator |
| 빌드 도구 | SBT 1.9.9                   |
| Java | OpenJDK 17                  |

---

## Prerequisites

로컬 개발 환경에 아래 도구가 설치되어 있어야 합니다.

```bash
java -version   # openjdk 17
sbt --version   # 1.9.9
git --version
```

설치되어 있지 않다면:

```bash
# Mac
brew install --cask temurin@17
brew install sbt

# 환경변수 등록 (~/.zshrc)
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
```

---

## Getting Started

### 1. 클론

```bash
git clone https://github.com/ycb/ntoiceberg.git
cd ntoiceberg
```

### 2. 의존성 다운로드

```bash
sbt update
```

### 3. 컴파일

```bash
sbt compile
```

### 4. 테스트

```bash
sbt test
```

### 5. fat JAR 빌드

```bash
sbt assembly

# 빌드 결과
ls -lh target/scala-2.12/kafka-to-iceberg-1.0.0.jar
```

---

## 실행 방법

### spark-submit (로컬 테스트)

```bash
spark-submit \
  --class com.yourcompany.pipeline.KafkaToIceberg \
  --master local[*] \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.0,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  target/scala-2.12/kafka-to-iceberg-1.0.0.jar \
  --kafka-servers localhost:9092 \
  --topic order-events \
  --table hive_prod.raw.order_events \
  --start-ts-ms 1716000000000 \
  --end-ts-ms   1716000600000 \
  --hms-uri thrift://localhost:9083 \
  --job-config '{"app_name":"test","topic":"order-events","table":"hive_prod.raw.order_events","fields":[{"name":"id","type":"string","nullable":false}]}'
```

### Airflow (운영)

Airflow DAG `kafka_to_iceberg_10min_batch` 가 10분마다 자동 실행됩니다.  
DAG 파일 위치: `airflow/dags/dag_kafka_to_iceberg.py`

---

## Job Config 설명

`--job-config` 인자에 JSON 형식으로 토픽별 스키마와 옵션을 전달합니다.

```json
{
  "app_name":         "kafka-iceberg-order-events",
  "topic":            "order-events",
  "table":            "hive_prod.raw.order_events",
  "primary_key":      "order_id",
  "dedup_keys":       ["order_id", "event_type"],
  "timestamp_field":  "ordered_at",
  "timestamp_format": "yyyy-MM-dd'T'HH:mm:ss",
  "partition_source": "kafka_timestamp",
  "fields": [
    {"name": "order_id",   "type": "string",       "nullable": false},
    {"name": "user_id",    "type": "long",          "nullable": true},
    {"name": "amount",     "type": "decimal(12,2)", "nullable": true},
    {"name": "status",     "type": "string",        "nullable": true},
    {"name": "ordered_at", "type": "string",        "nullable": true}
  ]
}
```

| 키 | 설명 | 필수 |
|---|---|---|
| `app_name` | Spark 앱 이름 | ✅ |
| `topic` | Kafka 토픽명 | ✅ |
| `table` | Iceberg 테이블 (catalog.db.table) | ✅ |
| `fields` | 메시지 스키마 필드 목록 | ✅ |
| `primary_key` | null 제거 기준 컬럼 | 선택 |
| `dedup_keys` | 중복 제거 기준 컬럼 목록 | 선택 |
| `timestamp_field` | 문자열 → Timestamp 변환 대상 | 선택 |
| `timestamp_format` | timestamp 포맷 (기본: `yyyy-MM-dd'T'HH:mm:ss`) | 선택 |
| `partition_source` | event_date 파티션 기준 컬럼 (기본: `kafka_timestamp`) | 선택 |

지원 타입: `string` `long` `integer` `double` `float` `boolean` `timestamp` `date` `decimal(p,s)`

---

## Project Structure

```
kafkatoiceberg/
├── build.sbt                          # 빌드 설정 (의존성, assembly)
├── project/
│   ├── build.properties               # SBT 버전 고정
│   └── plugins.sbt                    # SBT 플러그인
├── src/
│   ├── main/scala/com/yourcompany/pipeline/
│   │   ├── KafkaToIceberg.scala       # 메인 진입점 (파이프라인 전체 흐름)
│   │   ├── JobConfig.scala            # 설정 case class + JSON 파싱
│   │   └── SchemaBuilder.scala        # 동적 Spark 스키마 빌더
│   └── test/
│       ├── scala/                     # 단위 테스트
│       └── resources/
│           └── log4j2-test.xml        # 로컬 테스트 로그 설정
└── airflow/
    ├── dags/
    │   ├── dag_kafka_to_iceberg.py    # 메인 DAG (10분 배치)
    │   └── job_configs.py             # 토픽별 스키마 설정
    └── README.md
```

---

## 새 토픽 추가 방법

`airflow/dags/job_configs.py` 에 항목 추가 후 DAG에 태스크 추가합니다.

```python
# job_configs.py
"new-topic": {
    "app_name": "kafka-iceberg-new-topic",
    "topic":    "new-topic",
    "table":    "hive_prod.raw.new_topic",
    "fields": [
        {"name": "id",   "type": "string", "nullable": False},
        {"name": "name", "type": "string", "nullable": True},
    ],
}
```

---

## Iceberg 유지보수

매일 새벽 3시에 `iceberg_maintenance` DAG 가 자동 실행됩니다.

| 작업 | 주기 | 설명 |
|---|---|---|
| Compaction | 매일 | 작은 파일 128MB 단위로 병합 |
| Snapshot Expire | 매일 | 7일 이전 스냅샷 삭제 |
| Orphan File 제거 | 매일 | 실패한 작업이 남긴 파일 정리 |

---

## License

MIT License
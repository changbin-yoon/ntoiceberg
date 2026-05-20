package com.ycb.pipeline

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.util.{Failure, Success, Try}

// ─────────────────────────────────────────────────────────
// Kafka → Iceberg 범용 배치 Spark 작업
//
// spark-submit 인자:
//   --kafka-servers  kafka-broker:9092
//   --topic          order-events
//   --table          hive_prod.raw.order_events
//   --start-ts-ms    1716000000000   (epoch ms)
//   --end-ts-ms      1716000600000   (epoch ms)
//   --hms-uri        thrift://hive-metastore:9083
//   --job-config     '{ "fields": [...], ... }'
// ─────────────────────────────────────────────────────────
object KafkaToIceberg {

  private val log: Logger = LoggerFactory.getLogger(getClass)

  // ── CLI 인자 파싱 ────────────────────────────────────────
  case class CliArgs(
                      kafkaServers:  String,
                      topic:         String,
                      table:         String,
                      startTsMs:     Long,
                      endTsMs:       Long,
                      hmsUri:        String,
                      jobConfigJson: String,
                    )

  /**
   * "--key value" 쌍으로 넘어오는 인자를 Map 으로 변환
   * "--kafka-servers" → "kafka.servers" (하이픈 → 점)
   */
  private def parseArgs(args: Array[String]): CliArgs = {
    val argMap = args
      .sliding(2, 2)
      .collect { case Array(k, v) =>
        k.stripPrefix("--").replace("-", ".") -> v
      }
      .toMap

    def required(key: String): String =
      argMap.getOrElse(
        key,
        throw new IllegalArgumentException(s"필수 인자 누락: --${key.replace(".", "-")}")
      )

    CliArgs(
      kafkaServers  = required("kafka.servers"),
      topic         = required("topic"),
      table         = required("table"),
      startTsMs     = required("start.ts.ms").toLong,
      endTsMs       = required("end.ts.ms").toLong,
      hmsUri        = argMap.getOrElse("hms.uri", "thrift://hive-metastore:9083"),
      jobConfigJson = required("job.config"),
    )
  }

  // ── SparkSession (HMS Catalog) ───────────────────────────
  private def createSpark(appName: String, hmsUri: String): SparkSession = {
    val spark = SparkSession.builder()
      .appName(appName)
      // Iceberg Spark Extension 활성화
      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      )
      // HMS Catalog 설정
      .config("spark.sql.catalog.hive_prod", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.hive_prod.type", "hive")
      .config("spark.sql.catalog.hive_prod.uri", hmsUri)
      // 기본 카탈로그 지정
      .config("spark.sql.defaultCatalog", "hive_prod")
      // 성능 튜닝
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    log.info(s"SparkSession 생성 완료 | appName=$appName | HMS=$hmsUri")
    spark
  }

  // ── Kafka 배치 읽기 (timestamp 기반) ────────────────────
  /**
   * startingTimestamp ~ endingTimestamp 범위의 메시지를 읽음
   * Airflow 의 data_interval_start/end 를 epoch ms 로 변환해 전달
   */
  private def readKafka(
                         spark:      SparkSession,
                         servers:    String,
                         topic:      String,
                         startTsMs:  Long,
                         endTsMs:    Long,
                       ): DataFrame = {
    val fmt = DateTimeFormatter.ISO_INSTANT
    log.info(
      "Kafka 읽기 시작 | topic={} | {} ~ {}",
      topic,
      fmt.format(Instant.ofEpochMilli(startTsMs).atOffset(ZoneOffset.UTC)),
      fmt.format(Instant.ofEpochMilli(endTsMs).atOffset(ZoneOffset.UTC)),
    )

    val df = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", servers)
      .option("subscribe", topic)
      .option("startingTimestamp", startTsMs.toString)
      .option("endingTimestamp", endTsMs.toString)
      // 오프셋 유실 시 예외 대신 경고 (at-least-once)
      .option("failOnDataLoss", "false")
      .load()

    val count = df.count()
    log.info("Kafka 읽기 완료 | topic={} | 건수={}", topic, count)
    df
  }

  // ── 메시지 파싱 & 변환 ───────────────────────────────────
  /**
   * Kafka raw DataFrame → 비즈니스 컬럼 파싱
   *
   * 처리 순서:
   *   1. value(binary) → JSON 파싱 (JobConfig 스키마 적용)
   *   2. Kafka 메타 컬럼 추가 (kafka_timestamp, partition, offset)
   *   3. timestamp 문자열 → Timestamp 타입 변환 (선택)
   *   4. event_date 파티션 컬럼 추가
   *   5. null PK 제거
   *   6. 중복 제거
   */
  private def parseAndTransform(
                                 df:     DataFrame,
                                 schema: StructType,
                                 cfg:    JobConfig,
                               ): DataFrame = {

    // 1~2. JSON 파싱 + Kafka 메타 컬럼
    var result = df.select(
      from_json(col("value").cast("string"), schema).alias("msg"),
      col("timestamp").alias("kafka_timestamp"),
      col("partition").alias("kafka_partition"),
      col("offset").alias("kafka_offset"),
    ).select("msg.*", "kafka_timestamp", "kafka_partition", "kafka_offset")

    // 3. timestamp 문자열 → Timestamp 변환
    //    timestamp_field 가 None 이면 스킵 (이미 Timestamp 타입인 경우)
    cfg.timestamp_field.foreach { tsField =>
      val fieldExists = schema.fieldNames.contains(tsField)
      if (fieldExists) {
        result = result.withColumn(
          tsField,
          to_timestamp(col(tsField), cfg.timestamp_format),
        )
        log.info(s"timestamp 변환 적용 | field=$tsField | format=${cfg.timestamp_format}")
      } else {
        log.warn(s"timestamp_field '$tsField' 가 스키마에 없음. 변환 스킵.")
      }
    }

    // 4. 파티션 컬럼 + 처리 시각
    result = result
      .withColumn("event_date",   to_date(col(cfg.partition_source)))
      .withColumn("processed_at", current_timestamp())

    // 5. null PK 제거
    cfg.primary_key.foreach { pk =>
      val before = result.count()
      result = result.filter(col(pk).isNotNull)
      val after = result.count()
      if (before != after)
        log.warn(s"null PK 제거 | field=$pk | 제거건수=${before-after}")
    }

    // 6. 중복 제거
    if (cfg.dedup_keys.nonEmpty) {
      val before = result.count()
      result = result.dropDuplicates(cfg.dedup_keys)
      val after = result.count()
      if (before != after)
        log.info(s"중복 제거 완료 | keys= ${cfg.dedup_keys.mkString(",")}| 제거건수=${before - after}")
    }

    log.info(s"변환 완료 | 최종 건수=${result.count()}")
    result
  }

  // ── Iceberg 테이블 자동 생성 ─────────────────────────────
  /**
   * DataFrame 스키마를 기반으로 Iceberg 테이블 생성 (없으면)
   * event_date 컬럼으로 파티션 분리
   */
  private def ensureTable(
                           spark:        SparkSession,
                           table:        String,
                           df:           DataFrame,
                           partitionCol: String = "event_date",
                         ): Unit = {
    // DataFrame 스키마 → DDL 컬럼 문자열
    val fieldDdl = df.schema.fields.map { f =>
      s"  ${f.name} ${f.dataType.sql}"
    }.mkString(",\n")

    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $table (
         |$fieldDdl
         |)
         |USING iceberg
         |PARTITIONED BY ($partitionCol)
         |TBLPROPERTIES (
         |  'write.format.default'             = 'parquet',
         |  'write.parquet.compression-codec'  = 'snappy',
         |  'write.target-file-size-bytes'     = '134217728',
         |  'history.expire.max-snapshot-age-ms' = '604800000'
         |  'location' = 's3a://'
         |)
         |""".stripMargin
    )

    log.info("테이블 확인/생성 완료 | table={}", table)
  }

  // ── Iceberg 적재 ─────────────────────────────────────────
  /**
   * append 방식으로 Iceberg 테이블에 적재
   * merge-schema: true → 컬럼 추가 자동 반영
   */
  private def writeIceberg(df: DataFrame, table: String): Unit = {
    val count = df.count()
    log.info(s"Iceberg 적재 시작 | table=$table | 건수=$count")

    df.writeTo(table)
      .option("merge-schema", "true")
      .append()

    log.info(s"Iceberg 적재 완료 | table=$table")
  }

  // ── 메인 ─────────────────────────────────────────────────
  def main(args: Array[String]): Unit = {

    // 1. CLI 인자 파싱
    val cli = parseArgs(args)

    // 2. job-config JSON 파싱
    val config: JobConfig = Try(JobConfig.fromJson(cli.jobConfigJson)) match {
      case Success(c) => c
      case Failure(e) =>
        log.error(s"--job-config JSON 파싱 실패: ${e.getMessage}")
        sys.exit(1)
    }

    // 3. Spark 스키마 빌드
    val schema = SchemaBuilder.build(config.fields)
    log.info(s"스키마 빌드 완료 | fields=${config.fields.map(_.name).mkString(", ")}")

    // 4. SparkSession 생성
    val spark = createSpark(config.app_name, cli.hmsUri)

    // 5. 파이프라인 실행
    Try {
      // Kafka 읽기
      val rawDf = readKafka(spark, cli.kafkaServers, cli.topic, cli.startTsMs, cli.endTsMs)

      if (rawDf.rdd.isEmpty()) {
        log.info("처리할 메시지 없음. 정상 종료.")
        sys.exit(0)
      }

      // 파싱 & 변환
      val transformedDf = parseAndTransform(rawDf, schema, config)

      // 테이블 생성 (없으면)
      ensureTable(spark, cli.table, transformedDf)

      // Iceberg 적재
      writeIceberg(transformedDf, cli.table)

      log.info(s"배치 작업 완료 | topic=${cli.topic} | table=${cli.table}")

    } match {
      case Success(_) =>
      // 정상 종료

      case Failure(e) =>
        log.error(s"배치 작업 실패 | topic=${cli.topic} | 원인=${e.getMessage}")
        spark.stop()
        sys.exit(1)
    }

    spark.stop()
  }
}
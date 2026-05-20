package com.ycb.pipeline

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

// ─────────────────────────────────────────────────────────
// 스키마 필드 하나의 설정
//
// 예시 JSON:
//   {"name": "order_id", "type": "string", "nullable": false}
// ─────────────────────────────────────────────────────────
case class FieldConfig(
                        name:     String,
                        @JsonProperty("type") `type`: String,  // "type" 은 Scala 예약어 → annotation 필요
                        nullable: Boolean = true,
                      )

// ─────────────────────────────────────────────────────────
// 토픽별 Job 전체 설정
//
// 예시 JSON:
// {
//   "app_name":         "kafka-iceberg-order-events",
//   "topic":            "order-events",
//   "table":            "hive_prod.raw.order_events",
//   "primary_key":      "order_id",
//   "dedup_keys":       ["order_id", "event_type"],
//   "timestamp_field":  "ordered_at",
//   "timestamp_format": "yyyy-MM-dd'T'HH:mm:ss",
//   "partition_source": "kafka_timestamp",
//   "fields": [
//     {"name": "order_id",   "type": "string",        "nullable": false},
//     {"name": "user_id",    "type": "long",           "nullable": true},
//     {"name": "amount",     "type": "decimal(12,2)",  "nullable": true},
//     {"name": "ordered_at", "type": "string",         "nullable": true}
//   ]
// }
// ─────────────────────────────────────────────────────────
case class JobConfig(
                      app_name:         String,
                      topic:            String,
                      table:            String,
                      fields:           List[FieldConfig],
                      primary_key:      Option[String] = None,
                      dedup_keys:       List[String]   = Nil,
                      timestamp_field:  Option[String] = None,
                      timestamp_format: String         = "yyyy-MM-dd'T'HH:mm:ss",
                      partition_source: String         = "kafka_timestamp",
                    )

// ─────────────────────────────────────────────────────────
// JobConfig 파싱 유틸
// ─────────────────────────────────────────────────────────
object JobConfig {

  // Jackson ObjectMapper (싱글턴, 스레드 안전)
  private val mapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    // 알 수 없는 필드가 있어도 예외 발생하지 않음 (설정 확장 시 하위 호환)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    m
  }

  /** JSON 문자열 → JobConfig 파싱 */
  def fromJson(json: String): JobConfig =
    mapper.readValue(json, classOf[JobConfig])
}
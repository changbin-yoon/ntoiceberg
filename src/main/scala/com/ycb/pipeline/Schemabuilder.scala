package com.ycb.pipeline

import org.apache.spark.sql.types._

// ─────────────────────────────────────────────────────────
// FieldConfig 리스트 → Spark StructType 동적 변환
//
// 지원 타입:
//   string, long, integer, int, double, float,
//   boolean, timestamp, date, decimal(precision,scale)
// ─────────────────────────────────────────────────────────
object SchemaBuilder {

  /**
   * FieldConfig 리스트를 받아 Spark StructType 으로 변환
   *
   * @param fields JobConfig 에서 파싱된 필드 목록
   * @return Spark DataFrame 스키마
   */
  def build(fields: List[FieldConfig]): StructType =
    StructType(
      fields.map { f =>
        StructField(
          name     = f.name,
          dataType = resolveType(f.`type`),
          nullable = f.nullable,
        )
      }
    )

  // ── 타입 문자열 → Spark DataType ──────────────────────
  private def resolveType(typeStr: String): DataType = {
    val normalized = typeStr.trim.toLowerCase

    // decimal(precision, scale) 처리
    if (normalized.startsWith("decimal(") && normalized.endsWith(")")) {
      val inner              = normalized.slice("decimal(".length, normalized.length - 1)
      val Array(prec, scale) = inner.split(",").map(_.trim.toInt)
      return DecimalType(prec, scale)
    }

    normalized match {
      case "string"          => StringType
      case "long"            => LongType
      case "integer" | "int" => IntegerType
      case "short"           => ShortType
      case "byte"            => ByteType
      case "double"          => DoubleType
      case "float"           => FloatType
      case "boolean"         => BooleanType
      case "timestamp"       => TimestampType
      case "date"            => DateType
      case "binary"          => BinaryType
      case other =>
        throw new IllegalArgumentException(
          s"""지원하지 않는 타입: '$other'
             |지원 목록: string, long, integer, int, short, byte,
             |           double, float, boolean, timestamp, date,
             |           binary, decimal(precision,scale)""".stripMargin
        )
    }
  }
}
// ─────────────────────────────────────────────────────────
// 프로젝트 전역 설정
// ─────────────────────────────────────────────────────────
ThisBuild / organization := "com.ycb"
ThisBuild / version      := "1.0.0"
ThisBuild / scalaVersion := "2.12.21"

// Java 17 타겟 컴파일 버전 명시
ThisBuild / javacOptions ++= Seq("-source", "17", "-target", "17")

// ─────────────────────────────────────────────────────────
// 의존성 버전 상수 (한 곳에서 관리)
// ─────────────────────────────────────────────────────────
val sparkVersion   = "3.5.6"
val icebergVersion = "1.8.0"
val jacksonVersion = "2.15.2"
val log4jVersion   = "2.23.1"
val scalatestVersion = "3.2.18"

// ─────────────────────────────────────────────────────────
// 메인 프로젝트
// ─────────────────────────────────────────────────────────
lazy val root = (project in file("."))
  .settings(
    name := "kafka-to-iceberg",

    // ── Scala 컴파일 옵션 ──────────────────────────────────
    scalacOptions ++= Seq(
      "-encoding", "utf8",       // 소스 인코딩
      "-deprecation",            // deprecated API 경고
      "-feature",                // 고급 기능 사용 시 경고
      "-unchecked",              // 타입 소거 경고
      "-Xlint:unused",           // 미사용 import/변수 경고
      "-Ywarn-dead-code",        // 도달 불가 코드 경고
      "-Ywarn-value-discard",    // 반환값 버림 경고
    ),

    // ── Java 17 JVM 옵션 ───────────────────────────────────
    // Spark 3.5.x + Java 17 조합에서 필수 (모듈 접근 허용)
    javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      // Windows 로컬 실행 시 Spark 임시 디렉토리
      "-Djava.io.tmpdir=C:/tmp/spark",
      // 로컬 Spark 로그 레벨
      "-Dlog4j.configurationFile=src/test/resources/log4j2-test.xml",
    ),

    // ── 테스트 설정 ────────────────────────────────────────
    // fork=true 필수: javaOptions 가 테스트 JVM 에 적용되려면 별도 프로세스 필요
    Test / fork        := true,
    Test / javaOptions ++= (javaOptions).value,
    // 테스트 병렬 실행 비활성화 (Spark SparkContext 충돌 방지)
    Test / parallelExecution := false,

    // ── 의존성 ────────────────────────────────────────────
    libraryDependencies ++= Seq(

      // ── Spark (Provided: 클러스터 환경에 이미 존재) ──────
      "org.apache.spark" %% "spark-core"           % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql"            % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % Provided,

      // ── Iceberg (Provided: --packages 로 주입) ───────────
      "org.apache.iceberg" % "iceberg-spark-runtime-3.5_2.12" % icebergVersion % Provided,

      // ── Jackson (Spark에 포함 → Provided) ────────────────
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion % Provided,
      "com.fasterxml.jackson.core"    % "jackson-databind"     % jacksonVersion % Provided,

      // ── 로깅 ─────────────────────────────────────────────
      // Spark가 Log4j2를 사용 → Provided, 로컬 테스트용만 compile scope
      "org.apache.logging.log4j" % "log4j-api"        % log4jVersion % Provided,
      "org.apache.logging.log4j" % "log4j-core"       % log4jVersion % Provided,
      "org.apache.logging.log4j" % "log4j-slf4j2-impl"% log4jVersion % Test,

      // ── 테스트 ────────────────────────────────────────────
      "org.scalatest"    %% "scalatest"       % scalatestVersion % Test,
      // 로컬 단위 테스트용 Spark (Test scope = JAR에 미포함)
      "org.apache.spark" %% "spark-core"      % sparkVersion     % Test,
      "org.apache.spark" %% "spark-sql"       % sparkVersion     % Test,
    ),

    // ── assembly (fat JAR) 설정 ───────────────────────────
    assembly / mainClass       := Some("com.ycb.pipeline.KafkaToIceberg"),
    assembly / assemblyJarName := s"${name.value}-${version.value}.jar",

    // Provided 의존성을 fat JAR 에서 제외
    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      cp filter { jar =>
        val n = jar.data.getName
        n.startsWith("spark-")   ||
        n.startsWith("hadoop-")  ||
        n.startsWith("iceberg-") ||
        n.startsWith("kafka-")   ||
        n.startsWith("jackson-")
      }
    },

    assembly / assemblyMergeStrategy := {
      // SPI 파일 충돌 시 합치기 (Iceberg, Kafka 등)
      case PathList("META-INF", "services", _*)           => MergeStrategy.concat
      // Spark 설정 파일 합치기
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
      case PathList("META-INF", _*)                       => MergeStrategy.discard
      // Log4j, Hadoop 설정은 첫 번째 것 사용
      case "log4j2.xml"                                   => MergeStrategy.first
      case "core-default.xml"                             => MergeStrategy.first
      case "reference.conf"                               => MergeStrategy.concat
      case _                                              => MergeStrategy.first
    },
  )

// fat JAR 빌드
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.2.0")

// 의존성 트리 확인 (sbt dependencyTree)
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")

// 코드 포맷터 (선택)
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")

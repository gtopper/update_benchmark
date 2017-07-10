name := "update_benchmark"

organization := "io.iguaz"

version := "0.1"

scalaVersion := "2.11.8"

scalacOptions += "-target:jvm-1.7"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"

assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter {_.data.getName.contains("v3io")}
}

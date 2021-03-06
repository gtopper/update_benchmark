name := "update_benchmark"

organization := "io.iguaz"

version := "0.1"

scalaVersion := "2.11.8"

scalacOptions += "-target:jvm-1.8"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter {
    _.data.getName.contains("v3io")
  }
}

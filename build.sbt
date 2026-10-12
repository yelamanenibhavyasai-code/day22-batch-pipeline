
ThisBuild / scalaVersion := "2.12.18"

name := "day22-batch-pipeline"
version := "0.1.0"

libraryDependencies +=
  "org.apache.spark" %% "spark-sql" % "3.5.3"

Compile / run / fork := true

Compile / run / javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
)

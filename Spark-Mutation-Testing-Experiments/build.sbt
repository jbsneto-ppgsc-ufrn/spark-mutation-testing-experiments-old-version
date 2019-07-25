name := "Spark-Mutation-Testing-Experiments"

version := "1.0"

organization := "br.ufrn.dimap.forall.spark"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.1"

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

parallelExecution in Test := false

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "com.holdenkarau" %% "spark-testing-base" % "2.3.1_0.12.0" % "test",
  "com.holdenkarau" %% "spark-testing-kafka-0_8" % "2.3.1_0.12.0" % "test"
)


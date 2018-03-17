name := "spark-streaming-kinesis-sample"

version := "1.0"

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Xlint")

libraryDependencies ++= Seq( "org.apache.spark" % "spark-core_2.11" % "2.2.1" % "provided" )

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.1"

libraryDependencies += "org.apache.spark" % "spark-streaming-kinesis-asl_2.11" % "2.2.1"

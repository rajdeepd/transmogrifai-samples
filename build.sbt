name := "TransmogrifAI-samples"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.2",
  "org.apache.spark" %% "spark-mllib" % "2.3.2",
  "org.apache.spark" %% "spark-sql" % "2.3.2",
  "com.salesforce.transmogrifai" %% "transmogrifai-core" % "0.6.0",
  "net.liftweb" %% "lift-webkit" % "3.3.0",
  "com.google.code.gson" % "gson" % "2.8.5"
)

name := "activeLearning-tf"

version := "1.0"

organization := "mx.itam.deiis.activelearning"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "0.9.1",
  "org.scalaz" %% "scalaz-core" % "7.0.6"
)

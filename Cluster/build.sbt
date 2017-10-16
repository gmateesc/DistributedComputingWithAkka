name := "Distributed application using Akka Cluster"

version := "1.0"

scalaVersion := "2.11.7"

sbtVersion := "0.13.8"

resolvers += "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.4.1",
  "com.typesafe.akka" %% "akka-remote" % "2.4.1",
  "com.typesafe.akka" %% "akka-cluster" % "2.4.1")

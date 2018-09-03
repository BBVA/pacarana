name := "sequence-handler-core"

version := "0.2.9"

scalaVersion := "2.12.1"

organization := "com.bbvalabs"

val akkaVersion = "2.4.17"
val logbackVersion = "1.2.3"

libraryDependencies ++= Seq(
  "org.reactivemongo" %% "reactivemongo" % "0.12.1",
  "com.chuusai" %% "shapeless" % "2.3.2",
  "io.verizon.delorean" %% "core" % "1.2.40-scalaz-7.2",
  "org.scalacheck" %% "scalacheck" % "1.13.5",
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "de.flapdoodle.embed" % "de.flapdoodle.embed.mongo" % "2.0.0" % "test",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.25",
  "ch.qos.logback" % "logback-classic" % logbackVersion,
  ("com.typesafe.akka" %% "akka-stream" % akkaVersion).exclude("com.typesafe.akka", "actor"),
  "org.julienrf" %% "reactivemongo-derived-codecs" % "2.1-SNAPSHOT"
)

resolvers +=
  "Artifactory" at "http://artifactory.default.svc.cluster.local:8081/artifactory/ml/reactive-mongo-derived/snapshots"

publishTo := {
  val nexus = "http://artifactory.default.svc.cluster.local:8081/artifactory/ml/sequence-handler-core/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "snapshots/") 
  else
    Some("releases"  at nexus + "releases/")
}

unmanagedJars in Compile += file("lib/reactivemongo-derived-codecs-assembly-2.1-SNAPSHOT.jar")

artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.copy(`classifier` = Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)









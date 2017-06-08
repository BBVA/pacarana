name := "sequence-handler-data-api"

version := "0.2.0"

scalaVersion := "2.12.1"

organization := "com.bbvalabs"

val akkaVersion = "2.4.17"

libraryDependencies ++= Seq(
  "org.reactivemongo" %% "reactivemongo" % "0.12.1",
  "com.chuusai" %% "shapeless" % "2.3.2",
  "io.verizon.delorean" %% "core" % "1.2.40-scalaz-7.2",
  "org.scalacheck" %% "scalacheck" % "1.13.5",
  "de.flapdoodle.embed" % "de.flapdoodle.embed.mongo" % "2.0.0" % "test",
  ("com.typesafe.akka" %% "akka-stream" % akkaVersion).exclude("com.typesafe.akka", "actor")
)

publishTo := {
  val nexus = "http://artifactory.default.svc.cluster.local:8081/artifactory/ml/sequence-handler/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "snapshots/") 
  else
    Some("releases"  at nexus + "releases/")
}

artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.copy(`classifier` = Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)








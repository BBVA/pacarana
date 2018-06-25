name := "sequence-handler-core"

version := "0.3.1"

scalaVersion := "2.12.1"

organization := "com.bbvalabs"

val akkaVersion = "2.5.12"
val logbackVersion = "1.2.3"

libraryDependencies ++= Seq(
  "org.reactivemongo" %% "reactivemongo" % "0.12.5",
  "com.chuusai" %% "shapeless" % "2.3.2",
  "io.verizon.delorean" %% "core" % "1.2.40-scalaz-7.2",
  "junit" % "junit" % "4.12" % "test",
  "org.scalacheck" %% "scalacheck" % "1.13.5",
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "de.flapdoodle.embed" % "de.flapdoodle.embed.mongo" % "2.0.0" % "test",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.25",
  "ch.qos.logback" % "logback-classic" % logbackVersion,
  ("com.typesafe.akka" %% "akka-stream" % akkaVersion).exclude("com.typesafe.akka", "actor"),
  "org.julienrf" %% "reactivemongo-derived-codecs" % "3.0.0",
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,
  "org.scalatest" %% "scalatest" % "3.0.5" % Test
)

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")


publishTo := {
  val nexus = "https://globaldevtools.bbva.com/artifactory-api/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "innovation-labs-releases/") 
  else
    Some("releases"  at nexus + "innovation-labs-releases/")
}

artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.copy(`classifier` = Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)









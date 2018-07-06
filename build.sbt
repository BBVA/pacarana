name := "pacarana"

version := "0.1.0"

scalaVersion := "2.12.1"

organization := "com.bbva"

val akkaVersion     = "2.5.12"
val logbackVersion  = "1.2.3"
val slf4jVersion    = "1.7.25"
val reactiveMongoCodecsVersion = "3.0.0"
val deloreanVersion       = "1.2.40-scalaz-7.2"
val shapelessVersion      = "2.3.2"
val reactiveMongoVersion  = "0.12.5"
val scalatestVersion      = "3.0.5"
val embedMongo            = "2.0.0"

libraryDependencies ++= Seq(
  "org.reactivemongo"   %% "reactivemongo"    % reactiveMongoVersion,
  "com.chuusai"         %% "shapeless"        % shapelessVersion,
  "io.verizon.delorean" %% "core"             % deloreanVersion,
  "com.typesafe.akka"   %% "akka-slf4j"       % akkaVersion,
 ("com.typesafe.akka"   %% "akka-stream"      % akkaVersion).exclude("com.typesafe.akka", "actor"),
  "org.julienrf"        %% "reactivemongo-derived-codecs" % reactiveMongoCodecsVersion,
  "com.typesafe.akka"   %% "akka-stream-testkit" % akkaVersion % Test,
  "org.scalatest"       %% "scalatest" % scalatestVersion % Test,
  "org.slf4j"           % "slf4j-api"    % slf4jVersion,
  "org.slf4j"           % "slf4j-simple" % slf4jVersion,
  "de.flapdoodle.embed" % "de.flapdoodle.embed.mongo" % embedMongo % Test
)

artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.copy(`classifier` = Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)









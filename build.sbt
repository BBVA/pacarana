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

libraryDependencies ++= Seq(
  "org.reactivemongo"   %% "reactivemongo"    % reactiveMongoVersion,
  "com.chuusai"         %% "shapeless"        % shapelessVersion,
  "io.verizon.delorean" %% "core"             % deloreanVersion,
  "com.typesafe.akka"   %% "akka-slf4j"       % akkaVersion,
  "org.slf4j"           % "log4j-over-slf4j"  % slf4jVersion,
  "ch.qos.logback"      % "logback-classic"   % logbackVersion,
 ("com.typesafe.akka"  %% "akka-stream"      % akkaVersion).exclude("com.typesafe.akka", "actor"),
  "org.julienrf"        %% "reactivemongo-derived-codecs" % reactiveMongoCodecsVersion,
  "junit"               % "junit"             % "4.12" % Test,
  "de.flapdoodle.embed" % "de.flapdoodle.embed.mongo" % "2.0.0" % Test,
  "com.typesafe.akka"   %% "akka-stream-testkit" % akkaVersion % Test,
  "org.scalatest"       %% "scalatest" % "3.0.5" % Test
)

artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.copy(`classifier` = Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)









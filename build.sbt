ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "DataQualityGate"
  )

libraryDependencies ++= Seq(
  "com.amazon.deequ" % "deequ" % "2.0.1-spark-3.2",
  "org.apache.spark" %% "spark-sql" % "3.2.1" % "provided"
)
// META-INF discarding
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyJarName in assembly := s"${name.value}_${version.value}.jar".replace("-SNAPSHOT", "")

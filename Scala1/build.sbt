lazy val root = (project in file(".")).
  settings(
    name := "Scala1",
    version := "1.0",
    scalaVersion := "2.10.6",
    mainClass in Compile := Some("WordCount")
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.4.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.2.0" % "provided",
  "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.2.0"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

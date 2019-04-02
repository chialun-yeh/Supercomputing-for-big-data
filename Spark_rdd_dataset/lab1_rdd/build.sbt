ThisBuild / scalaVersion := "2.11.12"

lazy val example = (project in file("."))
  .settings(
    name := "Lab1 project",
    fork in run := true,

    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1", 
    libraryDependencies += "org.json4s" %% "json4s-native" % "3.6.1"
  )

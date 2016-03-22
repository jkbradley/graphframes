// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html

val sparkVer = sys.props.getOrElse("spark.version", "1.4.1")
val sparkBranch = sparkVer.substring(0, 3)

val defaultScalaVer = sparkBranch match {
  case "1.4" => "2.10.4"
  case "1.5" => "2.10.4"
  case "1.6" => "2.10.5"
  case "2.0" => "2.11.7"
}
val scalaVer = sys.props.getOrElse("scala.version", defaultScalaVer)

sparkVersion := sparkVer

scalaVersion := scalaVer

spName := "graphframes/graphframes"

// Don't forget to set the version
version := s"0.1.0-spark$sparkBranch"

// All Spark Packages need a license
licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))


// Add Spark components this package depends on, e.g, "mllib", ....
sparkComponents ++= Seq("graphx", "sql", "catalyst")

// uncomment and change the value below to change the directory where your zip artifact will be created
// spDistDirectory := target.value

// add any Spark Package dependencies using spDependencies.
// e.g. spDependencies += "databricks/spark-avro:0.1"

libraryDependencies += "org.scalatest" %% "scalatest" % (if (scalaVer < "2.11") "2.0" else "2.2.6") % "test"

parallelExecution := false

val sparkHackDir = if (sparkBranch == "1.4") {
  "spark-1.4"
} else if (sparkBranch == "1.5" || sparkBranch == "1.6") {
  "spark-1.5-6"
} else {
  "spark-2.0"
}

unmanagedSourceDirectories in Compile ++= Seq(baseDirectory.value / "src" / "main" / sparkHackDir)

scalacOptions in (Compile, doc) ++= Seq(
  "-groups",
  "-implicits",
  "-skip-packages", Seq("org.apache.spark").mkString(":"))

scalacOptions in (Test, doc) ++= Seq("-groups", "-implicits")

autoAPIMappings := true

unmanagedSourceDirectories in Test ++=
  Seq(baseDirectory.value / "src" / "test" / (if (sparkVersion.value.substring(0, 3) == "1.4") "spark-1.4" else "spark-x"))

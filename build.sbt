name := "pschema"

scalaVersion := "2.12.16"

val sparkVersion = "3.2.2"
// val wdsubVersion            = "0.0.16"
val shexsVersion = "0.2.16"
val srdfVersion = "0.1.122"
val utilsVersion = "0.2.25"

val catsVersion = "2.8.0"
val declineVersion = "2.2.0"
val jacksonVersion = "2.13.3"
val munitVersion = "0.7.29"
val munitEffectVersion = "1.0.7"
val sparkFastTestsVersion = "1.0.0"
val scallopVersion = "4.0.4"
val wikidataToolkitVersion = "0.14.0"
val graphFramesVersion = "0.8.2-spark3.2-s_2.12"

lazy val MUnitFramework = new TestFramework("munit.Framework")

val Java11 = JavaSpec.temurin("11")

ThisBuild / githubWorkflowJavaVersions := Seq(Java11)

homepage := Some(url("https://github.com/weso/pschema"))

sonatypeProfileName := "es.weso"

organization := "es.weso"

licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT"))

developers := List(
  Developer(
    id = "labra",
    name = "Jose Emilio Labra Gayo",
    email = "jelabra@gmail.com",
    url = url("https://weso.labra.es")
  )
)

resolvers ++= Seq(
  "Spark Packages Repo" at "https://repos.spark-packages.org/"
)

libraryDependencies ++= Seq(
  // Spark dependencies.
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-graphx" % sparkVersion % "provided",

  // WESO components dependencies.
  "es.weso" %% "shex" % shexsVersion,
  // "es.weso" %% "srdf"         % srdfVersion % Test,
  // "es.weso" %% "srdfjena"     % srdfVersion % Test,
  // "es.weso" %% "srdf4j"       % srdfVersion % Test,
  // "es.weso" %% "utils"        % utilsVersion % Test,

  // Wikidata toolkit dependencies.
  "org.wikidata.wdtk" % "wdtk-dumpfiles" % wikidataToolkitVersion % Test,
  "org.wikidata.wdtk" % "wdtk-wikibaseapi" % wikidataToolkitVersion % Test,
  "org.wikidata.wdtk" % "wdtk-datamodel" % wikidataToolkitVersion % Test,
  "org.wikidata.wdtk" % "wdtk-rdf" % wikidataToolkitVersion % Test,
  "org.wikidata.wdtk" % "wdtk-storage" % wikidataToolkitVersion % Test,
  "org.wikidata.wdtk" % "wdtk-util" % wikidataToolkitVersion % Test,

  // Jackson dependencies.
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion % Test,
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion % Test,
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion % Test,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.3" % Test,
  "es.weso" %% "wshex" % shexsVersion % Test,

  // Cats dependencies.
  "org.typelevel" %% "cats-core" % catsVersion,
  "org.typelevel" %% "cats-kernel" % catsVersion,

  // Testing dependencies.
  "com.github.mrpowers" %% "spark-fast-tests" % sparkFastTestsVersion % Test,

  // Munit dependencies.
  "org.scalameta" %% "munit" % munitVersion % Test,
  "org.typelevel" %% "munit-cats-effect-3" % munitEffectVersion % Test,

  // GraphFrames dependencies.
  "graphframes" % "graphframes" % graphFramesVersion
)

package es.weso.wdsub.spark

import org.rogach.scallop.ScallopConf

class SparkJobConfig(arguments: Seq[String]) extends ScallopConf(arguments) with Serializable {

  banner(
    """Welcome to WDSub Spark Launcher!!!
      |To launch any job please provide the following parameters""".stripMargin
  )

  val jobMode = opt[String](required = true, name = "mode", descr = "Select the mode to run: (test|cluster)")
  val jobName = opt[String](required = true, name = "name", descr = "Enter a name for the job")
  val jobInputDump = opt[String](required = true, name = "dump", descr = "Select the input dump by entering the path")
  val jobInputSchema = opt[String](required = true, name = "schema", descr = "Select the input schema by entering the path")
  val jobOutputDir = opt[String](required = true, name = "out", descr = "Select the output directory. Please remove the trailing slash")
  val keepShapes = opt[Boolean](required = false, default = Some(false), name = "keep_shapes", descr = "Show the shapes")
  val keepErrors = opt[Boolean](required = false, default = Some(false), name = "keep_errors", descr = "Keep errors during validation")

  printHelp()
  verify()
}

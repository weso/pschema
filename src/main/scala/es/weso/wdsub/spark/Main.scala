package es.weso.wdsub.spark

object Main extends Serializable {

  def main(args: Array[String]): Unit =
  {
    val sparkJobConf = new SparkJobConfig( args )
    new SparkJobDefinition( sparkJobConf )
  }
}

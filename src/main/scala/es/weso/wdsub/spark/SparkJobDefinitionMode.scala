package es.weso.wdsub.spark

object SparkJobDefinitionMode extends Enumeration with Serializable
{
  type SparkJobDefinitionMode = Value
  val Test, Cluster = Value

  def fromString(string: String): SparkJobDefinitionMode =
  {
    if(string.contentEquals("test")) return SparkJobDefinitionMode.Test
    else return SparkJobDefinitionMode.Cluster
  }
}

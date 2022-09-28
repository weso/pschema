package es.weso.wdsub.spark

@SerialVersionUID(10L)
class ResultFile() extends Serializable
{
  var jobName: String = ""
  var jobDate: String = ""
  var cores: String = ""
  var mem: String = ""
  var time: String = ""
  var jobResults: String = ""

  override def toString: String = {
    val sb = new StringBuilder
    sb.append( "==========================================\n" )
    sb.append( "JOB INFORMATION\n" )
    sb.append( "Job Name: " + jobName + "\n" )
    sb.append( "Job Date: " + jobDate + "\n" )
    sb.append( "Job Cores: " + cores + "\n" )
    sb.append( "Job Mem: " + mem + "\n" )
    sb.append( "Job Time: " + time + " seconds\n" )
    sb.append( "==========================================\n" )
    sb.append( "JOB RESULTS\n" )
    sb.append( jobResults )
    sb.toString
  }

}

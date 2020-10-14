package Classes
import org.apache.spark
import org.apache.log4j.Logger
import org.apache.log4j.Level


object Data_files_load{
  private def  set_spark():org.apache.spark.sql.SparkSession = {
   val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

    return spark
  }
}

class Data_files_load(filename:String) {
  this set_sessio_and_ignore_warnings()
  var spark_session:org.apache.spark.sql.SparkSession = Data_files_load.set_spark()

  def set_sessio_and_ignore_warnings(){
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }

  def load_csv(): org.apache.spark.sql.DataFrame ={
    val df = this.spark_session.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load(this.filename)//hdfs:///csv/file/dir/file.csv
      .toDF()
    return df
  }




}

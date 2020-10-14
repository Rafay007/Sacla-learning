import org.apache.spark
import org.apache.log4j.Logger
import org.apache.log4j.Level



object Main {
  def main(args:Array[String]):Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;


    val df = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/abdulrafay/PycharmProjects/cookiepedia_refined/Cookiepedia_scraped_data.csv")//hdfs:///csv/file/dir/file.csv
      .toDF()

    println(df.show(5))
    println("\n\n")

      for (row <- df.take(2)){
        println("Row"+row.getClass)
        return
      }

  }


}

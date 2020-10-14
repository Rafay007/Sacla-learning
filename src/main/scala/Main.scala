import Classes.Data_files_load
import org.apache.commons.math3.optim.MaxIter
import org.apache.spark
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Main {
  def main(args:Array[String]):Unit = {
    println("showing csv data\n")
    val obj1 = new Data_files_load("/Users/abdulrafay/PycharmProjects/cookiepedia_refined/Cookiepedia_scraped_data.csv")
    println(obj1.load_csv().show(2))

    println("showing Json data\n")
    val obj2 = new Data_files_load("/Users/abdulrafay/Downloads/DataEngineer-home-task/test-data-for-spark.json")
    val data=obj2.load_json()
    println(data.show(2))
    println(data.describe().show())



//    println(df.show(5))
//    println("\n\n")
//
//      for (row <- df.take(2)){
//        println("Row"+row.getClass)
//        return
//      }



  }


}

import Classes.Data_files_load
import org.apache.spark
import org.apache.log4j.Logger
import org.apache.log4j.Level



object Main {
  def main(args:Array[String]):Unit = {

    val obj = new Data_files_load("/Users/abdulrafay/PycharmProjects/cookiepedia_refined/Cookiepedia_scraped_data.csv")
    println(obj.load_csv().show(2))

//    println(df.show(5))
//    println("\n\n")
//
//      for (row <- df.take(2)){
//        println("Row"+row.getClass)
//        return
//      }



  }


}

import Classes.Data_files_load
import org.apache.commons.math3.optim.MaxIter
import org.apache.spark
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, OneHotEncoderModel, StringIndexer, VectorIndexer}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.mllib.recommendation.ALS
object Main {
  def main(args:Array[String]):Unit = {
//    println("showing csv data\n")
//    val obj1 = new Data_files_load("/Users/abdulrafay/PycharmProjects/cookiepedia_refined/Cookiepedia_scraped_data.csv")
//    println(obj1.load_csv().show(2))


    val conf = org.apache.spark.SparkContext().setAppName("read text file in pyspark").setMaster("spark://127.0.0.1:7077")
    val sc = SparkContext(conf=conf)


    # Read file into RDD
      lines = sc.textFile("hdfs://127.0.0.1:8020/tmp/rafay/data.txt")

    # Call collect() to get all data
    llist = lines.collect()

    # print line one by line
    for line in llist:
      print(line)


    //    println("showing Json data\n")
//    val obj2 = new Data_files_load("/Users/abdulrafay/Downloads/DataEngineer-home-task/test-data-for-spark.json")
//    val data=obj2.load_json()
//    println(data.show(2))
//    println(data.describe().show())



//    case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
//    def parseRating(str: String): Rating = {
//      val fields = str.split("::")
//      assert(fields.size == 4)
//      Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
//    }

//    val ratings =  data.map(x=>parseRating(x.toString()))
//      .toDF()
//



//    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    // Build the recommendation model using ALS on the training data
//    val als = new ALS()
//      .Max(5)
//      .setRegParam(0.01)
//      .setUserCol("userId")
//      .setItemCol("movieId")
//      .setRatingCol("rating")
//    val model = als.fit(training)
//
//    // Evaluate the model by computing the RMSE on the test data
//    // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
//    model.setColdStartStrategy("drop")
//    val predictions = model.transform(test)
//
//    val evaluator = new RegressionEvaluator()
//      .setMetricName("rmse")
//      .setLabelCol("rating")
//      .setPredictionCol("prediction")
//    val rmse = evaluator.evaluate(predictions)
//    println(s"Root-mean-square error = $rmse")
//
//    // Generate top 10 movie recommendations for each user
//    val userRecs = model.recommendForAllUsers(10)
//    // Generate top 10 user recommendations for each movie
//    val movieRecs = model.recommendForAllItems(10)
//
//    // Generate top 10 movie recommendations for a specified set of users
//    val users = ratings.select(als.getUserCol).distinct().limit(3)
//    val userSubsetRecs = model.recommendForUserSubset(users, 10)
//    // Generate top 10 user recommendations for a specified set of movies
//    val movies = ratings.select(als.getItemCol).distinct().limit(3)
//    val movieSubSetRecs = model.recommendForItemSubset(movies, 10)
//
//
//







//    val indexer = new StringIndexer()
//      .setInputCol("sku")
//      .setOutputCol("categoryIndex")
//      .fit(data)
//
//    val indexed = indexer.transform(data)
//
//    println(indexed.show(10))
//









    //    println(df.show(5))
//    println("\n\n")
//
//      for (row <- df.take(2)){
//        println("Row"+row.getClass)
//        return
//      }



  }


}

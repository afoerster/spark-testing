package sparktest

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main extends App {
  val conf =
    new SparkConf()
      .setAppName("used from spark-submit")
  val sc = new SparkContext(conf)

  val rdd = sc.textFile("some file")
  SparkDriver.runPipeline(rdd)
}

object SparkDriver {

  def runPipeline(rdd: RDD[String]): RDD[Int] = {
    val ints = convertToInt(rdd)
    square(ints)
  }

  def convertToInt(rdd: RDD[String]): RDD[Int] = {
    rdd.map(_.toInt)
  }

  def square(rdd: RDD[Int]): RDD[Int] = {
    rdd.map(x => x * x)
  }

}

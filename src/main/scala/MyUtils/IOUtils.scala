package MyUtils

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SparkSession和SparkContext对象的IO工具类，用于获取相关对象及关闭资源
  */
object IOUtils {

  private val conf: SparkConf = new SparkConf()

  /**
    * 获取SparkContext的实例
    * @param appName:应用名称
    * @return SparkContext实例
    */
  def getSparkContext(appName:String):SparkContext={

    conf.setAppName(appName).setMaster("local[*]")
    val sparkContext: SparkContext = new SparkContext(conf)
    sparkContext
  }

  /**
    * 关闭SparkContext的实例，释放资源
    * @param sparkContext:SparkContext实例
    */
  def closeSparkContext(sparkContext: SparkContext):Unit={
    sparkContext.stop()
  }

  /**
    * 获取SparkSession的实例
    * @param appName:应用名称
    * @return SparkSession实例
    */
  def getSparkSession(appName:String):SparkSession={
    val sparkSession: SparkSession = SparkSession.builder().appName(appName).master("local[*]").getOrCreate()
    sparkSession
  }

  /**
    * 关闭SparkContext的实例，释放资源
    * @param sparkSession:SparkContext实例
    */
  def closeSparkSession(sparkSession: SparkSession)={
    sparkSession.stop()
  }
}

package IOUtils

import org.apache.spark.{SparkConf, SparkContext}

/**
  * SparkContext的初始化工具，用于获取和关闭sc
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
    val sc: SparkContext = new SparkContext(conf)
    sc
  }

  /**
    * 关闭SparkContext的实例，释放资源
    * @param sc:SparkContext实例
    */
  def closeSparkContext(sc: SparkContext):Unit={
    sc.stop()
  }
}

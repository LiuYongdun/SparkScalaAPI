package SparkSQL

import MyUtils.IOUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.JavaConverters._

object SparkSQLDemo2 {

  def main(args: Array[String]): Unit = {

    val sc = IOUtils.getSparkContext("SparkSQLTest")
    //创建DataFrame需要使用SQLContext（version1.X）
    val sqlContext = new SQLContext(sc)

    val lines: RDD[String] = sc.textFile("hdfs://localhost:9000/data/GameLog.txt")
    val gameUsersRDD: RDD[Row] = lines.map(each => {
      val tokens: Array[String] = each.split('|')
      val level: Int = tokens(0).toInt
      val lastLaunchTime: String = tokens(1)
      val ip: String = tokens(2)
      val userName: String = tokens(3)
      val roleType: String = tokens(4)
      val sex: String = tokens(5)

      Row(level,lastLaunchTime,ip,userName,roleType,sex)
    })

    //使用StructType的方法关联RDD和Schema
    val schema: StructType = new StructType(Array(
      StructField("level", IntegerType),
      StructField("lastLaunchTime", StringType),
      StructField("ip", StringType),
      StructField("userName", StringType),
      StructField("roleType", StringType),
      StructField("sex", StringType)
    ))
    val gameUsers: DataFrame = sqlContext.createDataFrame(gameUsersRDD,schema)

//    //使用SQL的API
//    gameUsers.registerTempTable("game_users")
//    val result: DataFrame = sqlContext.sql("select * from game_users")
//    result.show(10)

    //使用DataFrame的API
    import sqlContext.implicits._
    val result: DataFrame = gameUsers.select("level","ip","userName","sex").orderBy($"level" desc)
    result.show(10)

    IOUtils.closeSparkContext(sc)

  }
}


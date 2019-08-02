package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SparkSQLDemo3 {

  /**
    * 使用spark2.x的API
    */

  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder().appName("SparkSQLDemo3")
      .master("local[*]").getOrCreate()

    //创建RDD
    val lines: RDD[String] = session.sparkContext.textFile("hdfs://localhost:9000/data/GameLog.txt")
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

    val gameUsers: DataFrame = session.createDataFrame(gameUsersRDD,schema)
    gameUsers.show(10)

    session.stop()

  }

}

package SparkSQL
import IOUtils.IOUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

object SparkSQLDemo1 {

  def main(args: Array[String]): Unit = {

    val sc = IOUtils.getSparkContext("SparkSQLTest")
    //创建DataFrame需要使用SQLContext（version1.X）
    val sqlContext = new SQLContext(sc)

    val lines: RDD[String] = sc.textFile("hdfs://localhost:9000/data/GameLog.txt")
    val gameUsersRDD: RDD[GameUser] = lines.map(each => {
      val tokens: Array[String] = each.split('|')
      val level: Int = tokens(0).toInt
      val lastLaunchTime: String = tokens(1)
      val ip: String = tokens(2)
      val userName: String = tokens(3)
      val roleType: String = tokens(4)
      val sex: String = tokens(5)

      GameUser(level, lastLaunchTime, ip, userName, roleType, sex)
    })

    //该RDD装的是GameUser类型的数据，有了shcema信息，但是还是一个RDD，将RDD转换成DataFrame,导入隐式转换
    import sqlContext.implicits._
    val gameUsers: DataFrame = gameUsersRDD.toDF()

    //使用SQL的API
    gameUsers.registerTempTable("game_users")
    val result: DataFrame = sqlContext.sql("select * from game_users")

    result.show(10)

    IOUtils.closeSparkContext(sc)

  }
}

case class GameUser(level:Int,lastLaunchTime:String,ip:String,userName:String,roleType:String,sex:String)

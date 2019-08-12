package SparkSQL

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ColumnName, DataFrame, Dataset, Row, SparkSession}

import scala.util.control.Breaks._
/**
  * 使用broadcast join的方式优化join操作的效率
  */
object SparkSQLDemo5 {

  def main(args: Array[String]): Unit = {

    //获取SparkSession连接
    val spark: SparkSession = MyUtils.IOUtils.getSparkSession("SparkDemo5")
    import spark.implicits._

    //读取小表数据
    val genderLines: RDD[String] = spark.sparkContext.textFile("hdfs://localhost:9000/data/gender.txt")
    val genderRDD: RDD[(String, String)] = genderLines.map(line => {
      val splits: Array[String] = line.split(",")
      val engSex: String = splits(0)
      val cnSex: String = splits(1)
      (engSex, cnSex)
    })

    //将join操作两张表中较小的那张（即gender.txt的数据）广播出去
    val genderBroadcastRef: Broadcast[Array[(String, String)]] = spark.sparkContext.broadcast(genderRDD.collect())

    //读取大表数据
    val userLines: Dataset[String] = spark.read.textFile("hdfs://localhost:9000/data/user.txt")
    val userDF: DataFrame = userLines.map(line => {
      val splits: Array[String] = line.split(",")
      val id: String = splits(0)
      val name: String = splits(1)
      val sex: String = splits(2)
      (id, name, sex)
    }).toDF("id", "name", "sex")

    //使用SQL的API,使用自定义的函数，可以优化代码，使得数据比较的次数减少，提高效率（在此未体现）
    userDF.createTempView("v_user")
    spark.udf.register("find_cn_sex",(sex:String)=>{

      var result:String=null
      val genders: Array[(String, String)] = genderBroadcastRef.value

      for(i<-0 to genders.length-1)
      {
        if(sex.equals(genders(i)._1))
        {
          result=genders(i)._2
        }
      }

      result
    }
    )
    val resultDF: DataFrame = spark.sql("SELECT name,find_cn_sex(sex) FROM v_user")
    resultDF.show()

    MyUtils.IOUtils.closeSparkSession(spark)

  }


}

package SparkSQL

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQLDemo4 {

  def main(args: Array[String]): Unit = {

    //获取SparkSession对象，用于使用SparkSQL
    val spark = SparkSession
      .builder()
      .appName("SparkSQLDemo4")
      .master("local[*]")
      .getOrCreate()

    //获取用户信息
    val lines: RDD[String] = spark.sparkContext.textFile("hdfs://localhost:9000/data/user.txt")
    val usersRDD: RDD[Row] = lines.map(line => {

      val tokens: Array[String] = line.split(",")
      val id: String = tokens(0)
      val name: String = tokens(1)
      val sex: String = tokens(2)
      Row(id, name, sex)
    })

    val usersSchema: StructType = StructType(Array(
      StructField("id", StringType),
      StructField("name", StringType),
      StructField("sex", StringType)
    ))

    val usersDF: DataFrame = spark.createDataFrame(usersRDD,usersSchema)

    //构建gender的DataFrame
    import spark.implicits._
    val gendersDataset: Dataset[String] = spark.createDataset(List("male,男","female,女"))
    val genders: Dataset[(String, String)] = gendersDataset.map(each => {
      val splits: Array[String] = each.split(",")
      val engSex: String = splits(0)
      val cnSex: String = splits(1)
      (engSex, cnSex)
    })
    val gendersDF: DataFrame = genders.toDF("eng_sex","cn_sex")

    //对两个DataFrame对象进行join操作
    //1.使用SQL的API
    usersDF.createTempView("v_users")
    gendersDF.createTempView("v_genders")

    val result: DataFrame = spark.sql("select u.name,g.cn_sex from v_users u join v_genders g on u.sex=g.eng_sex")
    result.show()

    spark.stop()
    
  }

}

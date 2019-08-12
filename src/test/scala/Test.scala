import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Test
{
  def main(args : Array[String]) : Unit =
  {
    val str1 : String = "hello jack nice to meet jack"
    val str2: String = "hello rose nice to meet rose"

    import MyUtils.IOUtils

    val sparkContext : SparkContext = IOUtils.getSparkContext("test")
    val strRDD: RDD[String] = sparkContext.makeRDD(Array(str1,str2))
    val result1: RDD[Array[String]] = strRDD.map(_.split(","))
    val result2: RDD[String] = strRDD.flatMap(_.split(","))

    val array: Array[Array[String]] = result1.collect()
    val array1: Array[String] = result2.collect()


    IOUtils.closeSparkContext(sparkContext)

  }
}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object HelloWorld {

  def main(args: Array[String]) {


    val ints = new ListBuffer[Int]

    for(x<- 1 to 10){
      ints.+=(x)
    }

    println(ints.toList)


  }

}

package TopNInGroups

import IOUtils.IOUtils
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  *分组TopN练习，统计每门技术下最受欢迎的讲师
  */
object TopNInGroups {

  def main(args: Array[String]): Unit = {

    //获取sc
    val sc: SparkContext = IOUtils.getSparkContext("TopNInGroups")

    //读取文件
    val lines: RDD[String] = sc.textFile("hdfs://hadoop01:9000/files/lecturerAccess.log")

    /**
      * 将每行数据映射为((技术,讲师),1)的键值对
      */
    val records: RDD[((String, String), Int)] = lines.map(each => {
      val tokens: Array[String] = each.split("[./]")

      //field:技术领域  lecturer:讲师
      val field: String = tokens(2)
      val lecturer: String = tokens(5)

      ((field, lecturer), 1)
    })

    //获取所有的技术领域
    val fields:Array[String]= records.map(_._1._1).distinct().collect()

    //按照给定的分区器对数据进行分区后聚合
    val reduced: RDD[((String, String), Int)] = records.reduceByKey(new FieldParititioner(fields),(x:Int,y:Int)=>x+y)

    //对每个分区进行单独的排序，注意：”each=>each.toList.sortBy(_._2).reverse.iterator“
    //这句代码是在单独一个节点上面使用scala的排序，有可能会遇到内存溢出的问题
    val sorted: RDD[((String, String), Int)] = reduced.mapPartitions(each=>each.toList.sortBy(_._2).reverse.iterator)

    //保存最终结果
    sorted.saveAsTextFile("hdfs://hadoop01:9000/output/topNInGroups")

    IOUtils.closeSparkContext(sc)
  }
}

/**
  * 自定义按照技术领域划分的分区器
  *
  * @param fields：所有技术领域
  */
class FieldParititioner(fields:Array[String]) extends Partitioner {

  //给每门技术初始化分区编号
  val partitions=new mutable.HashMap[String,Int]()
  var partitionIndex=0
  for(field<-fields){
    partitions(field)=partitionIndex
    partitionIndex+=1
  }

  //获取分区数量
  override def numPartitions: Int = partitions.size

  //根据key获取分区的编号
  override def getPartition(key: Any): Int = {
    var field:String=key.asInstanceOf[(String,String)]._1
    partitions(field)
  }
}

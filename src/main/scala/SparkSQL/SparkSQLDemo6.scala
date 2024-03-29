package SparkSQL

import java.lang

import MyUtils.IOUtils
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession, types}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}

/**
  * 使用UDAF（用户自定义聚合函数）
  */
object SparkSQLDemo6
{
  def main(args : Array[String]) : Unit =
  {
    val spark: SparkSession = IOUtils.getSparkSession("SparkSQLDemo6")
    import spark.implicits._

    //生成数据
    val numberDF : DataFrame = spark.range(1, 11).toDF("value")
    numberDF.count()

    val gm : GeoMean = new GeoMean

    val result: DataFrame = numberDF.agg(gm($"value"))
    result.show()
  }
}

//自定义聚合函数
class GeoMean extends UserDefinedAggregateFunction {


  //输入数据的类型
  override def inputSchema: StructType = StructType(List(
    StructField("value", DoubleType)
  ))

  //产生中间结果的数据类型
  override def bufferSchema: StructType = StructType(List(
    //相乘之后返回的积
    StructField("product", DoubleType),
    //参与运算数字的个数
    StructField("counts", LongType)
  ))

  //最终返回的结果类型
  override def dataType: DataType = DoubleType


  //确保一致性 一般用true
  override def deterministic: Boolean = true

  //指定初始值
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //相乘的初始值
    buffer(0) = 1.0
    //参与运算数字的个数的初始值
    buffer(1) = 0L
  }

  //每有一条数据参与运算就更新一下中间结果(update相当于在每一个分区中的运算)
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //每有一个数字参与运算就进行相乘（包含中间结果）
    buffer(0) = buffer.getDouble(0) * input.getDouble(0)
    //参与运算数据的个数也有更新
    buffer(1) = buffer.getLong(1) + 1L
  }

  //全局聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //每个分区计算的结果进行相乘
    buffer1(0) =  buffer1.getDouble(0) * buffer2.getDouble(0)
    //每个分区参与预算的中间结果进行相加
    buffer1(1) =  buffer1.getLong(1) + buffer2.getLong(1)
  }

  //计算最终的结果
  override def evaluate(buffer: Row): Double = {
    math.pow(buffer.getDouble(0), 1.toDouble / buffer.getLong(1))
  }


}

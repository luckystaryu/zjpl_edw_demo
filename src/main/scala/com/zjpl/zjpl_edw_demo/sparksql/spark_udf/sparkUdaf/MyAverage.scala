package com.zjpl.zjpl_edw_demo.sparksql.spark_udf.sparkUdaf

import com.zjpl.zjpl_edw_demo.sparksql.Dao.{Average, Employee}
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

object MyAverage extends Aggregator[Employee,Average,Double]{
  def zero:Average = Average(0L,0L)

  def reduce(buffer: Average,employee: Employee):Average={
      buffer.sum += employee.salary
      buffer.count += 1
    buffer
  }
  def merge(b1:Average,b2:Average):Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  override def finish(reduction: Average): Double = reduction.sum.toDouble/reduction.count

  override def bufferEncoder: Encoder[Average] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

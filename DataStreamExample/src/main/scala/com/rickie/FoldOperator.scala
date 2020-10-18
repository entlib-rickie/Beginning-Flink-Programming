package com.rickie
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object FoldOperator {
  def main(args: Array[String]): Unit ={
    // set up the streaming execution environment
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setParallelism(1)
    // 设置数据源，处理数据，打印
    val result = senv.fromElements(("b", 3), ("a", 5), ("a", 7),("b", 4), ("a", 2))
      .keyBy(0) // 以数组的第一个元素作为key
      .fold(1000)((x,y)=>(x+y._2))
      .print()
    // 执行任务
    senv.execute()
  }
}

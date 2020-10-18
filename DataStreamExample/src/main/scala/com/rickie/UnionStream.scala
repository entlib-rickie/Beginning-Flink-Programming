package com.rickie
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object UnionStream {
  def main(args: Array[String]): Unit = {
    // 获取程序入口类
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 导入隐式转换包
    import org.apache.flink.api.scala._
    // first stream
    val firstStream = env.fromElements("hello flink world", "hello scala world")
    // second stream
    val secondStream = env.fromElements("hello elasticsearch", "flink vs spark")
    // 合并两个流
    val unionStream = firstStream.union(secondStream)
    // 打印合并之后的流
    unionStream.print().setParallelism(1)
    // 执行任务
    env.execute("Union stream job")
  }
}

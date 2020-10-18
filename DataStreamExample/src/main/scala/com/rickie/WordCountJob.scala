package com.rickie
import org.apache.flink.streaming.api.scala._
/**
 * Skeleton for a Flink Streaming Job.
 */
object WordCountJob {
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置数据源
    val source = env.fromElements("hello flink world", "hello scala world", "hello world")
    // 处理数据
    val result = source.flatMap(_.split("\\W+"))
        .map((_, 1))
        .keyBy(0)
        .sum(1)
    result.print().setParallelism(1)
    // execute program
    env.execute("Get data from collection")
  }
}

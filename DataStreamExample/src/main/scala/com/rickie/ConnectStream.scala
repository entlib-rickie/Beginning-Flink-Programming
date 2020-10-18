package com.rickie
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
/**
 * 连接两个不同类型的DataStream
 */
object ConnectStream {
  def main(args: Array[String]): Unit = {
    // 获取程序入口类
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 导入隐式转换包
    import org.apache.flink.api.scala._
    // first stream
    val firstStream = env.fromElements("hello flink world", "hello scala world")
    // second stream
    val secondStream = env.fromElements(1,3,5,7,9)
    // 合并两个流
    val connectStream = firstStream.connect(secondStream)
    val finalStream = connectStream.map(x=>x+"!!!", y=>y+100)
    // 打印合并之后的流
    finalStream.print().setParallelism(1)
    // 执行任务
    env.execute("Connect stream job")
  }
}

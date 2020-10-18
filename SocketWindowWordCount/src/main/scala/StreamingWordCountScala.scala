import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object StreamingWordCountScala {
  def main(args: Array[String]): Unit = {
    // 获取socket端口号
    val port:Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e:Exception =>{
        System.err.println("没有指定端口，使用默认端口：9000")
      }
        9000
    }
    // 设置socket连接主机或IP
    val HOST:String = "localhost"
    // 获取执行环境
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置socket数据源(get socket text stream)
    val source:DataStream[String]= env.socketTextStream(HOST, port, '\n')
    // 转化处理数据
    val wordsCount = source
      .flatMap(_.split("\\W+"))
      .map((_,1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
    //输出到目的端
    wordsCount.print().setParallelism(1)
    //wordsCount.writeAsText("data/scala-stream-result.txt", WriteMode.OVERWRITE).setParallelism(1)
    //执行操作
    env.execute("Flink Streaming Word Count by Scala")
  }
}

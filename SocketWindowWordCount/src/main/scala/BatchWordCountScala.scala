import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode

object BatchWordCountScala {
  def main(args: Array[String]): Unit = {
    val inputPath="data/input/test.txt"
    val outputPath="data/result.csv"
    // 获取执行环境
    val env=ExecutionEnvironment.getExecutionEnvironment
    // 获取resource目录
    val resource = ClassLoader.getSystemResource(inputPath).getPath
    // 设置数据源
    val text = env.readTextFile(resource)
    // 导入隐式转换的类
    import org.apache.flink.api.scala._
    // 转化处理数据
    val counts = text.flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_,1))
      .groupBy(0)
      .sum(1)
    // 单线程输出结果
    counts.writeAsCsv(outputPath,"\n","\t", WriteMode.OVERWRITE)
      .setParallelism(1)
    // 执行应用
    env.execute("Batch word count by Scala")
  }
}

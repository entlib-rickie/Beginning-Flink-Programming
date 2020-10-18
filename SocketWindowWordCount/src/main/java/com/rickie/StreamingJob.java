package com.rickie;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

// Skeleton for a Flink Streaming Job.
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// 获取需要的端口号
		int port;
		try{
			ParameterTool parameterTool=ParameterTool.fromArgs(args);
			port = parameterTool.getInt("port");
		} catch (Exception e) {
			System.err.println("没有port参数，使用默认端口号：9000");
			port = 9000;
		}
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		/*
		 * 开始创建Flink应用的执行计划
		 *
		 * 从环境中获取数据，如 env.readTextFile(textPath);
		 *
		 * 接着，使用operations转换DataStream<String>，如
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 */
		String hostname="localhost";
		String delimiter="\n";
		// 连接Socket获取输入的数据
		DataStreamSource<String> lines = env.socketTextStream(hostname, port, delimiter);
		// 使用Flink算子对输入流的文本进行操作
		// 按空格切词、计数、分组、设置时间窗口、聚合
		DataStream<Tuple2<String, Integer>> windowCounts = lines
				.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
					@Override
					public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
						for (String word : value.split("\\s+")) {
							out.collect(Tuple2.of(word, 1));
						}
					}
				})
				.keyBy(0)
				.timeWindow(Time.seconds(5))
				.sum(1);

		// 单线程打印结果
		windowCounts.print().setParallelism(1);

		// execute program
		env.execute("Socket Window WordCount");
	}
}

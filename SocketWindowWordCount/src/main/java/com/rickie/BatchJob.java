package com.rickie;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

// Skeleton for a Flink Batch Job.
public class BatchJob {
	public static void main(String[] args) throws Exception {
		String inputPath = "data/input/test.txt";
		String outputPath = "data/result.csv";
		// 获取运行环境
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		// 获取resource目录
		String resource = ClassLoader.getSystemResource(inputPath).getPath();
		// 获取文件中的内容
		DataSource<String> text = env.readTextFile(resource);
		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataSet<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 */
		// 转化处理数据
		DataSet<Tuple2<String, Integer>> counts =
				// 通过自定义函数，生成二元组: (word,1)
				text.flatMap(new Tokenizer())
				// 根据二元组的第“0”位分组，然后对第“1”位求和
				.groupBy(0)
				.sum(1);
		// 输出数据到本地文件系统
		counts.writeAsCsv(outputPath, "\n", "\t", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		// execute program
		env.execute("batch word count");
	}

	// 自定义函数
	private static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
			// normalize and split the line
			// \W: 非词字符  +: 1次或者多次
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for(String word:tokens) {
				collector.collect(new Tuple2<>(word, 1));
			}
		}
	}
}

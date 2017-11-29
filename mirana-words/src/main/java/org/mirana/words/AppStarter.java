package org.mirana.words;

import java.util.Calendar;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 从数据流接口接收单词，并统计单词的频次
 *
 */
public class AppStarter {
	
	private static final Logger logger = LoggerFactory.getLogger(AppStarter.class);
	
	public static void main(String[] args) throws Exception {
		logger.info("Started App!");
		if (args.length != 2){
			System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>");
			return;
		}

		String hostName = args[0];
		Integer port = Integer.parseInt(args[1]);

		// 获取数据
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 从数据端口获取数据
		DataStream<String> text = env.socketTextStream(hostName, port);

		DataStream<Tuple2<String, Integer>> counts = text.flatMap(new LineSplitter())
														 .filter(t -> {
															 String word = t.getField(0);
															 return word.length() > 2;
														 })
														 //.assignTimestampsAndWatermarks()
														 .keyBy(0)
														 .window(TumblingEventTimeWindows.of(Time.minutes(1)))
														 .sum(1);
		//	输出结果
		counts.writeAsText("/opt/word.out", WriteMode.NO_OVERWRITE).name("统计过程中");
		counts.addSink(tuple -> {
			logger.info("{} : {}", tuple.getField(0), tuple.getField(1));
		}).name("输出统计结果");
		// 确定任务名称
		env.execute("单词统计");
		logger.info("Started App!");
	}
	
	public static class MyWaterMark implements AssignerWithPeriodicWatermarks<Tuple2<String, Integer>> {

		/* (non-Javadoc)
		 * @see org.apache.flink.streaming.api.functions.TimestampAssigner#extractTimestamp(java.lang.Object, long)
		 */
		@Override
		public long extractTimestamp(Tuple2<String, Integer> item, long arg1) {
			return 0;
		}

		/* (non-Javadoc)
		 * @see org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks#getCurrentWatermark()
		 */
		@Override
		public Watermark getCurrentWatermark() {
			return new Watermark(Calendar.getInstance().getTimeInMillis());
		}
		
	}

	/**
	 * 将每行字符串分割为单词，以Tuple2进行存储，格式为(word, times)
	 */
	public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

		private static final long serialVersionUID = -7397350453434198035L;

		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// 分割单词
			String[] tokens = value.toLowerCase().split("\\W+");
			// 收集数据
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}
}

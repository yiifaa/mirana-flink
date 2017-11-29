package org.mirana.words;

import java.util.Calendar;

import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 从数据流接口接收单词，并统计单词的频次
 *
 */
public class App {
	
	private static final Logger logger = LoggerFactory.getLogger(App.class);
	
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

		DataStream<WordEvent> counts = text.flatMap(new WordSplitter())
														 .filter(t -> {
															 String word = t.getWord();
															 return word.length() > 2;
														 })
														 .assignTimestampsAndWatermarks(new WordWatermark())
														 .keyBy("word")
														 .window(TumblingEventTimeWindows.of(Time.minutes(1)))
														 .sum("counter");
		//	输出结果
		counts.writeAsText("/opt/word.out", WriteMode.OVERWRITE).name("统计过程中");
		counts.addSink(tuple -> {
			logger.info("{} : {} : {}", tuple.getWord(), tuple.getCounter(), Calendar.getInstance().getTimeInMillis());
		}).name("输出统计结果");
		// 确定任务名称
		env.execute("单词统计");
		logger.info("Started App!");
	}
	
}

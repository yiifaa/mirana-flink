package org.mirana.words;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 
 * 
 * @author  <a href="mailto:ganhuanxp@163.com">甘焕</a>
 * @version  1.0
 * 开发日期：2017年11月28日 ： 下午6:47:58 
 */
public class WordSplitter implements FlatMapFunction<String, WordEvent> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4750779068212374919L;

	/* (non-Javadoc)
	 * @see org.apache.flink.api.common.functions.FlatMapFunction#flatMap(java.lang.Object, org.apache.flink.util.Collector)
	 */
	@Override
	public void flatMap(String value, Collector<WordEvent> out) throws Exception {
		// 分割单词
		String[] tokens = value.toLowerCase().split("\\W+");
		// 收集数据
		for (String token : tokens) {
			if (token.length() > 0) {
				out.collect(new WordEvent(token, 1L));
			}
		}
	}

}

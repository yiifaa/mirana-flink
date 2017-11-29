package org.mirana.words;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * 
 * 
 * @author  <a href="mailto:ganhuanxp@163.com">甘焕</a>
 * @version  1.0
 * 开发日期：2017年11月28日 ： 下午6:52:55 
 */
public class WordWatermark implements AssignerWithPeriodicWatermarks<WordEvent> {
	
	private Long maxTimes = 0L;
	
	private static final Long tolerance = 1000L;

	/**
	 * 
	 */
	private static final long serialVersionUID = 2780570730727213688L;
	
	

	/* (non-Javadoc)
	 * @see org.apache.flink.streaming.api.functions.TimestampAssigner#extractTimestamp(java.lang.Object, long)
	 */
	@Override
	public long extractTimestamp(WordEvent element, long previousElementTimestamp) {
		Long times = element.getTimes();
		if(times > maxTimes) {
			maxTimes = times;
		}
		return times;
	}

	/* (non-Javadoc)
	 * @see org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks#getCurrentWatermark()
	 */
	@Override
	public Watermark getCurrentWatermark() {
		return new Watermark(maxTimes - tolerance);
	}

}

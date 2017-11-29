package org.mirana.words;

import java.util.Calendar;

/**
 * 
 * 
 * @author  <a href="mailto:ganhuanxp@163.com">甘焕</a>
 * @version  1.0
 * 开发日期：2017年11月28日 ： 下午6:46:12 
 */
public class WordEvent {
	
	private String word;
	
	private Long counter;
	
	private Long times;

	/**
	 * @return the word
	 */
	public String getWord() {
		return word;
	}

	/**
	 * @param word the word to set
	 */
	public void setWord(String word) {
		this.word = word;
	}

	/**
	 * @return the counter
	 */
	public Long getCounter() {
		return counter;
	}

	/**
	 * @param counter the counter to set
	 */
	public void setCounter(Long counter) {
		this.counter = counter;
	}

	/**
	 * @return the times
	 */
	public Long getTimes() {
		return times;
	}

	/**
	 * @param times the times to set
	 */
	public void setTimes(Long times) {
		this.times = times;
	}

	/**
	 * @param word
	 * @param counter
	 */
	public WordEvent(String word, Long counter) {
		super();
		this.word = word;
		this.counter = counter;
		this.times = Calendar.getInstance().getTimeInMillis();
	}

	/**
	 * 
	 */
	public WordEvent() {
		super();
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "WordEvent [word=" + word + ", counter=" + counter + ", times=" + times + "]";
	}

}

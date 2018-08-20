package com.nyc.taxi.duration;

import java.io.Serializable;

/***
 * 
 * @author Mohan MS
 *
 */
public class AverageTuple implements Serializable {
	private int count;
	private long average;

	public AverageTuple(int count, long average) {
		super();
		this.count = count;
		this.average = average;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public long getAverage() {
		return average;
	}

	public void setAverage(long average) {
		this.average = average;
	}
}
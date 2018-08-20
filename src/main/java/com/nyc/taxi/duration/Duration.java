package com.nyc.taxi.duration;

/***
 * 
 * @author Mohan MS
 *
 */

public class Duration {

	private int pickUpLocationId;
	private String pickTime;
	private String dropTime;
	private long duration;

	public long getDuration() {
		return duration;
	}

	public void setDuration(long duration) {
		this.duration = duration;
	}

	public int getPickUpLocationId() {
		return pickUpLocationId;
	}

	public void setPickUpLocationId(int pickUpLocationId) {
		this.pickUpLocationId = pickUpLocationId;
	}

	public String getPickTime() {
		return pickTime;
	}

	public void setPickTime(String pickTime) {
		this.pickTime = pickTime;
	}

	public String getDropTime() {
		return dropTime;
	}

	public void setDropTime(String dropTime) {
		this.dropTime = dropTime;
	}

}

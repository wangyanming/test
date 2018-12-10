package com.hds.cn.bi.vo;

public class TotalOrgVo {
	private String date;
	private String sellAmount;
	private int sellCnt;
	private int uv;
	private int successUserCnt;
	private int incrementCnt;
	
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public String getSellAmount() {
		return sellAmount;
	}
	public void setSellAmount(String sellAmount) {
		this.sellAmount = sellAmount;
	}
	public int getSellCnt() {
		return sellCnt;
	}
	public void setSellCnt(int sellCnt) {
		this.sellCnt = sellCnt;
	}
	public int getUv() {
		return uv;
	}
	public void setUv(int uv) {
		this.uv = uv;
	}
	public int getSuccessUserCnt() {
		return successUserCnt;
	}
	public void setSuccessUserCnt(int successUserCnt) {
		this.successUserCnt = successUserCnt;
	}
	public int getIncrementCnt() {
		return incrementCnt;
	}
	public void setIncrementCnt(int incrementCnt) {
		this.incrementCnt = incrementCnt;
	}
}

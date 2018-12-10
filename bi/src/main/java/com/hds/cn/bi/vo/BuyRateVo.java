package com.hds.cn.bi.vo;

public class BuyRateVo {
	private String date;
	private int viewDetailCnt;
	private int confirmOrderCnt;
	private int payOrderCnt;
	private int successPaylCnt;
	private String totalRate;
	private String confirmOrderRate;
	private String payOrderRate;
	private String successOrderRate;
	
	public String getTotalRate() {
		return totalRate;
	}
	public void setTotalRate(String totalRate) {
		this.totalRate = totalRate;
	}
	public int getViewDetailCnt() {
		return viewDetailCnt;
	}
	public void setViewDetailCnt(int viewDetailCnt) {
		this.viewDetailCnt = viewDetailCnt;
	}
	public int getConfirmOrderCnt() {
		return confirmOrderCnt;
	}
	public void setConfirmOrderCnt(int confirmOrderCnt) {
		this.confirmOrderCnt = confirmOrderCnt;
	}
	public int getPayOrderCnt() {
		return payOrderCnt;
	}
	public void setPayOrderCnt(int payOrderCnt) {
		this.payOrderCnt = payOrderCnt;
	}
	public int getSuccessPaylCnt() {
		return successPaylCnt;
	}
	public void setSuccessPaylCnt(int successPaylCnt) {
		this.successPaylCnt = successPaylCnt;
	}
	public String getConfirmOrderRate() {
		return confirmOrderRate;
	}
	public void setConfirmOrderRate(String confirmOrderRate) {
		this.confirmOrderRate = confirmOrderRate;
	}
	public String getPayOrderRate() {
		return payOrderRate;
	}
	public void setPayOrderRate(String payOrderRate) {
		this.payOrderRate = payOrderRate;
	}
	public String getSuccessOrderRate() {
		return successOrderRate;
	}
	public void setSuccessOrderRate(String successOrderRate) {
		this.successOrderRate = successOrderRate;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	@Override
	public String toString() {
		return date + "," + viewDetailCnt + "," + confirmOrderCnt + ","
				+ payOrderCnt + "," + successPaylCnt + "," + totalRate
				+ "," + confirmOrderRate + "," + payOrderRate + ","
				+ successOrderRate;
	}
}

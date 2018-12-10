package com.hds.cn.bi.vo;

public class TotalVo {
	//日期
	private String date;
	//uv
	private int uv;
	//pv
	private int pv;
	//销售金额
	private String orderAmount;
	//订单数
	private int orderCnt;
	//成单用户数
	private int userCnt;
	//注册用户数
	private int registCnt;
	//转化率=userCnt/uv
	private String conversionRate;
	//商品/产品数量
	private int productCnt;
	
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public int getUv() {
		return uv;
	}
	public void setUv(int uv) {
		this.uv = uv;
	}
	public int getPv() {
		return pv;
	}
	public void setPv(int pv) {
		this.pv = pv;
	}
	public void setOrderAmount(String orderAmount) {
		this.orderAmount = orderAmount;
	}
	public String getOrderAmount() {
		return orderAmount;
	}
	public int getOrderCnt() {
		return orderCnt;
	}
	public void setOrderCnt(int orderCnt) {
		this.orderCnt = orderCnt;
	}
	public int getUserCnt() {
		return userCnt;
	}
	public void setUserCnt(int userCnt) {
		this.userCnt = userCnt;
	}
	public int getRegistCnt() {
		return registCnt;
	}
	public void setRegistCnt(int registCnt) {
		this.registCnt = registCnt;
	}
	public String getConversionRate() {
		return conversionRate;
	}
	public void setConversionRate(String conversionRate) {
		this.conversionRate = conversionRate;
	}
	public int getProductCnt() {
		return productCnt;
	}
	public void setProductCnt(int productCnt) {
		this.productCnt = productCnt;
	}
	@Override
	public String toString() {
		return date + "," + uv + "," + pv + "," + orderAmount + "," + orderCnt + "," + userCnt + "," + registCnt + "," + conversionRate + "," + productCnt;
	}
}

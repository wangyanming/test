package com.hds.cn.bi.vo;

/**
 * 
 * @author wangyanming
 *
 */
public class ClientVo {
	//渠道ID
	private int client_id;
	// uv
	private int pv;
	// pv
	private int uv;
	// 订单数量
	private int orderCnt;
	// 订单金额
	private String orderAmount;
	// 转化率
	private String conversionRate;
	//商品/产品数量
	private int productCnt;
	
	public int getClient_id() {
		return client_id;
	}
	public void setClient_id(int client_id) {
		this.client_id = client_id;
	}
	public int getPv() {
		return pv;
	}
	public void setPv(int pv) {
		this.pv = pv;
	}
	public int getUv() {
		return uv;
	}
	public void setUv(int uv) {
		this.uv = uv;
	}
	public int getOrderCnt() {
		return orderCnt;
	}
	public void setOrderCnt(int orderCnt) {
		this.orderCnt = orderCnt;
	}
	public String getOrderAmount() {
		return orderAmount;
	}
	public void setOrderAmount(String orderAmount) {
		this.orderAmount = orderAmount;
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
		return client_id + "," + pv + "," + uv + "," + orderCnt
				+ "," + orderAmount + "," + conversionRate + "," + productCnt;
	}
}

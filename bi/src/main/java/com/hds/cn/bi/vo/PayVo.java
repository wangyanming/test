package com.hds.cn.bi.vo;

/**
 * 
 * @author wangyanming
 *
 */
public class PayVo {
	//支付方式
	private String pay_way;
	//销售金额
	private String orderAmount;
	//转化率(支付成功人数/点击支付请求人数)
	private String conversionRate;
	//产品/商品销售数量
	private int productCnt;
	
	public String getPay_way() {
		return pay_way;
	}
	public void setPay_way(String pay_way) {
		this.pay_way = pay_way;
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
		return pay_way + "," + orderAmount + "," + conversionRate + "," + productCnt;
	}
}

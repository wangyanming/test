package com.hds.cn.bi.vo;

/**
 * 
 * @author wangyanming
 *
 */
public class RegionVo {
	//地区Id
	private int regionId;
	//UV
	private int uv;
	//PV
	private int pv;
	//销售金额
	private String orderAmount;
	//订单数
	private int orderCnt;
	//转化率
	private String conversionRate;
	//产品/商品销售数量
	private int productCnt;
	
	public int getRegionId() {
		return regionId;
	}
	public void setRegionId(int regionId) {
		this.regionId = regionId;
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
	public String getOrderAmount() {
		return orderAmount;
	}
	public void setOrderAmount(String orderAmount) {
		this.orderAmount = orderAmount;
	}
	public int getOrderCnt() {
		return orderCnt;
	}
	public void setOrderCnt(int orderCnt) {
		this.orderCnt = orderCnt;
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
		return regionId + "," + uv + "," + pv + "," + orderAmount
				+ "," + orderCnt + "," + conversionRate + "," + productCnt;
	}
}

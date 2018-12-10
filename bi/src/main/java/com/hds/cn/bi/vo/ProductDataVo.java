package com.hds.cn.bi.vo;

public class ProductDataVo {
	private int orgId;
	private int agentId;
	private String productType;
	private int productId;
	private String productRelId;
	private String productSubId;
	private String orderAmount;
	private int productCnt;
	
	public int getProductId() {
		return productId;
	}
	public void setProductId(int productId) {
		this.productId = productId;
	}
	public int getOrgId() {
		return orgId;
	}
	public void setOrgId(int orgId) {
		this.orgId = orgId;
	}
	public int getAgentId() {
		return agentId;
	}
	public void setAgentId(int agentId) {
		this.agentId = agentId;
	}
	public String getProductType() {
		return productType;
	}
	public void setProductType(String productType) {
		this.productType = productType;
	}
	public String getProductRelId() {
		return productRelId;
	}
	public void setProductRelId(String productRelId) {
		this.productRelId = productRelId;
	}
	public String getProductSubId() {
		return productSubId;
	}
	public void setProductSubId(String productSubId) {
		this.productSubId = productSubId;
	}
	public String getOrderAmount() {
		return orderAmount;
	}
	public void setOrderAmount(String orderAmount) {
		this.orderAmount = orderAmount;
	}
	public int getProductCnt() {
		return productCnt;
	}
	public void setProductCnt(int productCnt) {
		this.productCnt = productCnt;
	}
}

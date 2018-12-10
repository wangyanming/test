package com.hds.cn.bi.util;

import org.elasticsearch.action.search.SearchResponse;

public class IndexUtil {
	
	/**
	 * 初始化logon
	 * @return
	 */
	public static SearchResponse logonSr() {
		SearchResponse logonSr = EsClient.getConnect().prepareSearch(CommonConstant.LOGON).get();
		return logonSr;
	}
	
	/**
	 * 初始化order
	 * @return
	 */
	public static SearchResponse orderSr() {
		SearchResponse orderSr = EsClient.getConnect().prepareSearch(CommonConstant.ORDER).get();
		return orderSr;
	}
	
	/**
	 * 初始化regist
	 * @return
	 */
	public static SearchResponse registSr() {
		SearchResponse registSr = EsClient.getConnect().prepareSearch(CommonConstant.REGIST).get();
		return registSr;
	}
}

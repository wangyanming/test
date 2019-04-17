package com.hds.hdyapp.util;

import java.util.HashMap;
import java.util.Map;

import net.sf.json.JSONObject;

public class RequestUtil {
	public static Map<String, Object> requestUtil(JSONObject json) {
		Map<String, Object> requestMap = new HashMap<String, Object>();
		
		requestMap.put("startDate", json.getString("startDate"));
		requestMap.put("endDate", DateUtil.dateAdd(json.getString("endDate")));
		requestMap.put("orgId", json.getInt("orgId"));
		//畅销排行销售金额、销售数量
		if (null != json.get("sortType")) {
			requestMap.put("sortType", json.get("sortType"));
		}
		if (null != json.get("size")) {
			requestMap.put("size", json.get("size"));
		}
		//产品ID，产品类型
		if (null != json.get("productId")) {
			requestMap.put("productId", json.get("productId"));
			requestMap.put("productType", json.get("productType"));
		}
		//渠道ID
		if (null != json.get("channelId")) {
			requestMap.put("channelId", json.get("channelId"));
		}
		//省份ID
		if (null != json.get("regionId")) {
			requestMap.put("regionId", json.get("regionId"));
		}
		//终端ID
		if (null != json.get("clientId")) {
			requestMap.put("clientId", json.get("clientId"));
		}
		//日、周、月、年、自定义
		if (null != json.get("searchType")) {
			requestMap.put("searchType", json.get("searchType"));
		}
		return requestMap;
	}
}

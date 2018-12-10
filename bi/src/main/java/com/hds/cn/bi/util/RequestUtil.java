package com.hds.cn.bi.util;

import java.util.HashMap;
import java.util.Map;

import net.sf.json.JSONObject;

public class RequestUtil {
	public static Map<String, Object> requestUtil(JSONObject json) {
		Map<String, Object> requestMap = new HashMap<String, Object>();
		
		if (null != json.get("productIds")) {
			Object[] productIds = json.getJSONArray("productIds").toArray();
			requestMap.put("productIds", productIds);
		}
		if (null != json.get("agentIds")) {
			Object[] productIds = json.getJSONArray("agentIds").toArray();
			requestMap.put("agentIds", productIds);
		}
		if (null != json.get("agentId")) {
			requestMap.put("agentId", json.get("agentId"));
		}
		if (null != json.get("startDate")) {
			requestMap.put("startDate", json.getString("startDate"));
		}
		if (null != json.get("endDate")) {
			requestMap.put("endDate", DateUtil.dateAdd(json.getString("endDate")));
		}
		if (null != json.get("orgId")) {
			requestMap.put("orgId", json.getInt("orgId"));
		}
		if(null != json.get("productId")) {
			requestMap.put("productId", json.get("productId"));
		}
		if (null != json.get("productType")) {
			requestMap.put("productType", json.get("productType"));
		}
		if(null != json.get("regionType")) {
			requestMap.put("regionType", json.get("regionType"));
		}
		requestMap.put("size", json.get("size"));
		return requestMap;
	}
	
	public static Map<String, Object> requestHourUtil(JSONObject json) {
		String type = "";
		Map<String, Object> requestMap = new HashMap<String, Object>();
		//开始时间
		String sDate = json.getString("startDate");
		//结束时间
		String eDate = json.getString("endDate");
		
		if(sDate.equals(eDate)) {
			type = "hour";
		} else {
			type = "day";
		}
		requestMap.put("startDate", json.getString("startDate"));
		requestMap.put("endDate", DateUtil.dateAdd(json.getString("endDate")));
		requestMap.put("orgId", json.getInt("orgId"));
		if(null != json.get("productId")) {
			requestMap.put("productId", json.get("productId"));
			requestMap.put("productType", json.get("productType"));
		}
		if(null != json.get("regionType")) {
			requestMap.put("regionType", json.get("regionType"));
		}
		requestMap.put("size", json.get("size"));
		requestMap.put("type", type);
		return requestMap;
	}
	
	public static Map<String, Object> requestRegionUtil(JSONObject json) {
		Map<String, Object> requestMap = new HashMap<String, Object>();
		
		requestMap.put("startDate", json.getString("startDate"));
		requestMap.put("endDate", DateUtil.dateAdd(json.getString("endDate")));
		requestMap.put("orgId", json.getInt("orgId"));
		if(null != json.get("productId")) {
			requestMap.put("productId", json.get("productId"));
			requestMap.put("productType", json.get("productType"));
		}
		requestMap.put("regionType", json.get("regionType"));
		if(null != json.get("size") && !"".equals(json.get("size").toString())) {
			requestMap.put("size", json.get("size"));
		}
		return requestMap;
	}
	
	public static Map<String, Object> requesBuytUtil(JSONObject json) {
		Map<String, Object> requestMap = new HashMap<String, Object>();
		
		requestMap.put("startDate", json.getString("startDate"));
		requestMap.put("endDate", DateUtil.dateAdd(json.getString("endDate")));
		requestMap.put("orgId", json.getInt("orgId"));
		if(null != json.get("productId")) {
			requestMap.put("productId", json.get("productId"));
			requestMap.put("productType", json.get("productType"));
		}
		requestMap.put("size", json.get("size"));
		requestMap.put("agentType", json.get("agentType"));
		return requestMap;
	}
	
	public static Map<String, Object> requesHourtUtil(JSONObject json) {
		Map<String, Object> requestMap = new HashMap<String, Object>();
		//开始时间
		String sDate = json.getString("startDate");
		//结束时间
		String eDate = json.getString("endDate");
		
		if(sDate.equals(eDate)) {
			requestMap.put("type", "hour");
		} else {
			requestMap.put("type", "day");
		}
		requestMap.put("startDate", json.getString("startDate"));
		requestMap.put("endDate", DateUtil.dateAdd(json.getString("endDate")));
		if(null != json.get("orgId")) {
			requestMap.put("orgId", json.getInt("orgId"));
		}
		if(null != json.get("dateType")) {
			requestMap.put("dateType", json.get("dateType"));
		}
		if(null != json.get("productId")) {
			requestMap.put("productId", json.get("productId"));
			requestMap.put("productType", json.get("productType"));
		}
		return requestMap;
	}
	
	public static Map<String, Object> requestUserUtil(JSONObject json) {
		Map<String, Object> requestMap = new HashMap<String, Object>();
		
		requestMap.put("startDate", json.getString("startDate"));
		requestMap.put("endDate", json.getString("endDate"));
		requestMap.put("orgId", json.getInt("orgId"));
		if(null != json.get("productId")) {
			requestMap.put("productId", json.get("productId"));
			requestMap.put("productType", json.get("productType"));
		}
		requestMap.put("size", json.get("size"));
		return requestMap;
	}
}

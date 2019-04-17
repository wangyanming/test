package com.hds.cn.bi.util;

import java.util.Map;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;

/**
 * 
 * @author wangyanming
 *
 */
public class EsUtil {
	
	/**
	 * logon query
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder logonBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("visit_time.keyword").gte(requestMap.get("startDate")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		return bqb;
	}
	
	/**
	 * regist query
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder signBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("sign_time.keyword").gte(requestMap.get("startDate")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		bqb.must(QueryBuilders.matchQuery("event_type", "signUp")); //组织ID
		return bqb;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder signTotalBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		bqb.must(QueryBuilders.matchQuery("event_type", "signUp")); //组织ID
		return bqb;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder eventUserBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("create_date").gte(requestMap.get("startDate")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		return bqb;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder signTodayBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("sign_time.keyword").gte(requestMap.get("today")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		bqb.must(QueryBuilders.matchQuery("event_type", "signUp")); //组织ID
		return bqb;
	}
	
	/**
	 * order query
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder orderBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("end_time.keyword").gte(requestMap.get("startDate")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		if(null != requestMap.get("productId") && -1 != Integer.valueOf(requestMap.get("productId").toString())) {
			bqb.must(QueryBuilders.matchQuery("product_type", requestMap.get("productType")));//活动类型
			bqb.must(QueryBuilders.matchQuery("product_id", requestMap.get("productId"))); //活动ID
		}
		bqb.must(QueryBuilders.matchQuery("event_type", "paidOrder")); //组织ID
		return bqb;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder orderTotalBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		bqb.must(QueryBuilders.matchQuery("event_type", "paidOrder")); //组织ID
		return bqb;
	}
	
	/**
	 * orderMultiBqb
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder orderMultiBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		if (null != requestMap.get("orgId")) {
			bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		}
		if(null != requestMap.get("productIds")) {
			bqb.must(QueryBuilders.matchQuery("product_type", requestMap.get("productType")));//活动类型
			Object[] productIds = (Object[]) requestMap.get("productIds");
			bqb.must(QueryBuilders.termsQuery("product_id", productIds));
		}
		bqb.must(QueryBuilders.matchQuery("event_type", "paidOrder")); //组织ID
		return bqb;
	}
	
	/**
	 * order query
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder orderViewBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("start_time.keyword").gte(requestMap.get("startDate")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		if(null != requestMap.get("productId") && -1 != Integer.valueOf(requestMap.get("productId").toString())) {
			bqb.must(QueryBuilders.matchQuery("product_type", requestMap.get("productType")));//活动类型
			bqb.must(QueryBuilders.matchQuery("product_id", requestMap.get("productId"))); //活动ID
		}
		bqb.must(QueryBuilders.matchQuery("event_type", "ViewProduct")); //组织ID
		return bqb;
	}
	
	/**
	 * multiViewBqb
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder multiViewBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		if (null != requestMap.get("orgId")) {
			bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		}
		bqb.must(QueryBuilders.matchQuery("event_type", "ViewProduct")); //组织ID
		if(null != requestMap.get("productIds")) {
			bqb.must(QueryBuilders.matchQuery("product_type", requestMap.get("productType")));//活动类型
			Object[] productIds = (Object[]) requestMap.get("productIds");
			bqb.must(QueryBuilders.termsQuery("product_id", productIds));
		}
		return bqb;
	}
	
	/**
	 * order query
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder orderBqbLogon(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("end_time.keyword").lt(requestMap.get("startDate"))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		if(null != requestMap.get("productId") && -1 != Integer.valueOf(requestMap.get("productId").toString())) {
			bqb.must(QueryBuilders.matchQuery("product_type", requestMap.get("productType")));//活动类型
			bqb.must(QueryBuilders.matchQuery("product_id", requestMap.get("productId"))); //活动ID
		}
		bqb.must(QueryBuilders.matchQuery("event_type", "paidOrder")); //组织ID
		return bqb;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder orderBqbPay(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("start_time.keyword").gte(requestMap.get("startDate")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		if(null != requestMap.get("productId") && -1 != Integer.valueOf(requestMap.get("productId").toString())) {
			bqb.must(QueryBuilders.matchQuery("product_type", requestMap.get("productType")));//活动类型
			bqb.must(QueryBuilders.matchQuery("product_id", requestMap.get("productId"))); //活动ID
		}
		bqb.must(QueryBuilders.matchQuery("event_type", "payorder")); //组织ID
		return bqb;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder orderBqbBuyRate(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("start_time.keyword").gte(requestMap.get("startDate")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		if(2 == Integer.valueOf(requestMap.get("agentType").toString())) {
			bqb.must(QueryBuilders.matchQuery("agent_type", "1"));
		} else if(3 == Integer.valueOf(requestMap.get("agentType").toString())) {
			bqb.mustNot(QueryBuilders.matchQuery("agent_type", "1"));
		}
		if(null != requestMap.get("productId") && -1 != Integer.valueOf(requestMap.get("productId").toString())) {
			bqb.must(QueryBuilders.matchQuery("product_type", requestMap.get("productType")));//活动类型
			bqb.must(QueryBuilders.matchQuery("product_id", requestMap.get("productId"))); //活动ID
		}
		return bqb;
	}
	
	public static BoolQueryBuilder orderBqbTodayBuyRate(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("start_time.keyword").gte(requestMap.get("today")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		if(2 == Integer.valueOf(requestMap.get("agentType").toString())) {
			bqb.must(QueryBuilders.matchQuery("agent_type", "1"));
		} else if(3 == Integer.valueOf(requestMap.get("agentType").toString())) {
			bqb.mustNot(QueryBuilders.matchQuery("agent_type", "1"));
		}
		if(null != requestMap.get("productId") && -1 != Integer.valueOf(requestMap.get("productId").toString())) {
			bqb.must(QueryBuilders.matchQuery("product_type", requestMap.get("productType")));//活动类型
			bqb.must(QueryBuilders.matchQuery("product_id", requestMap.get("productId"))); //活动ID
		}
		
		return bqb;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder orderBqbDayBuyRate(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("stat_time.keyword").gte(requestMap.get("startDate")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		bqb.must(QueryBuilders.matchQuery("device_type", requestMap.get("agentType")));
		bqb.must(QueryBuilders.matchQuery("product_type", requestMap.get("productType")));//活动类型
		bqb.must(QueryBuilders.matchQuery("product_id", requestMap.get("productId"))); //活动ID
		return bqb;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder orderHourBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("stat_time.keyword").gte(requestMap.get("startDate")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		bqb.must(QueryBuilders.matchQuery("product_id", requestMap.get("productId"))); //活动ID
		bqb.must(QueryBuilders.matchQuery("product_type", requestMap.get("productType")));//活动类型
		return bqb;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder orderAgentHourBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("stat_time.keyword").gte(requestMap.get("startDate")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		bqb.must(QueryBuilders.matchQuery("agent_id", requestMap.get("agentId"))); //组织ID
		bqb.must(QueryBuilders.matchQuery("type", 5)); //组织ID
		return bqb;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder orderAgentRegionBqb1(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("stat_time.keyword").gte(requestMap.get("startDate")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		bqb.must(QueryBuilders.matchQuery("agent_id", requestMap.get("agentId"))); //组织ID
		bqb.must(QueryBuilders.matchQuery("type", 3)); //组织ID
		return bqb;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder orderAgentFromBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("stat_time.keyword").gte(requestMap.get("startDate")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		bqb.must(QueryBuilders.matchQuery("agent_id", requestMap.get("agentId"))); //组织ID
		bqb.must(QueryBuilders.matchQuery("type", 1)); //组织ID
		return bqb;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder orderAgentclientBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("stat_time.keyword").gte(requestMap.get("startDate")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		bqb.must(QueryBuilders.matchQuery("agent_id", requestMap.get("agentId"))); //组织ID
		bqb.must(QueryBuilders.matchQuery("type", 2)); //组织ID
		return bqb;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder orderUserHourBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("stat_time.keyword").gte(requestMap.get("startDate")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		bqb.must(QueryBuilders.matchQuery("product_id", "-1")); 
		bqb.must(QueryBuilders.matchQuery("product_type", "all")); 
		return bqb;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder logonTodayBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("visit_time.keyword").gte(requestMap.get("today")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		return bqb;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder orderTodayBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("end_time.keyword").gte(requestMap.get("today")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		if(null != requestMap.get("productId") && -1 != Integer.valueOf(requestMap.get("productId").toString())) {
			bqb.must(QueryBuilders.matchQuery("product_type", requestMap.get("productType")));//活动类型
			bqb.must(QueryBuilders.matchQuery("product_id", requestMap.get("productId"))); //活动ID
		}
		bqb.must(QueryBuilders.matchQuery("event_type", "PaidOrder"));
		return bqb;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder orderAgentTodayBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("end_time.keyword").gte(requestMap.get("today")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		bqb.must(QueryBuilders.matchQuery("agent_id", requestMap.get("agentId"))); //组织ID
		bqb.must(QueryBuilders.matchQuery("event_type", "PaidOrder"));
		return bqb;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder orderTodayViewBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("start_time.keyword").gte(requestMap.get("today")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		if(null != requestMap.get("productId") && -1 != Integer.valueOf(requestMap.get("productId").toString())) {
			bqb.must(QueryBuilders.matchQuery("product_type", requestMap.get("productType")));//活动类型
			bqb.must(QueryBuilders.matchQuery("product_id", requestMap.get("productId"))); //活动ID
		}
		bqb.must(QueryBuilders.matchQuery("event_type", "ViewProduct"));
		return bqb;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder orderAgentTodayViewBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("start_time.keyword").gte(requestMap.get("today")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		bqb.must(QueryBuilders.matchQuery("agent_id", requestMap.get("agentId"))); //组织ID
		bqb.must(QueryBuilders.matchQuery("event_type", "ViewProduct"));
		return bqb;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder orderUserTodayBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("end_time.keyword").gte(requestMap.get("today")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		bqb.must(QueryBuilders.matchQuery("event_type", "paidorder"));
		return bqb;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static AggregationBuilder totalOrderAb(Map<String, Object> requestMap) {
		AggregationBuilder ab = AggregationBuilders.count("orderCnt").field("order_id");
		ab.subAggregation(AggregationBuilders.sum("orderAmount").field("order_amount"))
		.subAggregation(AggregationBuilders.cardinality("userCnt").field("user_id"))
		.subAggregation(AggregationBuilders.sum("productCnt").field("product_num"));
		if(null != requestMap.get("productId") && -1 != Integer.valueOf(requestMap.get("productId").toString())) {
			ab.subAggregation(AggregationBuilders.cardinality("uv").field("user_id"))
			.subAggregation(AggregationBuilders.count("pv").field("user_id"));
		}
		return ab;
	}
	
	/**
	 * 
	 * order AggregationBuilder
	 * @param requestMap
	 * @return
	 */
	public static AggregationBuilder orderAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		if(-1 == Integer.valueOf(requestMap.get("size").toString())) {
			ab = AggregationBuilders.terms("channelCnt").field("channel_id").order(BucketOrder.aggregation("orderAmount", false)).size(5);
		} else {
			ab = AggregationBuilders.terms("channelCnt").field("channel_id").order(BucketOrder.aggregation("orderAmount", false)).size(1000);//求无限大方法
		}
		ab.subAggregation(AggregationBuilders.cardinality("orderCnt").field("batch_id"))
		.subAggregation(AggregationBuilders.sum("orderAmount").field("order_amount"))
		.subAggregation(AggregationBuilders.cardinality("userCnt").field("user_id"))
		.subAggregation(AggregationBuilders.sum("productCnt").field("product_num"));
		return ab;
	}
	
	public static AggregationBuilder orderMultiAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		ab = AggregationBuilders.terms("product").field("product_id").order(BucketOrder.aggregation("orderAmount", false)).size(1000);//求无限大方法
		ab.subAggregation(AggregationBuilders.cardinality("orderCnt").field("batch_id"))
		.subAggregation(AggregationBuilders.sum("orderAmount").field("order_amount"))
		.subAggregation(AggregationBuilders.cardinality("userCnt").field("user_id"))
		.subAggregation(AggregationBuilders.sum("productCnt").field("product_num"));
		return ab;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static AggregationBuilder orderBatchAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		ab = AggregationBuilders.terms("batch_id").field("batch_id").size(10000);//求无限大方法
		ab.subAggregation(AggregationBuilders.terms("channelCnt").field("channel_id"))
		.subAggregation(AggregationBuilders.sum("freightAmount").field("freight_money"));
		return ab;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static AggregationBuilder orderFreightAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		ab = AggregationBuilders.terms("batch_id").field("batch_id").size(10000);//求无限大方法
		ab.subAggregation(AggregationBuilders.terms("freight").field("freight_money"))
		.subAggregation(AggregationBuilders.sum("freightAmount").field("freight_money"));
		return ab;
	}
	
	/**
	 * multiBatchAb
	 * @param requestMap
	 * @return
	 */
	public static AggregationBuilder multiBatchAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		ab = AggregationBuilders.terms("batch_id").field("batch_id").size(10000);//求无限大方法
		ab.subAggregation(AggregationBuilders.terms("product").field("product_id"))
		.subAggregation(AggregationBuilders.sum("freightAmount").field("freight_money"));
		return ab;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static AggregationBuilder orderPageViewAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		ab = AggregationBuilders.terms("channelCnt").field("channel_id").order(BucketOrder.aggregation("uv", false)).size(1000);//求无限大方法
		ab.subAggregation(AggregationBuilders.count("pv").field("user_id"))
		.subAggregation(AggregationBuilders.cardinality("uv").field("user_id"));
		return ab;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static AggregationBuilder multiViewAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		ab = AggregationBuilders.terms("product").field("product_id").order(BucketOrder.aggregation("uv", false)).size(1000);//求无限大方法
		ab.subAggregation(AggregationBuilders.count("pv").field("user_id"))
		.subAggregation(AggregationBuilders.cardinality("uv").field("user_id"));
		return ab;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static AggregationBuilder orderClientAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		if(-1 == Integer.valueOf(requestMap.get("size").toString())) {
			ab = AggregationBuilders.terms("clientCnt").field("agent_type").order(BucketOrder.aggregation("orderAmount", false)).size(5);
		} else {
			ab = AggregationBuilders.terms("clientCnt").field("agent_type").order(BucketOrder.aggregation("orderAmount", false)).size(1000);//求无限大方法
		}
		ab.subAggregation(AggregationBuilders.cardinality("orderCnt").field("batch_id"))
		.subAggregation(AggregationBuilders.sum("orderAmount").field("order_amount"))
		.subAggregation(AggregationBuilders.cardinality("userCnt").field("user_id"))
		.subAggregation(AggregationBuilders.sum("productCnt").field("product_num"));
		return ab;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static AggregationBuilder orderClientBatchAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		ab = AggregationBuilders.terms("batch_id").field("batch_id").size(10000);//求无限大方法
		ab.subAggregation(AggregationBuilders.terms("clientCnt").field("agent_type"));
		return ab;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static AggregationBuilder orderClientViewAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		ab = AggregationBuilders.terms("clientCnt").field("agent_type").size(10000);//求无限大方法
		ab.subAggregation(AggregationBuilders.count("pv").field("user_id"))
		.subAggregation(AggregationBuilders.cardinality("uv").field("user_id"));
		return ab;
	}
	
	/**
	 *
	 * @param requestMap
	 * @return
	 */
	public static AggregationBuilder orderPaidAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		if(-1 == Integer.valueOf(requestMap.get("size").toString())) {
			ab = AggregationBuilders.terms("payType").field("pay_way").order(BucketOrder.aggregation("orderAmount", false)).size(5);
		} else {
			ab = AggregationBuilders.terms("payType").field("pay_way").order(BucketOrder.aggregation("orderAmount", false)).size(1000);//求无限大方法
		}
		ab.subAggregation(AggregationBuilders.sum("orderAmount").field("order_amount"))
		.subAggregation(AggregationBuilders.cardinality("userCnt").field("user_id"))
		.subAggregation(AggregationBuilders.sum("productCnt").field("product_num"));
		return ab;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static AggregationBuilder orderPaidBatchAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		ab = AggregationBuilders.terms("batch_id").field("batch_id").size(10000);//求无限大方法
		ab.subAggregation(AggregationBuilders.terms("payType").field("pay_way"));
		return ab;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static AggregationBuilder regionLogonAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		if(1 == Integer.valueOf(requestMap.get("regionType").toString())) {
			ab = AggregationBuilders.terms("region").field("province_id").size(5000);
		} else {
			ab = AggregationBuilders.terms("region").field("city_id").size(5000);
		}
		ab.subAggregation(AggregationBuilders.cardinality("uv").field("user_id"));
		return ab;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static AggregationBuilder regionOrderAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		if(1 == Integer.valueOf(requestMap.get("regionType").toString())) {
			ab = AggregationBuilders.terms("region").field("province_id").size(5000);
		} else {
			ab = AggregationBuilders.terms("region").field("city_id").size(5000);
		}
		ab.subAggregation(AggregationBuilders.cardinality("orderCnt").field("batch_id"))
		.subAggregation(AggregationBuilders.sum("orderAmount").field("order_amount"))
		.subAggregation(AggregationBuilders.cardinality("userCnt").field("user_id"))
		.subAggregation(AggregationBuilders.sum("productCnt").field("product_num"));
		return ab;
	}
	
	public static AggregationBuilder orderAgentRegionAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		ab = AggregationBuilders.terms("region").field("province_id").size(5000);
		ab.subAggregation(AggregationBuilders.cardinality("orderCnt").field("batch_id"))
		.subAggregation(AggregationBuilders.sum("orderAmount").field("order_amount"))
		.subAggregation(AggregationBuilders.cardinality("userCnt").field("user_id"))
		.subAggregation(AggregationBuilders.sum("productCnt").field("product_num"));
		return ab;
	}
	
	public static AggregationBuilder orderAgentRegionAb1(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		ab = AggregationBuilders.terms("region").field("province_id").size(5000);
		ab.subAggregation(AggregationBuilders.sum("orderCnt").field("order_count"))
		.subAggregation(AggregationBuilders.sum("orderAmount").field("order_amount"))
		.subAggregation(AggregationBuilders.sum("userCnt").field("success_user"))
		.subAggregation(AggregationBuilders.sum("productCnt").field("product_number"));
		return ab;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static AggregationBuilder orderRegionBatchAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		ab = AggregationBuilders.terms("batch_id").field("batch_id").size(10000);
		if(1 == Integer.valueOf(requestMap.get("regionType").toString())) {
			ab.subAggregation(AggregationBuilders.terms("region").field("province_id").size(10000));
		} else {
			ab.subAggregation(AggregationBuilders.terms("region").field("city_id").size(10000));
		}
		return ab;
	}
	
	public static AggregationBuilder orderRegionAgentAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		ab = AggregationBuilders.terms("batch_id").field("batch_id").size(10000);
		ab.subAggregation(AggregationBuilders.terms("region").field("province_id").size(10000));
		return ab;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static AggregationBuilder regionViewAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		if(1 == Integer.valueOf(requestMap.get("regionType").toString())) {
			ab = AggregationBuilders.terms("region").field("province_id").size(5000);
		} else {
			ab = AggregationBuilders.terms("region").field("city_id").size(5000);
		}
		ab.subAggregation(AggregationBuilders.count("pv").field("user_id"))
		.subAggregation(AggregationBuilders.cardinality("uv").field("user_id"));
		return ab;
	}
	
	public static AggregationBuilder agentRegionAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		ab = AggregationBuilders.terms("region").field("province_id").size(5000);
		ab.subAggregation(AggregationBuilders.count("pv").field("user_id"))
		.subAggregation(AggregationBuilders.cardinality("uv").field("user_id"));
		return ab;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static AggregationBuilder regionSignAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		if(1 == Integer.valueOf(requestMap.get("regionType").toString())) {
			ab = AggregationBuilders.terms("region").field("province_id").order(BucketOrder.aggregation("registCnt", false));
			if(-1 == Integer.valueOf(requestMap.get("size").toString())) 
				ab = AggregationBuilders.terms("region").field("province_id").order(BucketOrder.aggregation("registCnt", false)).size(5);
			else {
				ab = AggregationBuilders.terms("region").field("province_id").order(BucketOrder.aggregation("registCnt", false)).size(5000);
			}
		} else {
			ab = AggregationBuilders.terms("region").field("city_id").order(BucketOrder.aggregation("registCnt", false));
			if(-1 == Integer.valueOf(requestMap.get("size").toString())) 
				ab = AggregationBuilders.terms("region").field("city_id").order(BucketOrder.aggregation("registCnt", false)).size(5);
			else {
				ab = AggregationBuilders.terms("region").field("city_id").order(BucketOrder.aggregation("registCnt", false)).size(5000);
			}
		}
		ab.subAggregation(AggregationBuilders.count("registCnt").field("user_id"));
		return ab;
	}
	
	/**
	 * @param requestMap
	 * @return
	 */
	public static AggregationBuilder registChannelAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		if(-1 == Integer.valueOf(requestMap.get("size").toString())) {
			ab = AggregationBuilders.terms("channelCnt").field("channel_id").order(BucketOrder.aggregation("registCnt", false)).size(5);
		} else {
			ab = AggregationBuilders.terms("channelCnt").field("channel_id").order(BucketOrder.aggregation("registCnt", false)).size(5000);
		}
		ab.subAggregation(AggregationBuilders.count("registCnt").field("user_id"));
		return ab;
	}
	
	/**
	 *
	 * @param requestMap
	 * @return
	 */
	public static AggregationBuilder payFromAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		if(-1 == Integer.valueOf(requestMap.get("size").toString())) {
			ab = AggregationBuilders.terms("payType").field("pay_way").order(BucketOrder.aggregation("userCnt", false)).size(5);
		} else {
			ab = AggregationBuilders.terms("payType").field("pay_way").order(BucketOrder.aggregation("userCnt", false)).size(1000);//求无限大方法
		}
		ab.subAggregation(AggregationBuilders.cardinality("userCnt").field("user_id"));
		return ab;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static AggregationBuilder clientFromAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		if(-1 == Integer.valueOf(requestMap.get("size").toString())) {
			ab = AggregationBuilders.terms("clientType").field("agent_type").order(BucketOrder.aggregation("userCnt", false)).size(5);
		} else {
			ab = AggregationBuilders.terms("clientType").field("agent_type").order(BucketOrder.aggregation("userCnt", false)).size(1000);//求无限大方法
		}
		ab.subAggregation(AggregationBuilders.cardinality("userCnt").field("user_id"));
		return ab;
	}
	
	public static BoolQueryBuilder report4vipBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		if (requestMap.get("dateType").equals("year")) {
			bqb.must(QueryBuilders.matchQuery("time_dim", "year"));
		} else {
			bqb.must(QueryBuilders.matchQuery("time_dim", "month"));
		}
		bqb.must(QueryBuilders.rangeQuery("stat_time.keyword").gte(requestMap.get("startDate")).lte(requestMap.get("endDate")));
		bqb.must(QueryBuilders.matchQuery("data_dim", "user_cnt"));
		return bqb;
	}
	
	public static BoolQueryBuilder multiAgentBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		if (null != requestMap.get("startDate")) {
			bqb.must(QueryBuilders.rangeQuery("start_time.keyword").gte(requestMap.get("startDate")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		}
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); 
		bqb.must(QueryBuilders.matchQuery("event_type", "ViewProduct")); 
		if (null != requestMap.get("agentIds")) {
			if(!"all".equals(requestMap.get("productType"))) {
				bqb.must(QueryBuilders.matchQuery("product_type", requestMap.get("productType")));//活动类型
			}
			Object[] agentIds = (Object[]) requestMap.get("agentIds");
			bqb.must(QueryBuilders.termsQuery("agent_id", agentIds));
		}
		return bqb;
	}
	
	public static BoolQueryBuilder agentRegionBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("start_time.keyword").gte(requestMap.get("startDate")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); 
		bqb.must(QueryBuilders.matchQuery("event_type", "ViewProduct")); 
		bqb.must(QueryBuilders.matchQuery("agent_id", requestMap.get("agentId")));
		return bqb;
	}
	
	public static AggregationBuilder multiAgentAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		ab = AggregationBuilders.terms("agent").field("agent_id").order(BucketOrder.aggregation("uv", false)).size(1000);//求无限大方法
		ab.subAggregation(AggregationBuilders.count("pv").field("user_id"))
		.subAggregation(AggregationBuilders.cardinality("uv").field("user_id"));
		return ab;
	}
	
	/**
	 * 
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder orderAgentBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		if (null != requestMap.get("startDate")) {
			bqb.must(QueryBuilders.rangeQuery("end_time.keyword").gte(requestMap.get("startDate")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		}
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		if(null != requestMap.get("agentIds")) {
			if(!"all".equals(requestMap.get("productType"))) {
				bqb.must(QueryBuilders.matchQuery("product_type", requestMap.get("productType")));//活动类型
			}
			Object[] agentIds = (Object[]) requestMap.get("agentIds");
			bqb.must(QueryBuilders.termsQuery("agent_id", agentIds));
		}
		bqb.must(QueryBuilders.matchQuery("event_type", "paidOrder")); //组织ID
		return bqb;
	}
	
	public static BoolQueryBuilder orderAgentRegionBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("end_time.keyword").gte(requestMap.get("startDate")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		bqb.must(QueryBuilders.matchQuery("agent_id", requestMap.get("agentId"))); //组织ID
		bqb.must(QueryBuilders.matchQuery("event_type", "paidOrder")); //组织ID
		return bqb;
	}
	
	public static AggregationBuilder orderAgentAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		ab = AggregationBuilders.terms("agent").field("agent_id").order(BucketOrder.aggregation("orderAmount", false)).size(1000);//求无限大方法
		ab.subAggregation(AggregationBuilders.cardinality("orderCnt").field("batch_id"))
		.subAggregation(AggregationBuilders.sum("orderAmount").field("order_amount"))
		.subAggregation(AggregationBuilders.cardinality("userCnt").field("user_id"))
		.subAggregation(AggregationBuilders.sum("productCnt").field("product_num"));
		return ab;
	}
	
	public static AggregationBuilder agentAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		ab = AggregationBuilders.terms("batch_id").field("batch_id").size(10000);//求无限大方法
		ab.subAggregation(AggregationBuilders.terms("agent").field("agent_id"))
		.subAggregation(AggregationBuilders.sum("freightAmount").field("freight_money"));
		return ab;
	}
	
}

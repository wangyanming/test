package com.hds.hdyapp.util;

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
	 * order query
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder orderTestBqb(Map<String, Object> requestMap) {
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
	 * logon query
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder logonBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		if (null != requestMap.get("searchType") && requestMap.get("searchType").equals("day")) {
			bqb.must(QueryBuilders.rangeQuery("visit_time.keyword").gte(requestMap.get("today")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		} else if (null != requestMap.get("searchType") && requestMap.get("searchType").equals("week")) {
			bqb.must(QueryBuilders.rangeQuery("visit_time.keyword").gte(DateUtil.startWeek(requestMap.get("endDate").toString())).lte(requestMap.get("endDate")));
		} else if (null != requestMap.get("searchType") && requestMap.get("searchType").equals("month")) {
			bqb.must(QueryBuilders.rangeQuery("visit_time.keyword").gte(DateUtil.startMonth(requestMap.get("endDate").toString())).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		} else if (null != requestMap.get("searchType") && requestMap.get("searchType").equals("year")) {
			bqb.must(QueryBuilders.rangeQuery("visit_time.keyword").gte(DateUtil.startYear(requestMap.get("endDate").toString())).lte(requestMap.get("endDate")));
		} else {
			bqb.must(QueryBuilders.rangeQuery("visit_time.keyword").gte(requestMap.get("startDate")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		}
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		if (null != requestMap.get("channelId")) {
			bqb.must(QueryBuilders.matchQuery("channel_id", requestMap.get("channelId")));
		}
		if (null != requestMap.get("regionId")) {
			bqb.must(QueryBuilders.matchQuery("province_id", requestMap.get("regionId")));
		}
		if (null != requestMap.get("clientId")) {
			bqb.must(QueryBuilders.matchQuery("agent_type", requestMap.get("clientId")));
		}
		return bqb;
	}
	
	/**
	 * order query
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder orderBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		if (null != requestMap.get("searchType") && requestMap.get("searchType").equals("day")) {
			bqb.must(QueryBuilders.rangeQuery("end_time.keyword").gte(requestMap.get("today")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		} else if (null != requestMap.get("searchType") && requestMap.get("searchType").equals("week")) {
			bqb.must(QueryBuilders.rangeQuery("end_time.keyword").gte(DateUtil.startWeek(requestMap.get("endDate").toString())).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		} else if (null != requestMap.get("searchType") && requestMap.get("searchType").equals("month")) {
			bqb.must(QueryBuilders.rangeQuery("end_time.keyword").gte(DateUtil.startMonth(requestMap.get("endDate").toString())).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		} else if (null != requestMap.get("searchType") && requestMap.get("searchType").equals("year")) {
			bqb.must(QueryBuilders.rangeQuery("end_time.keyword").gte(DateUtil.startYear(requestMap.get("endDate").toString())).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		} else {
			bqb.must(QueryBuilders.rangeQuery("end_time.keyword").gte(requestMap.get("startDate")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		}
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		
		//查询order索引无需处理productId==1的情况
		if(null != requestMap.get("productId") && -1 != Integer.valueOf(requestMap.get("productId").toString())) {
			//bqb.must(QueryBuilders.matchQuery("product_type", requestMap.get("productType")));//活动类型
			bqb.must(QueryBuilders.matchQuery("product_id", requestMap.get("productId"))); //活动ID
		}
		if (null != requestMap.get("channelId")) {
			bqb.must(QueryBuilders.matchQuery("channel_id", requestMap.get("channelId")));
		}
		if (null != requestMap.get("regionId")) {
			bqb.must(QueryBuilders.matchQuery("province_id", requestMap.get("regionId")));
		}
		if (null != requestMap.get("clientId")) {
			bqb.must(QueryBuilders.matchQuery("agent_type", requestMap.get("clientId")));
		}
		//bqb.must(QueryBuilders.matchQuery("product_type", "event")); //产品类型 event：票
		bqb.must(QueryBuilders.matchQuery("event_type", "paidOrder")); //组织ID
		return bqb;
	}
	
	/**
	 * order-view query
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder orderViewBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		
		if (null != requestMap.get("searchType") && requestMap.get("searchType").equals("day")) {
			bqb.must(QueryBuilders.rangeQuery("start_time.keyword").gte(requestMap.get("today")).lte(requestMap.get("endDate")));
		} else if (null != requestMap.get("searchType") && requestMap.get("searchType").equals("week")) {
			bqb.must(QueryBuilders.rangeQuery("start_time.keyword").gte(DateUtil.startWeek(requestMap.get("startDate").toString())).lte(requestMap.get("endDate")));
		} else if (null != requestMap.get("searchType") && requestMap.get("searchType").equals("month")) {
			bqb.must(QueryBuilders.rangeQuery("start_time.keyword").gte(DateUtil.startMonth(requestMap.get("startDate").toString())).lte(requestMap.get("endDate")));
		} else if (null != requestMap.get("searchType") && requestMap.get("searchType").equals("year")) {
			bqb.must(QueryBuilders.rangeQuery("start_time.keyword").gte(DateUtil.startYear(requestMap.get("endDate").toString())).lte(requestMap.get("endDate")));
		} else {
			bqb.must(QueryBuilders.rangeQuery("start_time.keyword").gte(requestMap.get("startDate")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		}
		
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		
		if(null != requestMap.get("productId") && -1 != Integer.valueOf(requestMap.get("productId").toString())) {
			//bqb.must(QueryBuilders.matchQuery("product_type", requestMap.get("productType")));//活动类型
			bqb.must(QueryBuilders.matchQuery("product_id", requestMap.get("productId"))); //活动ID
		}
		if (null != requestMap.get("channelId")) {
			bqb.must(QueryBuilders.matchQuery("channel_id", requestMap.get("channelId")));
		}
		if (null != requestMap.get("regionId")) {
			bqb.must(QueryBuilders.matchQuery("province_id", requestMap.get("regionId")));
		}
		if (null != requestMap.get("clientId")) {
			bqb.must(QueryBuilders.matchQuery("agent_type", requestMap.get("clientId")));
		}
		bqb.must(QueryBuilders.matchQuery("product_type", "event")); //产品类型 event：票
		bqb.must(QueryBuilders.matchQuery("event_type", "ViewProduct")); //组织ID
		return bqb;
	}
	
	/**
	 * orderTicketsBqb
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder orderTicketsBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("end_time.keyword").gte(requestMap.get("startDate")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		bqb.must(QueryBuilders.matchQuery("product_type", "event")); //产品类型 event：票
		bqb.must(QueryBuilders.matchQuery("event_type", "paidOrder")); //组织ID
		return bqb;
	}
	
	/**
	 * orderGoodsBqb
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder orderGoodsBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("end_time.keyword").gte(requestMap.get("startDate")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		bqb.must(QueryBuilders.matchQuery("product_type", "goods")); //产品类型 event：票, goods商品
		bqb.must(QueryBuilders.matchQuery("event_type", "paidOrder")); //组织ID
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
	public static BoolQueryBuilder signTodayBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("sign_time.keyword").gte(requestMap.get("today")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		bqb.must(QueryBuilders.matchQuery("event_type", "signUp")); //组织ID
		return bqb;
	}
	
	/**
	 * 日周月汇总索引查询方法
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder analysisDataBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		
		if (null != requestMap.get("searchType") && requestMap.get("searchType").equals("day")) {
			bqb.must(QueryBuilders.rangeQuery("stat_time.keyword").gte(requestMap.get("startDate")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		} else if (null != requestMap.get("searchType") && requestMap.get("searchType").equals("week")) {
			bqb.must(QueryBuilders.rangeQuery("stat_time.keyword").gte(DateUtil.startWeek(requestMap.get("startDate").toString())).lte(requestMap.get("endDate")));
		} else if (null != requestMap.get("searchType") && requestMap.get("searchType").equals("month")) {
			bqb.must(QueryBuilders.rangeQuery("stat_time.keyword").gte(DateUtil.startMonth(requestMap.get("startDate").toString())).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		} else if (null != requestMap.get("searchType") && requestMap.get("searchType").equals("year")) {
			bqb.must(QueryBuilders.rangeQuery("stat_time.keyword").gte(DateUtil.startYear(requestMap.get("startDate").toString())).lte(requestMap.get("endDate")));
		} else {
			bqb.must(QueryBuilders.rangeQuery("stat_time.keyword").gte(requestMap.get("startDate")).lte(requestMap.get("endDate"))); //gt:大于 lt:小于
		}
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		if (null != requestMap.get("productId") && -1 != Integer.valueOf(requestMap.get("productId").toString())) {
			bqb.must(QueryBuilders.matchQuery("product_type", requestMap.get("productType")));//活动类型
			bqb.must(QueryBuilders.matchQuery("product_id", requestMap.get("productId"))); //活动ID
		} else {
			bqb.must(QueryBuilders.matchQuery("product_id", -1));//活动类型
			bqb.must(QueryBuilders.matchQuery("product_type", "all")); //活动ID
		}
		return bqb;
	}
	
	/**
	 * 年汇总索引查询方法
	 * @param requestMap
	 * @return
	 */
	public static BoolQueryBuilder AnalysisYearBqb(Map<String, Object> requestMap) {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.must(QueryBuilders.rangeQuery("stat_time.keyword").gte(DateUtil.getYearByStr(requestMap.get("startDate").toString())).lte(DateUtil.getYearByStr(DateUtil.endYear(requestMap.get("endDate").toString())))); //gt:大于 lt:小于
		bqb.must(QueryBuilders.matchQuery("org_id", requestMap.get("orgId"))); //组织ID
		if (null != requestMap.get("productId") && -1 != Integer.valueOf(requestMap.get("productId").toString())) {
			bqb.must(QueryBuilders.matchQuery("product_type", requestMap.get("productType")));//活动类型
			bqb.must(QueryBuilders.matchQuery("product_id", requestMap.get("productId"))); //活动ID
		} else {
			bqb.must(QueryBuilders.matchQuery("product_id", "-1")); 
			bqb.must(QueryBuilders.matchQuery("product_type", "all"));
		}
		return bqb;
	}
	
	/**
	 * 支付渠道聚合方法
	 * order AggregationBuilder
	 * @param requestMap
	 * @return
	 */
	public static AggregationBuilder orderChannelAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		ab = AggregationBuilders.terms("channelCnt").field("channel_id").order(BucketOrder.aggregation("orderAmount", false)).size(10000);//求无限大方法
		ab.subAggregation(AggregationBuilders.count("orderCnt").field("order_id"))
		.subAggregation(AggregationBuilders.sum("orderAmount").field("order_amount"))
		.subAggregation(AggregationBuilders.cardinality("userCnt").field("user_id"))
		.subAggregation(AggregationBuilders.sum("productCnt").field("product_num"));
		return ab;
	}
	
	/**
	 * 运费计算聚合方法
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
	 * 运费计算聚合方法
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
	 * 
	 * @param requestMap
	 * @return
	 */
	public static AggregationBuilder orderClientAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		ab = AggregationBuilders.terms("clientCnt").field("agent_type").order(BucketOrder.aggregation("orderAmount", false)).size(10000);
		ab.subAggregation(AggregationBuilders.count("orderCnt").field("order_id"))
		.subAggregation(AggregationBuilders.sum("orderAmount").field("order_amount"))
		.subAggregation(AggregationBuilders.cardinality("userCnt").field("user_id"))
		.subAggregation(AggregationBuilders.sum("productCnt").field("product_num"));
		return ab;
	}
	
	/**
	 * getUserDistributionByDate--用户地域分布聚合方法
	 * @param requestMap
	 * @return
	 */
	public static AggregationBuilder orderRegionAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		ab = AggregationBuilders.terms("provinceId").field("province_id").size(10000);//求无限大方法
		ab.subAggregation(AggregationBuilders.count("orderCnt").field("order_id"))
		.subAggregation(AggregationBuilders.sum("orderAmount").field("order_amount"))
		.subAggregation(AggregationBuilders.cardinality("userCnt").field("user_id"))
		.subAggregation(AggregationBuilders.sum("productCnt").field("product_num"));
		return ab;
	}
	
	/**
	 * getUserPayWayByDate--用户支付方式聚合方法
	 * @param requestMap
	 * @return
	 */
	public static AggregationBuilder orderPayAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		ab = AggregationBuilders.terms("payWay").field("pay_way").size(5000);
		ab.subAggregation(AggregationBuilders.cardinality("userCnt").field("user_id"));
		return ab;
	}
	
	/**
	 * getBestSellingByDate--畅销排行聚合方法
	 * @param requestMap
	 * @return
	 */
	public static AggregationBuilder orderSellingAb(Map<String, Object> requestMap) {
		AggregationBuilder ab;
		if (Integer.valueOf(requestMap.get("sortType").toString()) == 1) {
			if (Integer.valueOf(requestMap.get("size").toString()) == 1) {
				ab = AggregationBuilders.terms("productId").field("product_id").order(BucketOrder.aggregation("orderAmount", false)).size(5);
			} else {
				ab = AggregationBuilders.terms("productId").field("product_id").order(BucketOrder.aggregation("orderAmount", false)).size(10000);
			}
		} else {
			if (Integer.valueOf(requestMap.get("size").toString()) == 1) {
				ab = AggregationBuilders.terms("productId").field("product_id").order(BucketOrder.aggregation("productCnt", false)).size(5);
			} else {
				ab = AggregationBuilders.terms("productId").field("product_id").order(BucketOrder.aggregation("productCnt", false)).size(10000);
			}
		}
		ab.subAggregation(AggregationBuilders.sum("productCnt").field("product_num"))
		.subAggregation(AggregationBuilders.sum("orderAmount").field("order_amount"))
		.subAggregation(AggregationBuilders.sum("freightAmount").field("freight_money"));
		return ab;
	}
}

package com.hds.cn.bi.util;

import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilders;

public class BaseServiceImpl {
	
	/**
	 * 运费查询通用方法
	 * @param requestMap
	 * @return
	 */
	public SearchResponse getFreightSr(Map<String, Object> requestMap) {
		SearchResponse freightSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("batch_id").field("batch_id").size(10000)
						.subAggregation(AggregationBuilders.terms("freight").field("freight_money"))
						.subAggregation(AggregationBuilders.sum("freightAmount").field("freight_money")))
				.get();
		
		return freightSr;
	}
	
	
}

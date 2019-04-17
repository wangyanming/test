package com.hds.hdyapp.service.impl;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.cardinality.InternalCardinality;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCount;
import org.springframework.stereotype.Service;

import com.hds.hdyapp.service.HdyAppService;
import com.hds.hdyapp.util.CommonConstant;
import com.hds.hdyapp.util.DateUtil;
import com.hds.hdyapp.util.EsClient;
import com.hds.hdyapp.util.EsUtil;
import com.hds.hdyapp.util.ListSortUtil;
import com.hds.hdyapp.vo.BestSellingVo;
import com.hds.hdyapp.vo.ClientCountVo;
import com.hds.hdyapp.vo.DataSourceVo;
import com.hds.hdyapp.vo.ProductDataVo;
import com.hds.hdyapp.vo.RegionDistributionVo;
import com.hds.hdyapp.vo.TotalDataVo;
import com.hds.hdyapp.vo.UserDistributionVo;
import com.hds.hdyapp.vo.UserPayWayVo;

@Service
public class IHdyAppServiceImpl implements HdyAppService{
	
	@Override
	public Map<String, Object> getTotalData(Map<String, Object> requestMap) {
		double freight_money = 0;
		Map<String, Object> resultMap = new HashMap<String, Object>();

		// 查询统计周期内uv、pv
		SearchResponse logonResponse = EsClient.getConnect().prepareSearch("logon")
				.setQuery(EsUtil.logonBqb(requestMap))
				.addAggregation(AggregationBuilders.cardinality("uv").field("user_id"))
				.get();

		// 查询统计周期内销售金额、成功订单数、累计用户数（以手机号为维度的购买人即为用户）
		SearchResponse orderResponse = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.sum("orderAmount").field("order_amount")) 
				.addAggregation(AggregationBuilders.cardinality("orderCnt").field("batch_id"))
				.addAggregation(AggregationBuilders.cardinality("orderUser").field("user_id")) 
				.get(); 
		
		//售票数量
		SearchResponse ticketsResponse = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderTicketsBqb(requestMap))
				.addAggregation(AggregationBuilders.sum("ticketCnt").field("product_num")) 
				.get();
		
		//商品销售数
		SearchResponse goodsResponse = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderGoodsBqb(requestMap))
				.addAggregation(AggregationBuilders.sum("goodsCnt").field("product_num")) 
				.get();
		
		// 查询运费
		SearchResponse freightSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("batch_id").field("batch_id").size(10000)
						.subAggregation(AggregationBuilders.terms("freight").field("freight_money"))
						.subAggregation(AggregationBuilders.sum("freightAmount").field("freight_money")))
				.get();

		Map<String, Aggregation> freightMap = freightSr.getAggregations().asMap();
		StringTerms freightTerms = (StringTerms) freightMap.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> freightBucketIt = freightTerms
				.getBuckets().iterator();

		while (freightBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket freightBucket = freightBucketIt
					.next();
			Terms doubleTerms = (Terms) freightBucket.getAggregations().asMap().get("freight");
			Iterator<? extends org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> bucketsIt = doubleTerms
					.getBuckets().iterator();
			while (bucketsIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket = bucketsIt.next();
				freight_money = freight_money + (Double) bucket.getKeyAsNumber();
			}
		}

		DecimalFormat df = new DecimalFormat("0.00");
		resultMap.put("uv", ((InternalCardinality) logonResponse.getAggregations().asMap().get("uv")).getValue());
		resultMap.put("orderAmount", df.format(((InternalSum) orderResponse.getAggregations().asMap().get("orderAmount")).getValue() + freight_money));
		resultMap.put("orderCnt", ((InternalCardinality) orderResponse.getAggregations().asMap().get("orderCnt")).getValue());
		resultMap.put("orderUser", ((InternalCardinality) orderResponse.getAggregations().asMap().get("orderUser")).getValue());
		resultMap.put("ticketCnt", ((InternalSum) ticketsResponse.getAggregations().asMap().get("ticketCnt")).getValue());
		resultMap.put("goodsCnt", ((InternalSum) goodsResponse.getAggregations().asMap().get("goodsCnt")).getValue());
		return resultMap;
	}
	
	@Override
	public List<TotalDataVo> getTotalDataByDay(Map<String, Object> requestMap) {
		double freight_money = 0;
		List<TotalDataVo> totalList = new ArrayList<TotalDataVo>();
		if (DateUtil.dateAdd(DateUtil.dateStampToDay(System.currentTimeMillis()))
				.equals(requestMap.get("endDate"))) {// 查询统计周期内uv、pv
			requestMap.put("today", DateUtil.dateStampToDay(System.currentTimeMillis()));
			TotalDataVo totalDataVo = new TotalDataVo();
			SearchResponse logonResponse = EsClient.getConnect().prepareSearch("logon")
					.setQuery(EsUtil.logonBqb(requestMap))
					.addAggregation(AggregationBuilders.cardinality("uv").field("user_id"))
					.get();
			
			SearchResponse viewResponse = EsClient.getConnect().prepareSearch("order")
					.setQuery(EsUtil.orderViewBqb(requestMap))
					.addAggregation(AggregationBuilders.cardinality("uv").field("user_id"))
					.get();
			
			// 查询统计周期内销售金额、成功订单数、累计用户数（以手机号为维度的购买人即为用户）
			SearchResponse orderResponse = EsClient.getConnect().prepareSearch("order")
					.setQuery(EsUtil.orderBqb(requestMap))
					.addAggregation(AggregationBuilders.sum("orderAmount").field("order_amount")) 
					.addAggregation(AggregationBuilders.cardinality("orderCnt").field("batch_id")) 
					.addAggregation(AggregationBuilders.cardinality("orderUser").field("user_id"))
					.addAggregation(AggregationBuilders.sum("productCnt").field("product_num"))
					.get(); 
			
			// 查询运费
			SearchResponse freightSr = EsClient.getConnect().prepareSearch("order")
					.setQuery(EsUtil.orderBqb(requestMap))
					.addAggregation(AggregationBuilders.terms("batch_id").field("batch_id").size(10000)
							.subAggregation(AggregationBuilders.terms("freight").field("freight_money"))
							.subAggregation(AggregationBuilders.sum("freightAmount").field("freight_money")))
					.get();

			Map<String, Aggregation> freightMap = freightSr.getAggregations().asMap();
			StringTerms freightTerms = (StringTerms) freightMap.get("batch_id");
			Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> freightBucketIt = freightTerms
					.getBuckets().iterator();

			while (freightBucketIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket freightBucket = freightBucketIt
						.next();
				Terms doubleTerms = (Terms) freightBucket.getAggregations().asMap().get("freight");
				Iterator<? extends org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> bucketsIt = doubleTerms
						.getBuckets().iterator();
				while (bucketsIt.hasNext()) {
					org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket = bucketsIt.next();
					freight_money = freight_money + (Double) bucket.getKeyAsNumber();
				}
			}
			DecimalFormat df = new DecimalFormat("0.00");
			totalDataVo.setDate(DateUtil.toDate(DateUtil.dateStampToDay(System.currentTimeMillis())));
			if (null != requestMap.get("productId") && -1 != Integer.valueOf(requestMap.get("productId").toString())) {
				totalDataVo.setUv((int)((InternalCardinality) viewResponse.getAggregations().asMap().get("uv")).getValue());
			} else {
				totalDataVo.setUv((int)((InternalCardinality) logonResponse.getAggregations().asMap().get("uv")).getValue());
			}
			totalDataVo.setOrderAmount(df.format(((InternalSum) orderResponse.getAggregations().asMap().get("orderAmount")).getValue() + freight_money));
			totalDataVo.setOrderCnt((int)((InternalCardinality) orderResponse.getAggregations().asMap().get("orderCnt")).getValue());
			totalDataVo.setProductCnt((int)((InternalSum) orderResponse.getAggregations().asMap().get("productCnt")).getValue());
			totalList.add(totalDataVo);
		}

		SearchResponse sr = EsClient.getConnect().prepareSearch("bi_user_analysis_day")
				.setQuery(EsUtil.analysisDataBqb(requestMap)).setFrom(0).setSize(5000).get();
		for (SearchHit srHit : sr.getHits().getHits()) {
			TotalDataVo totalDataVo = new TotalDataVo();
			int uv = Integer.valueOf(null == srHit.getSourceAsMap().get("unique_visitor") ? "0"
					: srHit.getSourceAsMap().get("unique_visitor").toString());
			String orderAmount = String.valueOf(null == srHit.getSourceAsMap().get("order_amount") ? "0"
					: srHit.getSourceAsMap().get("order_amount").toString());
			int orderCnt = Integer.valueOf(null == srHit.getSourceAsMap().get("order_count") ? "0"
					: srHit.getSourceAsMap().get("order_count").toString());
			int productCnt = Integer.valueOf(null == srHit.getSourceAsMap().get("product_number") ? "0"
					: srHit.getSourceAsMap().get("product_number").toString());

			totalDataVo.setDate(DateUtil.toDate((srHit.getSourceAsMap().get("stat_time").toString())));
			totalDataVo.setUv(uv);
			totalDataVo.setOrderAmount(orderAmount);
			totalDataVo.setOrderCnt(orderCnt);
			totalDataVo.setProductCnt(productCnt);

			totalList.add(totalDataVo);
		}
		ListSortUtil.listSort(totalList);
		return totalList;
	}

	@Override
	public List<TotalDataVo> getTotalDataByWeek(Map<String, Object> requestMap) {
		double freight_money = 0;
		List<TotalDataVo> totalList = new ArrayList<TotalDataVo>();
		if (DateUtil.dateAdd(DateUtil.dateStampToDay(System.currentTimeMillis()))
				.equals(requestMap.get("endDate"))) {// 查询统计周期内uv、pv
			TotalDataVo totalDataVo = new TotalDataVo();
			SearchResponse logonResponse = EsClient.getConnect().prepareSearch("logon")
					//.setQuery(EsUtil.logonWeekBqb(requestMap))
					.setQuery(EsUtil.logonBqb(requestMap))
					.addAggregation(AggregationBuilders.cardinality("uv").field("user_id"))
					.get();
			
			SearchResponse viewResponse = EsClient.getConnect().prepareSearch("order")
					//.setQuery(EsUtil.orderWeekViewBqb(requestMap))
					.setQuery(EsUtil.orderViewBqb(requestMap))
					.addAggregation(AggregationBuilders.cardinality("uv").field("user_id"))
					.get();
			
			// 查询统计周期内销售金额、成功订单数、累计用户数（以手机号为维度的购买人即为用户）
			SearchResponse orderResponse = EsClient.getConnect().prepareSearch("order")
					//.setQuery(EsUtil.orderWeekBqb(requestMap))
					.setQuery(EsUtil.orderBqb(requestMap))
					.addAggregation(AggregationBuilders.sum("orderAmount").field("order_amount")) 
					.addAggregation(AggregationBuilders.cardinality("orderCnt").field("batch_id")) 
					.addAggregation(AggregationBuilders.cardinality("orderUser").field("user_id"))
					.addAggregation(AggregationBuilders.sum("productCnt").field("product_num"))
					.get(); 
			
			// 查询运费
			SearchResponse freightSr = EsClient.getConnect().prepareSearch("order")
					//.setQuery(EsUtil.orderWeekBqb(requestMap))
					.setQuery(EsUtil.orderBqb(requestMap))
					.addAggregation(AggregationBuilders.terms("batch_id").field("batch_id").size(10000)
							.subAggregation(AggregationBuilders.terms("freight").field("freight_money"))
							.subAggregation(AggregationBuilders.sum("freightAmount").field("freight_money")))
					.get();

			Map<String, Aggregation> freightMap = freightSr.getAggregations().asMap();
			StringTerms freightTerms = (StringTerms) freightMap.get("batch_id");
			Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> freightBucketIt = freightTerms
					.getBuckets().iterator();

			while (freightBucketIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket freightBucket = freightBucketIt
						.next();
				Terms doubleTerms = (Terms) freightBucket.getAggregations().asMap().get("freight");
				Iterator<? extends org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> bucketsIt = doubleTerms
						.getBuckets().iterator();
				while (bucketsIt.hasNext()) {
					org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket = bucketsIt.next();
					freight_money = freight_money + (Double) bucket.getKeyAsNumber();
				}
			}
			DecimalFormat df = new DecimalFormat("0.00");
			totalDataVo.setDate(DateUtil.getWeek(requestMap.get("endDate").toString()));
			if (null != requestMap.get("productId") && -1 != Integer.valueOf(requestMap.get("productId").toString())) {
				totalDataVo.setUv((int)((InternalCardinality) viewResponse.getAggregations().asMap().get("uv")).getValue());
			} else {
				totalDataVo.setUv((int)((InternalCardinality) logonResponse.getAggregations().asMap().get("uv")).getValue());
			}
			totalDataVo.setOrderAmount(df.format(((InternalSum) orderResponse.getAggregations().asMap().get("orderAmount")).getValue() + freight_money));
			totalDataVo.setOrderCnt((int)((InternalCardinality) orderResponse.getAggregations().asMap().get("orderCnt")).getValue());
			totalDataVo.setProductCnt((int)((InternalSum) orderResponse.getAggregations().asMap().get("productCnt")).getValue());
			totalList.add(totalDataVo);
		}

		SearchResponse sr = EsClient.getConnect().prepareSearch("bi_user_analysis_week")
				.setQuery(EsUtil.analysisDataBqb(requestMap)).setFrom(0).setSize(5000).get();
		for (SearchHit srHit : sr.getHits().getHits()) {
			TotalDataVo totalDataVo = new TotalDataVo();
			int uv = Integer.valueOf(null == srHit.getSourceAsMap().get("unique_visitor") ? "0"
					: srHit.getSourceAsMap().get("unique_visitor").toString());
			String orderAmount = String.valueOf(null == srHit.getSourceAsMap().get("order_amount") ? "0"
					: srHit.getSourceAsMap().get("order_amount").toString());
			int orderCnt = Integer.valueOf(null == srHit.getSourceAsMap().get("order_count") ? "0"
					: srHit.getSourceAsMap().get("order_count").toString());
			int productCnt = Integer.valueOf(null == srHit.getSourceAsMap().get("product_number") ? "0"
					: srHit.getSourceAsMap().get("product_number").toString());

			totalDataVo.setDate(DateUtil.getWeek((srHit.getSourceAsMap().get("stat_time").toString())));
			totalDataVo.setUv(uv);
			totalDataVo.setOrderAmount(orderAmount);
			totalDataVo.setOrderCnt(orderCnt);
			totalDataVo.setProductCnt(productCnt);

			totalList.add(totalDataVo);
		}
		ListSortUtil.listSort(totalList);
		return totalList;
	}

	@Override
	public List<TotalDataVo> getTotalDataByMonth(Map<String, Object> requestMap) {
		double freight_money = 0;
		List<TotalDataVo> totalList = new ArrayList<TotalDataVo>();
		if (DateUtil.dateAdd(DateUtil.dateStampToDay(System.currentTimeMillis()))
				.equals(requestMap.get("endDate"))) {// 查询统计周期内uv、pv
			TotalDataVo totalDataVo = new TotalDataVo();
			SearchResponse logonResponse = EsClient.getConnect().prepareSearch("logon")
					//.setQuery(EsUtil.logonMonthBqb(requestMap))
					.setQuery(EsUtil.logonBqb(requestMap))
					.addAggregation(AggregationBuilders.cardinality("uv").field("user_id"))
					.get();
			
			SearchResponse viewResponse = EsClient.getConnect().prepareSearch("order")
					//.setQuery(EsUtil.orderMonthViewBqb(requestMap))
					.setQuery(EsUtil.orderViewBqb(requestMap))
					.addAggregation(AggregationBuilders.cardinality("uv").field("user_id"))
					.get();
			
			// 查询统计周期内销售金额、成功订单数、累计用户数（以手机号为维度的购买人即为用户）
			SearchResponse orderResponse = EsClient.getConnect().prepareSearch("order")
					//.setQuery(EsUtil.orderMonthBqb(requestMap))
					.setQuery(EsUtil.orderBqb(requestMap))
					.addAggregation(AggregationBuilders.sum("orderAmount").field("order_amount")) 
					.addAggregation(AggregationBuilders.cardinality("orderCnt").field("batch_id")) 
					.addAggregation(AggregationBuilders.cardinality("orderUser").field("user_id"))
					.addAggregation(AggregationBuilders.sum("productCnt").field("product_num"))
					.get(); 
			
			// 查询运费
			SearchResponse freightSr = EsClient.getConnect().prepareSearch("order")
					//.setQuery(EsUtil.orderMonthBqb(requestMap))
					.setQuery(EsUtil.orderBqb(requestMap))
					.addAggregation(AggregationBuilders.terms("batch_id").field("batch_id").size(10000)
							.subAggregation(AggregationBuilders.terms("freight").field("freight_money"))
							.subAggregation(AggregationBuilders.sum("freightAmount").field("freight_money")))
					.get();

			Map<String, Aggregation> freightMap = freightSr.getAggregations().asMap();
			StringTerms freightTerms = (StringTerms) freightMap.get("batch_id");
			Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> freightBucketIt = freightTerms
					.getBuckets().iterator();

			while (freightBucketIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket freightBucket = freightBucketIt
						.next();
				Terms doubleTerms = (Terms) freightBucket.getAggregations().asMap().get("freight");
				Iterator<? extends org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> bucketsIt = doubleTerms
						.getBuckets().iterator();
				while (bucketsIt.hasNext()) {
					org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket = bucketsIt.next();
					freight_money = freight_money + (Double) bucket.getKeyAsNumber();
				}
			}
			DecimalFormat df = new DecimalFormat("0.00");
			totalDataVo.setDate(DateUtil.getMonth(requestMap.get("endDate").toString()));
			if (null != requestMap.get("productId") && -1 != Integer.valueOf(requestMap.get("productId").toString())) {
				totalDataVo.setUv((int)((InternalCardinality) viewResponse.getAggregations().asMap().get("uv")).getValue());
			} else {
				totalDataVo.setUv((int)((InternalCardinality) logonResponse.getAggregations().asMap().get("uv")).getValue());
			}
			totalDataVo.setUv((int)((InternalCardinality) logonResponse.getAggregations().asMap().get("uv")).getValue());
			totalDataVo.setOrderAmount(df.format(((InternalSum) orderResponse.getAggregations().asMap().get("orderAmount")).getValue() + freight_money));
			totalDataVo.setOrderCnt((int)((InternalCardinality) orderResponse.getAggregations().asMap().get("orderCnt")).getValue());
			totalDataVo.setProductCnt((int)((InternalSum) orderResponse.getAggregations().asMap().get("productCnt")).getValue());
			totalList.add(totalDataVo);
		}

		SearchResponse sr = EsClient.getConnect().prepareSearch("bi_user_analysis_month")
				.setQuery(EsUtil.analysisDataBqb(requestMap)).setFrom(0).setSize(5000).get();
		for (SearchHit srHit : sr.getHits().getHits()) {
			TotalDataVo totalDataVo = new TotalDataVo();
			int uv = Integer.valueOf(null == srHit.getSourceAsMap().get("unique_visitor") ? "0"
					: srHit.getSourceAsMap().get("unique_visitor").toString());
			String orderAmount = String.valueOf(null == srHit.getSourceAsMap().get("order_amount") ? "0"
					: srHit.getSourceAsMap().get("order_amount").toString());
			int orderCnt = Integer.valueOf(null == srHit.getSourceAsMap().get("order_count") ? "0"
					: srHit.getSourceAsMap().get("order_count").toString());
			int productCnt = Integer.valueOf(null == srHit.getSourceAsMap().get("product_number") ? "0"
					: srHit.getSourceAsMap().get("product_number").toString());

			totalDataVo.setDate(DateUtil.getMonth((srHit.getSourceAsMap().get("stat_time").toString())));
			totalDataVo.setUv(uv);
			totalDataVo.setOrderAmount(orderAmount);
			totalDataVo.setOrderCnt(orderCnt);
			totalDataVo.setProductCnt(productCnt);

			totalList.add(totalDataVo);
		}
		ListSortUtil.listMonthSort(totalList);
		return totalList;
	}

	@Override
	public List<TotalDataVo> getTotalDataByYear(Map<String, Object> requestMap) {
		double freight_money = 0;
		List<TotalDataVo> totalList = new ArrayList<TotalDataVo>();
		if (DateUtil.dateAdd(DateUtil.dateStampToDay(System.currentTimeMillis()))
				.equals(requestMap.get("endDate"))) {// 查询统计周期内uv、pv
			TotalDataVo totalDataVo = new TotalDataVo();
			SearchResponse logonResponse = EsClient.getConnect().prepareSearch("logon")
					//.setQuery(EsUtil.logonYearBqb(requestMap))
					.setQuery(EsUtil.logonBqb(requestMap))
					.addAggregation(AggregationBuilders.cardinality("uv").field("user_id"))
					.get();
			
			SearchResponse viewResponse = EsClient.getConnect().prepareSearch("order")
					.setQuery(EsUtil.orderViewBqb(requestMap))
					.addAggregation(AggregationBuilders.cardinality("uv").field("user_id"))
					.get();
			
			// 查询统计周期内销售金额、成功订单数、累计用户数（以手机号为维度的购买人即为用户）
			SearchResponse orderResponse = EsClient.getConnect().prepareSearch("order")
					//.setQuery(EsUtil.orderYearBqb(requestMap))
					.setQuery(EsUtil.orderBqb(requestMap))
					.addAggregation(AggregationBuilders.sum("orderAmount").field("order_amount")) 
					.addAggregation(AggregationBuilders.cardinality("orderCnt").field("batch_id")) 
					.addAggregation(AggregationBuilders.cardinality("orderUser").field("user_id"))
					.addAggregation(AggregationBuilders.sum("productCnt").field("product_num"))
					.get(); 
			
			// 查询运费
			SearchResponse freightSr = EsClient.getConnect().prepareSearch("order")
					//.setQuery(EsUtil.orderYearBqb(requestMap))
					.setQuery(EsUtil.orderBqb(requestMap))
					.addAggregation(AggregationBuilders.terms("batch_id").field("batch_id").size(10000)
							.subAggregation(AggregationBuilders.terms("freight").field("freight_money"))
							.subAggregation(AggregationBuilders.sum("freightAmount").field("freight_money")))
					.get();

			Map<String, Aggregation> freightMap = freightSr.getAggregations().asMap();
			StringTerms freightTerms = (StringTerms) freightMap.get("batch_id");
			Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> freightBucketIt = freightTerms
					.getBuckets().iterator();

			while (freightBucketIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket freightBucket = freightBucketIt
						.next();
				Terms doubleTerms = (Terms) freightBucket.getAggregations().asMap().get("freight");
				Iterator<? extends org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> bucketsIt = doubleTerms
						.getBuckets().iterator();
				while (bucketsIt.hasNext()) {
					org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket = bucketsIt.next();
					freight_money = freight_money + (Double) bucket.getKeyAsNumber();
				}
			}
			DecimalFormat df = new DecimalFormat("0.00");
			if (null != requestMap.get("productId") && -1 != Integer.valueOf(requestMap.get("productId").toString())) {
				totalDataVo.setUv((int)((InternalCardinality) viewResponse.getAggregations().asMap().get("uv")).getValue());
			} else {
				totalDataVo.setUv((int)((InternalCardinality) logonResponse.getAggregations().asMap().get("uv")).getValue());
			}
			totalDataVo.setDate(DateUtil.getYearByStr(requestMap.get("endDate").toString()) + CommonConstant.YEAR1);
			totalDataVo.setUv((int)((InternalCardinality) logonResponse.getAggregations().asMap().get("uv")).getValue());
			totalDataVo.setOrderAmount(df.format(((InternalSum) orderResponse.getAggregations().asMap().get("orderAmount")).getValue() + freight_money));
			totalDataVo.setOrderCnt((int)((InternalCardinality) orderResponse.getAggregations().asMap().get("orderCnt")).getValue());
			totalDataVo.setProductCnt((int)((InternalSum) orderResponse.getAggregations().asMap().get("productCnt")).getValue());
			totalList.add(totalDataVo);
		}

		SearchResponse sr = EsClient.getConnect().prepareSearch("bi_user_analysis_year")
				.setQuery(EsUtil.AnalysisYearBqb(requestMap)).setFrom(0).setSize(5000).get();
		for (SearchHit srHit : sr.getHits().getHits()) {
			TotalDataVo totalDataVo = new TotalDataVo();
			int uv = Integer.valueOf(null == srHit.getSourceAsMap().get("unique_visitor") ? "0"
					: srHit.getSourceAsMap().get("unique_visitor").toString());
			String orderAmount = String.valueOf(null == srHit.getSourceAsMap().get("order_amount") ? "0"
					: srHit.getSourceAsMap().get("order_amount").toString());
			int orderCnt = Integer.valueOf(null == srHit.getSourceAsMap().get("order_count") ? "0"
					: srHit.getSourceAsMap().get("order_count").toString());
			int productCnt = Integer.valueOf(null == srHit.getSourceAsMap().get("product_number") ? "0"
					: srHit.getSourceAsMap().get("product_number").toString());

			totalDataVo.setDate(srHit.getSourceAsMap().get("stat_time").toString() + CommonConstant.YEAR1);
			totalDataVo.setUv(uv);
			totalDataVo.setOrderAmount(orderAmount);
			totalDataVo.setOrderCnt(orderCnt);
			totalDataVo.setProductCnt(productCnt);

			totalList.add(totalDataVo);
		}
		ListSortUtil.listYearSort(totalList);
		return totalList;
	}

	@Override
	public List<DataSourceVo> getDataSourceByDate(Map<String, Object> requestMap) {
		DecimalFormat df = new DecimalFormat("0.00");
		double order_amount = 0;
		double freight_money = 0;
		Map<String, Object> freight = new HashMap<String, Object>();
		Map<String, Object> batch = new HashMap<String, Object>();
		Map<String, Object> batch1 = new HashMap<String, Object>();
		Map<String, Object> logon = new HashMap<String, Object>();
		Map<String, Object> order = new HashMap<String, Object>();
		
		// 查询统计周期内uv、pv
		SearchResponse logonResponse = EsClient.getConnect().prepareSearch("logon")
				.setQuery(EsUtil.logonBqb(requestMap))
				.addAggregation(AggregationBuilders.cardinality("uv").field("user_id"))
				.get();

		// 查询统计周期内销售金额、成功订单数、累计用户数（以手机号为维度的购买人即为用户）
		SearchResponse orderResponse = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.sum("orderAmount").field("order_amount")) 
				.addAggregation(AggregationBuilders.cardinality("orderCnt").field("batch_id")) 
				.addAggregation(AggregationBuilders.cardinality("orderUser").field("user_id"))
				.addAggregation(AggregationBuilders.sum("productCnt").field("product_num"))
				.get();
		
		// 查询运费
		SearchResponse freightResponse = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("batch_id").field("batch_id").size(10000)
						.subAggregation(AggregationBuilders.terms("freight").field("freight_money"))
						.subAggregation(AggregationBuilders.sum("freightAmount").field("freight_money")))
				.get();

		Map<String, Aggregation> freightMap1 = freightResponse.getAggregations().asMap();
		StringTerms freightTerms1 = (StringTerms) freightMap1.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> freightBucketIt1 = freightTerms1
				.getBuckets().iterator();

		while (freightBucketIt1.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket freightBucket = freightBucketIt1
					.next();
			Terms doubleTerms = (Terms) freightBucket.getAggregations().asMap().get("freight");
			Iterator<? extends org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> bucketsIt = doubleTerms
					.getBuckets().iterator();
			while (bucketsIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket = bucketsIt.next();
				freight_money = freight_money + (Double) bucket.getKeyAsNumber();
			}
		}
		double totalUv =  (double)((InternalCardinality) logonResponse.getAggregations().asMap().get("uv")).getValue();
		double totalOrderAmount = (double)((InternalSum) orderResponse.getAggregations().asMap().get("orderAmount")).getValue() + freight_money;
		double totalOrderCnt = (double)((InternalCardinality) orderResponse.getAggregations().asMap().get("orderCnt")).getValue();
		double totalProductCnt = (double)((InternalSum) orderResponse.getAggregations().asMap().get("productCnt")).getValue();

		// 查询统计周期内uv、pv
		SearchResponse logonSr = EsClient.getConnect().prepareSearch("logon").setQuery(EsUtil.logonBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("channelCnt").field("channel_id").size(1000)
						.subAggregation(AggregationBuilders.cardinality("uv").field("user_id")))
				.get();
		
		// 按渠道查询订单数、订单金额、下单人数
		SearchResponse orderSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderChannelAb(requestMap)).get();

		//查询运费
		SearchResponse batchSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderBatchAb(requestMap)).get();
		
		SearchResponse freightSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderFreightAb(requestMap)).get();
		
		Map<String, Aggregation> batchtMap = batchSr.getAggregations().asMap();
		StringTerms batchTerms = (StringTerms) batchtMap.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> batchBucketIt = batchTerms
				.getBuckets().iterator();
		
		Map<String, Aggregation> freightMap = freightSr.getAggregations().asMap();
		StringTerms freightTerms = (StringTerms) freightMap.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> freightBucketIt = freightTerms
				.getBuckets().iterator();
		
		while (freightBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket freightBucket = freightBucketIt
					.next();
			Terms doubleTerms = (Terms) freightBucket.getAggregations().asMap().get("freight");
			Iterator<? extends org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> bucketsIt = doubleTerms
					.getBuckets().iterator();
			while (bucketsIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket = bucketsIt.next();
				freight_money = (Double) bucket.getKeyAsNumber();
			}
			freight.put(freightBucket.getKeyAsString(), freight_money);
		}
		
		while (batchBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket batchBucket = batchBucketIt
					.next();
			StringTerms sTerms = (StringTerms) batchBucket.getAggregations().asMap().get("channelCnt");
			Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> sIt = sTerms.getBuckets().iterator();
			while (sIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket sBucket = sIt.next();
				batch.put(batchBucket.getKeyAsString(), sBucket.getKeyAsString());
			}
		}
		
		for (Map.Entry<String, Object> frei : freight.entrySet()) {
			for (Map.Entry<String, Object> bat : batch.entrySet()) {
				if (frei.getKey().equals(bat.getKey())) {
					if (batch1.containsKey(bat.getValue())) {
						batch1.put(bat.getValue().toString(), (Double)frei.getValue() + (Double)batch1.get(bat.getValue().toString()));
					} else {
						batch1.put(bat.getValue().toString(), frei.getValue());
					}
				}
			}
		}
		
		Map<String, Aggregation> logonMap = logonSr.getAggregations().asMap();

		StringTerms logonTerms = (StringTerms) logonMap.get("channelCnt");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> logonBucketIt = logonTerms
				.getBuckets().iterator();

		while (logonBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket logonBucket = logonBucketIt.next();
			Map<String, Aggregation> subAggMap = logonBucket.getAggregations().asMap();
			int uv = (int) ((InternalCardinality) subAggMap.get("uv")).getValue();
			logon.put(logonBucket.getKey().toString(), logonBucket.getDocCount() + "," + uv);
		}

		Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();

		StringTerms orderTerms = (StringTerms) orderMap.get("channelCnt");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> orderBucketIt = orderTerms
				.getBuckets().iterator();
		while (orderBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket orderBucket = orderBucketIt.next();
			Map<String, Aggregation> subAggMap = orderBucket.getAggregations().asMap();
			// sum值获取方法
			double orderAmount = ((InternalSum) subAggMap.get("orderAmount")).getValue();
			// cardinality值获取方法
			int userCnt = (int) ((InternalCardinality) subAggMap.get("userCnt")).getValue();
			// count值获取方法
			int orderCnt = (int) ((InternalValueCount) subAggMap.get("orderCnt")).getValue();
			int productCnt = (int) ((InternalSum) subAggMap.get("productCnt")).getValue();
			order.put(orderBucket.getKey().toString(), orderAmount + "," + userCnt + "," + orderCnt + "," + productCnt);
		}
		
		List<DataSourceVo> sourceList = new ArrayList<DataSourceVo>();
		for (Map.Entry<String, Object> or : order.entrySet()) {
			Iterator<Entry<String, Object>> log = logon.entrySet().iterator();
			Iterator<Entry<String, Object>> bat = batch1.entrySet().iterator();
			while (log.hasNext()) {
				Entry<String, Object> logEntry = log.next();
				if (or.getKey().equals(logEntry.getKey())) {
					DataSourceVo sourceVo = new DataSourceVo();
					String[] array = (or.getValue() + "," + logEntry.getValue()).toString().split(",");
					sourceVo.setChannel_id(or.getKey());
					sourceVo.setUv(Integer.valueOf(array[5])); //UV
					sourceVo.setOrderCnt(Integer.valueOf(array[2])); //OrderCnt
					order_amount = Double.valueOf(array[0].toString()); 
					while (bat.hasNext()) {
						Entry<String, Object> batEntry = bat.next();
						if (or.getKey().equals(batEntry.getKey())) {
							order_amount = (double) batEntry.getValue() + Double.valueOf(array[0].toString());
						} 
					}
					sourceVo.setOrderAmount(df.format(order_amount)); //OrderAmount
					sourceVo.setProductCnt(Integer.valueOf(array[3])); //ProductCnt
					sourceVo.setOrderAmountRate(DateUtil.numFormat((order_amount) /totalOrderAmount));
					sourceVo.setOrderCntRate(DateUtil.numFormat(Integer.valueOf(array[2]) /totalOrderCnt));
					sourceVo.setProductCntRate(DateUtil.numFormat(Integer.valueOf(array[3]) /totalProductCnt));
					sourceVo.setUvRate(DateUtil.numFormat(Integer.valueOf(array[5]) /totalUv));
					sourceList.add(sourceVo);
					log.remove();
				}
			}
		}
		for (Map.Entry<String, Object> vw : logon.entrySet()) {
			DataSourceVo sourceVo = new DataSourceVo();
			String[] array = (vw.getValue()).toString().split(",");
			sourceVo.setChannel_id(vw.getKey());
			sourceVo.setUv(Integer.valueOf(array[1]));
			sourceVo.setOrderCnt(0);
			sourceVo.setOrderCntRate("0.00");
			sourceVo.setOrderAmount("0.00");
			sourceVo.setOrderAmountRate("0.00");
			sourceVo.setProductCnt(0);
			sourceVo.setProductCntRate("0.00");
			sourceVo.setUvRate(DateUtil.numFormat(Integer.valueOf(array[1]) /totalUv));
			sourceList.add(sourceVo);
		}
		return sourceList;
	}

	@Override
	public List<RegionDistributionVo> getRegionDistributionByDate(Map<String, Object> requestMap) {
		DecimalFormat df = new DecimalFormat("0.00");
		double order_amount = 0;
		double freight_money = 0;
		Map<String, Object> freight = new HashMap<String, Object>();
		Map<String, Object> batch = new HashMap<String, Object>();
		Map<String, Object> batch1 = new HashMap<String, Object>();
		Map<String, Object> logon = new HashMap<String, Object>();
		Map<String, Object> order = new HashMap<String, Object>();
		
		// 查询统计周期内uv、pv
		SearchResponse logonResponse = EsClient.getConnect().prepareSearch("logon")
				.setQuery(EsUtil.logonBqb(requestMap))
				.addAggregation(AggregationBuilders.cardinality("uv").field("user_id"))
				.get();
		
		// 查询统计周期内销售金额、成功订单数、累计用户数（以手机号为维度的购买人即为用户）
		SearchResponse orderResponse = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.sum("orderAmount").field("order_amount")) 
				.addAggregation(AggregationBuilders.cardinality("orderCnt").field("batch_id")) 
				.addAggregation(AggregationBuilders.cardinality("orderUser").field("user_id"))
				.addAggregation(AggregationBuilders.sum("productCnt").field("product_num"))
				.get();
		
		// 查询运费 TODO 耗时
		SearchResponse freightResponse = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("batch_id").field("batch_id").size(10000)
						.subAggregation(AggregationBuilders.terms("freight").field("freight_money"))
						.subAggregation(AggregationBuilders.sum("freightAmount").field("freight_money")))
				.get();
		
		Map<String, Aggregation> freightMap1 = freightResponse.getAggregations().asMap();
		StringTerms freightTerms1 = (StringTerms) freightMap1.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> freightBucketIt1 = freightTerms1
				.getBuckets().iterator();

		while (freightBucketIt1.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket freightBucket = freightBucketIt1
					.next();
			Terms doubleTerms = (Terms) freightBucket.getAggregations().asMap().get("freight");
			Iterator<? extends org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> bucketsIt = doubleTerms
					.getBuckets().iterator();
			while (bucketsIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket = bucketsIt.next();
				freight_money = freight_money + (Double) bucket.getKeyAsNumber();
			}
		}
		double totalUv =  (double)((InternalCardinality) logonResponse.getAggregations().asMap().get("uv")).getValue();
		double totalOrderAmount = (double)((InternalSum) orderResponse.getAggregations().asMap().get("orderAmount")).getValue() + freight_money;
		double totalOrderCnt = (double)((InternalCardinality) orderResponse.getAggregations().asMap().get("orderCnt")).getValue();
		double totalProductCnt = (double)((InternalSum) orderResponse.getAggregations().asMap().get("productCnt")).getValue();
		
		// 查询统计周期内uv、pv
		SearchResponse logonSr = EsClient.getConnect().prepareSearch("logon")
				.setQuery(EsUtil.logonBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("provinceId").field("province_id").size(1000)
						.subAggregation(AggregationBuilders.cardinality("uv").field("user_id")))
				.get();
		
		// 按渠道查询订单数、订单金额、下单人数
		SearchResponse orderSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderRegionAb(requestMap))
				.get();
		
		//查询运费
		SearchResponse batchSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderBatchAb(requestMap)).get();
		
		SearchResponse freightSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderFreightAb(requestMap)).get();
		
		Map<String, Aggregation> batchtMap = batchSr.getAggregations().asMap();
		StringTerms batchTerms = (StringTerms) batchtMap.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> batchBucketIt = batchTerms
				.getBuckets().iterator();
		
		Map<String, Aggregation> freightMap = freightSr.getAggregations().asMap();
		StringTerms freightTerms = (StringTerms) freightMap.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> freightBucketIt = freightTerms
				.getBuckets().iterator();
		
		while (freightBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket freightBucket = freightBucketIt
					.next();
			Terms doubleTerms = (Terms) freightBucket.getAggregations().asMap().get("freight");
			Iterator<? extends org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> bucketsIt = doubleTerms
					.getBuckets().iterator();
			while (bucketsIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket = bucketsIt.next();
				freight_money = (Double) bucket.getKeyAsNumber();
			}
			freight.put(freightBucket.getKeyAsString(), freight_money);
		}
		
		while (batchBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket batchBucket = batchBucketIt
					.next();
			StringTerms sTerms = (StringTerms) batchBucket.getAggregations().asMap().get("channelCnt");
			Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> sIt = sTerms.getBuckets().iterator();
			while (sIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket sBucket = sIt.next();
				batch.put(batchBucket.getKeyAsString(), sBucket.getKeyAsString());
			}
		}
		
		for (Map.Entry<String, Object> frei : freight.entrySet()) {
			for (Map.Entry<String, Object> bat : batch.entrySet()) {
				if (frei.getKey().equals(bat.getKey())) {
					if (batch1.containsKey(bat.getValue())) {
						batch1.put(bat.getValue().toString(), (Double)frei.getValue() + (Double)batch1.get(bat.getValue().toString()));
					} else {
						batch1.put(bat.getValue().toString(), frei.getValue());
					}
				}
			}
		}
		
		Map<String, Aggregation> logonMap = logonSr.getAggregations().asMap();

		LongTerms logonTerms = (LongTerms) logonMap.get("provinceId");
		Iterator<Bucket> logonBucketIt = logonTerms
				.getBuckets().iterator();

		while (logonBucketIt.hasNext()) {
			Bucket logonBucket = logonBucketIt.next();
			Map<String, Aggregation> subAggMap = logonBucket.getAggregations().asMap();
			int uv = (int) ((InternalCardinality) subAggMap.get("uv")).getValue();
			logon.put(logonBucket.getKey().toString(), logonBucket.getDocCount() + "," + uv);
		}

		Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();

		LongTerms orderTerms = (LongTerms) orderMap.get("provinceId");
		Iterator<Bucket> orderBucketIt = orderTerms
				.getBuckets().iterator();
		while (orderBucketIt.hasNext()) {
			Bucket orderBucket = orderBucketIt.next();
			Map<String, Aggregation> subAggMap = orderBucket.getAggregations().asMap();
			// sum值获取方法
			double orderAmount = ((InternalSum) subAggMap.get("orderAmount")).getValue();
			// cardinality值获取方法
			int userCnt = (int) ((InternalCardinality) subAggMap.get("userCnt")).getValue();
			// count值获取方法
			int orderCnt = (int) ((InternalValueCount) subAggMap.get("orderCnt")).getValue();
			int productCnt = (int) ((InternalSum) subAggMap.get("productCnt")).getValue();
			order.put(orderBucket.getKey().toString(), orderAmount + "," + userCnt + "," + orderCnt + "," + productCnt);
		}
		
		List<RegionDistributionVo> regionList = new ArrayList<RegionDistributionVo>();
		for (Map.Entry<String, Object> or : order.entrySet()) {
			Iterator<Entry<String, Object>> log = logon.entrySet().iterator();
			Iterator<Entry<String, Object>> bat = batch1.entrySet().iterator();
			while (log.hasNext()) {
				Entry<String, Object> logEntry = log.next();
				if (or.getKey().equals(logEntry.getKey())) {
					RegionDistributionVo regionVo = new RegionDistributionVo();
					String[] array = (or.getValue() + "," + logEntry.getValue()).toString().split(",");
					regionVo.setRegionId(Integer.valueOf(or.getKey()));
					regionVo.setUv(Integer.valueOf(array[5])); //UV
					regionVo.setOrderCnt(Integer.valueOf(array[2])); //OrderCnt
					order_amount = Double.valueOf(array[0].toString()); 
					while (bat.hasNext()) {
						Entry<String, Object> batEntry = bat.next();
						if (or.getKey().equals(batEntry.getKey())) {
							order_amount = (double) batEntry.getValue() + Double.valueOf(array[0].toString());
						} 
					}
					regionVo.setOrderAmount(df.format(order_amount)); //OrderAmount
					regionVo.setProductCnt(Integer.valueOf(array[3])); //ProductCnt
					regionVo.setOrderAmountRate(DateUtil.numFormat((order_amount) /totalOrderAmount));
					regionVo.setOrderCntRate(DateUtil.numFormat(Integer.valueOf(array[2]) /totalOrderCnt));
					regionVo.setProductCntRate(DateUtil.numFormat(Integer.valueOf(array[3]) /totalProductCnt));
					regionVo.setUvRate(DateUtil.numFormat(Integer.valueOf(array[5]) /totalUv));
					regionList.add(regionVo);
					log.remove();
				}
			}
		}
		for (Map.Entry<String, Object> vw : logon.entrySet()) {
			RegionDistributionVo regionVo = new RegionDistributionVo();
			String[] array = (vw.getValue()).toString().split(",");
			regionVo.setRegionId(Integer.valueOf(vw.getKey()));
			regionVo.setUv(Integer.valueOf(array[1]));
			regionVo.setOrderCnt(0);
			regionVo.setOrderCntRate("0.00");
			regionVo.setOrderAmount("0.00");
			regionVo.setOrderAmountRate("0.00");
			regionVo.setProductCnt(0);
			regionVo.setProductCntRate("0.00");
			regionVo.setUvRate(DateUtil.numFormat(Integer.valueOf(array[1]) /totalUv));
			regionList.add(regionVo);
		}
		return regionList;
	}

	@Override
	public List<ClientCountVo> getClientDataByDate(Map<String, Object> requestMap) {
		DecimalFormat df = new DecimalFormat("0.00");
		double order_amount = 0;
		double freight_money = 0;
		Map<String, Object> freight = new HashMap<String, Object>();
		Map<String, Object> batch = new HashMap<String, Object>();
		Map<String, Object> batch1 = new HashMap<String, Object>();
		Map<String, Object> logon = new HashMap<String, Object>();
		Map<String, Object> order = new HashMap<String, Object>();
		
		// 查询统计周期内uv、pv
		SearchResponse logonResponse = EsClient.getConnect().prepareSearch("logon")
				.setQuery(EsUtil.logonBqb(requestMap))
				.addAggregation(AggregationBuilders.cardinality("uv").field("user_id"))
				.get();

		// 查询统计周期内销售金额、成功订单数、累计用户数（以手机号为维度的购买人即为用户）
		SearchResponse orderResponse = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.sum("orderAmount").field("order_amount")) 
				.addAggregation(AggregationBuilders.cardinality("orderCnt").field("batch_id")) 
				.addAggregation(AggregationBuilders.cardinality("orderUser").field("user_id"))
				.addAggregation(AggregationBuilders.sum("productCnt").field("product_num"))
				.get();
		
		// 查询运费
		SearchResponse freightResponse = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("batch_id").field("batch_id").size(10000)
						.subAggregation(AggregationBuilders.terms("freight").field("freight_money"))
						.subAggregation(AggregationBuilders.sum("freightAmount").field("freight_money")))
				.get();

		Map<String, Aggregation> freightMap1 = freightResponse.getAggregations().asMap();
		StringTerms freightTerms1 = (StringTerms) freightMap1.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> freightBucketIt1 = freightTerms1
				.getBuckets().iterator();

		while (freightBucketIt1.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket freightBucket = freightBucketIt1
					.next();
			Terms doubleTerms = (Terms) freightBucket.getAggregations().asMap().get("freight");
			Iterator<? extends org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> bucketsIt = doubleTerms
					.getBuckets().iterator();
			while (bucketsIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket = bucketsIt.next();
				freight_money = freight_money + (Double) bucket.getKeyAsNumber();
			}
		}
		double totalUv =  (double)((InternalCardinality) logonResponse.getAggregations().asMap().get("uv")).getValue();
		double totalOrderAmount = (double)((InternalSum) orderResponse.getAggregations().asMap().get("orderAmount")).getValue() + freight_money;
		double totalOrderCnt = (double)((InternalCardinality) orderResponse.getAggregations().asMap().get("orderCnt")).getValue();
		double totalProductCnt = (double)((InternalSum) orderResponse.getAggregations().asMap().get("productCnt")).getValue();

		// 查询统计周期内uv、pv
		SearchResponse logonSr = EsClient.getConnect().prepareSearch("logon").setQuery(EsUtil.logonBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("clientCnt").field("agent_type").size(1000)
						.subAggregation(AggregationBuilders.cardinality("uv").field("user_id")))
				.get();
		
		// 按渠道查询订单数、订单金额、下单人数
		SearchResponse orderSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderClientAb(requestMap)).get();

		//查询运费
		SearchResponse batchSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderBatchAb(requestMap)).get();
		
		SearchResponse freightSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderFreightAb(requestMap)).get();
		
		Map<String, Aggregation> batchtMap = batchSr.getAggregations().asMap();
		StringTerms batchTerms = (StringTerms) batchtMap.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> batchBucketIt = batchTerms
				.getBuckets().iterator();
		
		Map<String, Aggregation> freightMap = freightSr.getAggregations().asMap();
		StringTerms freightTerms = (StringTerms) freightMap.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> freightBucketIt = freightTerms
				.getBuckets().iterator();
		
		while (freightBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket freightBucket = freightBucketIt
					.next();
			Terms doubleTerms = (Terms) freightBucket.getAggregations().asMap().get("freight");
			Iterator<? extends org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> bucketsIt = doubleTerms
					.getBuckets().iterator();
			while (bucketsIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket = bucketsIt.next();
				freight_money = (Double) bucket.getKeyAsNumber();
			}
			freight.put(freightBucket.getKeyAsString(), freight_money);
		}
		
		while (batchBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket batchBucket = batchBucketIt
					.next();
			StringTerms sTerms = (StringTerms) batchBucket.getAggregations().asMap().get("channelCnt");
			Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> sIt = sTerms.getBuckets().iterator();
			while (sIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket sBucket = sIt.next();
				batch.put(batchBucket.getKeyAsString(), sBucket.getKeyAsString());
			}
		}
		
		for (Map.Entry<String, Object> frei : freight.entrySet()) {
			for (Map.Entry<String, Object> bat : batch.entrySet()) {
				if (frei.getKey().equals(bat.getKey())) {
					if (batch1.containsKey(bat.getValue())) {
						batch1.put(bat.getValue().toString(), (Double)frei.getValue() + (Double)batch1.get(bat.getValue().toString()));
					} else {
						batch1.put(bat.getValue().toString(), frei.getValue());
					}
				}
			}
		}
		
		Map<String, Aggregation> logonMap = logonSr.getAggregations().asMap();

		LongTerms logonTerms = (LongTerms) logonMap.get("clientCnt");
		Iterator<Bucket> logonBucketIt = logonTerms
				.getBuckets().iterator();

		while (logonBucketIt.hasNext()) {
			Bucket logonBucket = logonBucketIt.next();
			Map<String, Aggregation> subAggMap = logonBucket.getAggregations().asMap();
			int uv = (int) ((InternalCardinality) subAggMap.get("uv")).getValue();
			logon.put(logonBucket.getKey().toString(), logonBucket.getDocCount() + "," + uv);
		}

		Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();

		LongTerms orderTerms = (LongTerms) orderMap.get("clientCnt");
		Iterator<Bucket> orderBucketIt = orderTerms
				.getBuckets().iterator();
		while (orderBucketIt.hasNext()) {
			Bucket orderBucket = orderBucketIt.next();
			Map<String, Aggregation> subAggMap = orderBucket.getAggregations().asMap();
			// sum值获取方法
			double orderAmount = ((InternalSum) subAggMap.get("orderAmount")).getValue();
			// cardinality值获取方法
			int userCnt = (int) ((InternalCardinality) subAggMap.get("userCnt")).getValue();
			// count值获取方法
			int orderCnt = (int) ((InternalValueCount) subAggMap.get("orderCnt")).getValue();
			int productCnt = (int) ((InternalSum) subAggMap.get("productCnt")).getValue();
			order.put(orderBucket.getKey().toString(), orderAmount + "," + userCnt + "," + orderCnt + "," + productCnt);
		}
		
		List<ClientCountVo> clientList = new ArrayList<ClientCountVo>();
		for (Map.Entry<String, Object> or : order.entrySet()) {
			Iterator<Entry<String, Object>> log = logon.entrySet().iterator();
			Iterator<Entry<String, Object>> bat = batch1.entrySet().iterator();
			while (log.hasNext()) {
				Entry<String, Object> logEntry = log.next();
				if (or.getKey().equals(logEntry.getKey())) {
					ClientCountVo clientVo = new ClientCountVo();
					String[] array = (or.getValue() + "," + logEntry.getValue()).toString().split(",");
					clientVo.setClient_id(Integer.valueOf(or.getKey()));
					clientVo.setUv(Integer.valueOf(array[5])); //UV
					clientVo.setOrderCnt(Integer.valueOf(array[2])); //OrderCnt
					order_amount = Double.valueOf(array[0].toString()); 
					while (bat.hasNext()) {
						Entry<String, Object> batEntry = bat.next();
						if (or.getKey().equals(batEntry.getKey())) {
							order_amount = (double) batEntry.getValue() + Double.valueOf(array[0].toString());
						} 
					}
					clientVo.setOrderAmount(df.format(order_amount)); //OrderAmount
					clientVo.setProductCnt(Integer.valueOf(array[3])); //ProductCnt
					clientVo.setOrderAmountRate(DateUtil.numFormat((order_amount) / totalOrderAmount));
					clientVo.setOrderCntRate(DateUtil.numFormat(Integer.valueOf(array[2]) / totalOrderCnt));
					clientVo.setProductCntRate(DateUtil.numFormat(Integer.valueOf(array[3]) / totalProductCnt));
					clientVo.setUvRate(DateUtil.numFormat(Integer.valueOf(array[5]) / totalUv));
					clientList.add(clientVo);
					log.remove();
				}
			}
		}
		for (Map.Entry<String, Object> vw : logon.entrySet()) {
			ClientCountVo clientVo = new ClientCountVo();
			String[] array = (vw.getValue()).toString().split(",");
			clientVo.setClient_id(Integer.valueOf(vw.getKey()));
			clientVo.setUv(Integer.valueOf(array[1]));
			clientVo.setOrderCnt(0);
			clientVo.setOrderCntRate("0.00");
			clientVo.setOrderAmount("0.00");
			clientVo.setOrderAmountRate("0.00");
			clientVo.setProductCnt(0);
			clientVo.setProductCntRate("0.00");
			clientVo.setUvRate(DateUtil.numFormat(Integer.valueOf(array[1]) /totalUv));
			clientList.add(clientVo);
		}
		return clientList;
	}

	@Override
	public Map<String, Object> getUserDataByDate(Map<String, Object> requestMap) {
		Map<String, Object> resultMap = new HashMap<String, Object>();
		
		//查询统计周期内uv、pv
		SearchResponse logonSr = EsClient.getConnect().prepareSearch("logon")
				.setQuery(EsUtil.logonBqb(requestMap))
				.addAggregation(AggregationBuilders.cardinality("uv").field("user_id"))
				.get();
		
		//查询购买用户数
		SearchResponse orderSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.cardinality("orderUser").field("user_id"))
				.get();
		
		resultMap.put("uv", ((InternalCardinality)logonSr.getAggregations().asMap().get("uv")).getValue());
		resultMap.put("orderUser", ((InternalCardinality)orderSr.getAggregations().asMap().get("orderUser")).getValue());
		
		return resultMap;
	}

	@Override
	public List<UserDistributionVo> getUserDistributionByDate(Map<String, Object> requestMap) {
		List<UserDistributionVo> userList = new ArrayList<UserDistributionVo>();
		
		//查询各个省份购买用户数
		SearchResponse orderSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderRegionAb(requestMap))
				.get();
		
		//查询全部购买用户数
		SearchResponse totalSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.cardinality("userCnt").field("user_id"))
				.get();
		
		int totalUser = (int)((InternalCardinality)totalSr.getAggregations().asMap().get("userCnt")).getValue();
		Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();
		LongTerms orderTerms = (LongTerms) orderMap.get("provinceId");
		Iterator<Bucket> orderIt = orderTerms.getBuckets().iterator();
		while (orderIt.hasNext()) {
			UserDistributionVo userVo = new UserDistributionVo();
			Bucket orderBucket = orderIt.next();
			userVo.setRegionId(Integer.valueOf(orderBucket.getKeyAsString()));
			int orderUser = (int)((InternalCardinality)orderBucket.getAggregations().get("userCnt")).getValue();
			userVo.setOrderUser(orderUser);
			userVo.setOrderUserRate(DateUtil.numFormat(orderUser/ Double.valueOf(totalUser)));
			userList.add(userVo);
		}
		return userList;
	}

	@Override
	public List<UserPayWayVo> getUserPayWayByDate(Map<String, Object> requestMap) {
		List<UserPayWayVo> userList = new ArrayList<UserPayWayVo>();
		
		//查询各个省份购买用户数
		SearchResponse orderSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderPayAb(requestMap))
				.get();
		
		//查询全部购买用户数
		SearchResponse totalSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.cardinality("userCnt").field("user_id"))
				.get();
		
		double totalUser = Double.valueOf(((InternalCardinality)totalSr.getAggregations().asMap().get("userCnt")).getValueAsString());
		Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();
		StringTerms orderTerms = (StringTerms) orderMap.get("payWay");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> orderIt = orderTerms.getBuckets().iterator();
		while (orderIt.hasNext()) {
			UserPayWayVo userVo = new UserPayWayVo();
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket orderBucket = orderIt.next();
			userVo.setPay_way(orderBucket.getKeyAsString());
			int userCnt = (int)((InternalCardinality)orderBucket.getAggregations().get("userCnt")).getValue();
			userVo.setOrderUser(userCnt);
			userVo.setOrderUserRate(DateUtil.numFormat(userCnt / totalUser));
			userList.add(userVo);
		}
		return userList;
	}

	@Override
	public List<BestSellingVo> getBestSellingByDate(Map<String, Object> requestMap) {
		List<BestSellingVo> sellingList = new ArrayList<BestSellingVo>();
		Map<String, Object> order = new HashMap<String, Object>();
		Map<String, Object> userView = new HashMap<String, Object>();
		
		//查询各个产品销售数量、销售金额、运费
		SearchResponse orderSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderSellingAb(requestMap))
				.get();
		
		//查询各个产品的uv
		SearchResponse uvSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderViewBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("productId").field("product_id")
						.subAggregation(AggregationBuilders.cardinality("uv").field("user_id")))
				.get();
		
		Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();
		LongTerms orderTerms = (LongTerms) orderMap.get("productId");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.LongTerms.Bucket> orderIt = orderTerms.getBuckets().iterator();
		while (orderIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.LongTerms.Bucket orderBucket = orderIt.next();
			Map<String, Aggregation> subAggMap = orderBucket.getAggregations().asMap();
			int productCnt = (int)((InternalSum)subAggMap.get("productCnt")).getValue(); 
			double orderAmount = (double)((InternalSum)subAggMap.get("orderAmount")).getValue() + (double)((InternalSum)subAggMap.get("freightAmount")).getValue(); 
			order.put(orderBucket.getKeyAsString(), productCnt + "," + orderAmount);
		}
		
		Map<String, Aggregation> uvMap = uvSr.getAggregations().asMap();
		LongTerms uvTerms = (LongTerms) uvMap.get("productId");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.LongTerms.Bucket> uvIt = uvTerms.getBuckets().iterator();
		while (uvIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.LongTerms.Bucket orderBucket = uvIt.next();
			Map<String, Aggregation> subAggMap = orderBucket.getAggregations().asMap();
			int uv = (int)((InternalCardinality)subAggMap.get("uv")).getValue(); 
			userView.put(orderBucket.getKeyAsString(), uv);
		}
		
		for (Map.Entry<String, Object> or : order.entrySet()) {
			for (Map.Entry<String, Object> uv : userView.entrySet()) {
				if (or.getKey().equals(uv.getKey())) {
					BestSellingVo sellingVo = new BestSellingVo();
					String []str = or.getValue().toString().split(",");
					sellingVo.setProductId(Integer.valueOf(or.getKey()));
					sellingVo.setProductCnt(Integer.valueOf(str[0]));
					sellingVo.setOrderAmount(str[1]);
					sellingVo.setUv(Integer.valueOf(uv.getValue().toString()));
					sellingList.add(sellingVo);
				}
			}
		}
		return sellingList;
	}

	@Override
	public Map<String, Object> getProductData(Map<String, Object> requestMap) {
		double freight_money = 0;
		Map<String, Object> resultMap = new HashMap<String, Object>();

		// 查询统计周期内销售金额、成功订单数、累计用户数（以手机号为维度的购买人即为用户）
		SearchResponse orderResponse = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.sum("orderAmount").field("order_amount"))//累计销售额
				.addAggregation(AggregationBuilders.cardinality("orderCnt").field("batch_id")) //订单数
				.addAggregation(AggregationBuilders.cardinality("orderUser").field("user_id")) //购买人数
				.addAggregation(AggregationBuilders.sum("productCnt").field("product_num"))//销售数量
				.get(); 
		
		//uv
		SearchResponse viewResponse = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderViewBqb(requestMap))
				.addAggregation(AggregationBuilders.cardinality("uv").field("user_id"))
				.get();
		
		// 查询运费
		SearchResponse freightSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("batch_id").field("batch_id").size(10000)
						.subAggregation(AggregationBuilders.terms("freight").field("freight_money"))
						.subAggregation(AggregationBuilders.sum("freightAmount").field("freight_money")))
				.get();

		Map<String, Aggregation> freightMap = freightSr.getAggregations().asMap();
		StringTerms freightTerms = (StringTerms) freightMap.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> freightBucketIt = freightTerms
				.getBuckets().iterator();

		while (freightBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket freightBucket = freightBucketIt
					.next();
			Terms doubleTerms = (Terms) freightBucket.getAggregations().asMap().get("freight");
			Iterator<? extends org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> bucketsIt = doubleTerms
					.getBuckets().iterator();
			while (bucketsIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket = bucketsIt.next();
				freight_money = freight_money + (Double) bucket.getKeyAsNumber();
			}
		}

		DecimalFormat df = new DecimalFormat("0.00");
		int uv = (int)((InternalCardinality) viewResponse.getAggregations().asMap().get("uv")).getValue();
		int orderUser = (int)((InternalCardinality) orderResponse.getAggregations().asMap().get("orderUser")).getValue();
		resultMap.put("uv", uv);
		resultMap.put("orderAmount", df.format(((InternalSum) orderResponse.getAggregations().asMap().get("orderAmount")).getValue() + freight_money));
		resultMap.put("orderCnt", ((InternalCardinality) orderResponse.getAggregations().asMap().get("orderCnt")).getValue());
		resultMap.put("orderUser", orderUser);
		resultMap.put("productCnt", ((InternalSum) orderResponse.getAggregations().asMap().get("productCnt")).getValue());
		resultMap.put("successConvertRate", DateUtil.numFormat(orderUser / Double.valueOf(uv)));
		return resultMap;
	}

	@Override
	public List<DataSourceVo> getDataSourceByProductId(Map<String, Object> requestMap) {
		DecimalFormat df = new DecimalFormat("0.00");
		double order_amount = 0;
		double freight_money = 0;
		Map<String, Object> freight = new HashMap<String, Object>();
		Map<String, Object> batch = new HashMap<String, Object>();
		Map<String, Object> batch1 = new HashMap<String, Object>();
		Map<String, Object> logon = new HashMap<String, Object>();
		Map<String, Object> order = new HashMap<String, Object>();
		
		//uv
		SearchResponse viewResponse = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderViewBqb(requestMap))
				.addAggregation(AggregationBuilders.cardinality("uv").field("user_id"))
				.get();

		// 查询统计周期内销售金额、成功订单数、累计用户数（以手机号为维度的购买人即为用户）
		SearchResponse orderResponse = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.sum("orderAmount").field("order_amount")) 
				.addAggregation(AggregationBuilders.cardinality("orderCnt").field("batch_id")) 
				.addAggregation(AggregationBuilders.cardinality("orderUser").field("user_id"))
				.addAggregation(AggregationBuilders.sum("productCnt").field("product_num"))
				.get();
		
		// 查询运费
		SearchResponse freightResponse = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("batch_id").field("batch_id").size(10000)
						.subAggregation(AggregationBuilders.terms("freight").field("freight_money"))
						.subAggregation(AggregationBuilders.sum("freightAmount").field("freight_money")))
				.get();

		Map<String, Aggregation> freightMap1 = freightResponse.getAggregations().asMap();
		StringTerms freightTerms1 = (StringTerms) freightMap1.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> freightBucketIt1 = freightTerms1
				.getBuckets().iterator();

		while (freightBucketIt1.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket freightBucket = freightBucketIt1
					.next();
			Terms doubleTerms = (Terms) freightBucket.getAggregations().asMap().get("freight");
			Iterator<? extends org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> bucketsIt = doubleTerms
					.getBuckets().iterator();
			while (bucketsIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket = bucketsIt.next();
				freight_money = freight_money + (Double) bucket.getKeyAsNumber();
			}
		}
		double totalUv =  (double)((InternalCardinality) viewResponse.getAggregations().asMap().get("uv")).getValue();
		double totalOrderAmount = (double)((InternalSum) orderResponse.getAggregations().asMap().get("orderAmount")).getValue() + freight_money;
		double totalOrderCnt = (double)((InternalCardinality) orderResponse.getAggregations().asMap().get("orderCnt")).getValue();
		double totalProductCnt = (double)((InternalSum) orderResponse.getAggregations().asMap().get("productCnt")).getValue();

		// 查询统计周期内uv、pv
		SearchResponse viewSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderViewBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("channelCnt").field("channel_id")
						.subAggregation(AggregationBuilders.cardinality("uv").field("user_id")))
				.get();
		
		// 按渠道查询订单数、订单金额、下单人数
		SearchResponse orderSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderChannelAb(requestMap)).get();

		//查询运费
		SearchResponse batchSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderBatchAb(requestMap)).get();
		
		SearchResponse freightSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderFreightAb(requestMap)).get();
		
		Map<String, Aggregation> batchtMap = batchSr.getAggregations().asMap();
		StringTerms batchTerms = (StringTerms) batchtMap.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> batchBucketIt = batchTerms
				.getBuckets().iterator();
		
		Map<String, Aggregation> freightMap = freightSr.getAggregations().asMap();
		StringTerms freightTerms = (StringTerms) freightMap.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> freightBucketIt = freightTerms
				.getBuckets().iterator();
		
		while (freightBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket freightBucket = freightBucketIt
					.next();
			Terms doubleTerms = (Terms) freightBucket.getAggregations().asMap().get("freight");
			Iterator<? extends org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> bucketsIt = doubleTerms
					.getBuckets().iterator();
			while (bucketsIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket = bucketsIt.next();
				freight_money = (Double) bucket.getKeyAsNumber();
			}
			freight.put(freightBucket.getKeyAsString(), freight_money);
		}
		
		while (batchBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket batchBucket = batchBucketIt
					.next();
			StringTerms sTerms = (StringTerms) batchBucket.getAggregations().asMap().get("channelCnt");
			Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> sIt = sTerms.getBuckets().iterator();
			while (sIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket sBucket = sIt.next();
				batch.put(batchBucket.getKeyAsString(), sBucket.getKeyAsString());
			}
		}
		
		for (Map.Entry<String, Object> frei : freight.entrySet()) {
			for (Map.Entry<String, Object> bat : batch.entrySet()) {
				if (frei.getKey().equals(bat.getKey())) {
					if (batch1.containsKey(bat.getValue())) {
						batch1.put(bat.getValue().toString(), (Double)frei.getValue() + (Double)batch1.get(bat.getValue().toString()));
					} else {
						batch1.put(bat.getValue().toString(), frei.getValue());
					}
				}
			}
		}
		
		Map<String, Aggregation> logonMap = viewSr.getAggregations().asMap();

		StringTerms logonTerms = (StringTerms) logonMap.get("channelCnt");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> logonBucketIt = logonTerms
				.getBuckets().iterator();

		while (logonBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket logonBucket = logonBucketIt.next();
			Map<String, Aggregation> subAggMap = logonBucket.getAggregations().asMap();
			int uv = (int) ((InternalCardinality) subAggMap.get("uv")).getValue();
			logon.put(logonBucket.getKey().toString(), logonBucket.getDocCount() + "," + uv);
		}

		Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();

		StringTerms orderTerms = (StringTerms) orderMap.get("channelCnt");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> orderBucketIt = orderTerms
				.getBuckets().iterator();
		while (orderBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket orderBucket = orderBucketIt.next();
			Map<String, Aggregation> subAggMap = orderBucket.getAggregations().asMap();
			// sum值获取方法
			double orderAmount = ((InternalSum) subAggMap.get("orderAmount")).getValue();
			// cardinality值获取方法
			int userCnt = (int) ((InternalCardinality) subAggMap.get("userCnt")).getValue();
			// count值获取方法
			int orderCnt = (int) ((InternalValueCount) subAggMap.get("orderCnt")).getValue();
			int productCnt = (int) ((InternalSum) subAggMap.get("productCnt")).getValue();
			order.put(orderBucket.getKey().toString(), orderAmount + "," + userCnt + "," + orderCnt + "," + productCnt);
		}
		
		List<DataSourceVo> sourceList = new ArrayList<DataSourceVo>();
		for (Map.Entry<String, Object> or : order.entrySet()) {
			Iterator<Entry<String, Object>> log = logon.entrySet().iterator();
			Iterator<Entry<String, Object>> bat = batch1.entrySet().iterator();
			while (log.hasNext()) {
				Entry<String, Object> logEntry = log.next();
				if (or.getKey().equals(logEntry.getKey())) {
					DataSourceVo sourceVo = new DataSourceVo();
					String[] array = (or.getValue() + "," + logEntry.getValue()).toString().split(",");
					sourceVo.setChannel_id(or.getKey());
					sourceVo.setUv(Integer.valueOf(array[5])); //UV
					sourceVo.setOrderCnt(Integer.valueOf(array[2])); //OrderCnt
					order_amount = Double.valueOf(array[0].toString()); 
					while (bat.hasNext()) {
						Entry<String, Object> batEntry = bat.next();
						if (or.getKey().equals(batEntry.getKey())) {
							order_amount = (double) batEntry.getValue() + Double.valueOf(array[0].toString());
						} 
					}
					sourceVo.setOrderAmount(df.format(order_amount)); //OrderAmount
					sourceVo.setProductCnt(Integer.valueOf(array[3])); //ProductCnt
					sourceVo.setOrderAmountRate(DateUtil.numFormat((order_amount) / totalOrderAmount));
					sourceVo.setOrderCntRate(DateUtil.numFormat(Integer.valueOf(array[2]) / totalOrderCnt));
					sourceVo.setProductCntRate(DateUtil.numFormat(Integer.valueOf(array[3]) / totalProductCnt));
					sourceVo.setUvRate(DateUtil.numFormat(Integer.valueOf(array[5]) / totalUv));
					sourceList.add(sourceVo);
					log.remove();
				}
			}
		}
		for (Map.Entry<String, Object> vw : logon.entrySet()) {
			DataSourceVo sourceVo = new DataSourceVo();
			String[] array = (vw.getValue()).toString().split(",");
			sourceVo.setChannel_id(vw.getKey());
			sourceVo.setUv(Integer.valueOf(array[1]));
			sourceVo.setOrderCnt(0);
			sourceVo.setOrderCntRate("0.00");
			sourceVo.setOrderAmount("0.00");
			sourceVo.setOrderAmountRate("0.00");
			sourceVo.setProductCnt(0);
			sourceVo.setProductCntRate("0.00");
			sourceVo.setUvRate(DateUtil.numFormat(Integer.valueOf(array[1]) /totalUv));
			sourceList.add(sourceVo);
		}
		return sourceList;
	}

	@Override
	public List<RegionDistributionVo> getRegionDistributionByProductId(Map<String, Object> requestMap) {
		DecimalFormat df = new DecimalFormat("0.00");
		double order_amount = 0;
		double freight_money = 0;
		Map<String, Object> freight = new HashMap<String, Object>();
		Map<String, Object> batch = new HashMap<String, Object>();
		Map<String, Object> batch1 = new HashMap<String, Object>();
		Map<String, Object> logon = new HashMap<String, Object>();
		Map<String, Object> order = new HashMap<String, Object>();
		
		// 查询统计周期内uv、pv
		SearchResponse viewResponse = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderViewBqb(requestMap))
				.addAggregation(AggregationBuilders.cardinality("uv").field("user_id"))
				.get();

		// 查询统计周期内销售金额、成功订单数、累计用户数（以手机号为维度的购买人即为用户）
		SearchResponse orderResponse = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.sum("orderAmount").field("order_amount")) 
				.addAggregation(AggregationBuilders.cardinality("orderCnt").field("batch_id")) 
				.addAggregation(AggregationBuilders.cardinality("orderUser").field("user_id"))
				.addAggregation(AggregationBuilders.sum("productCnt").field("product_num"))
				.get();
		
		// 查询运费
		SearchResponse freightResponse = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("batch_id").field("batch_id").size(10000)
						.subAggregation(AggregationBuilders.terms("freight").field("freight_money"))
						.subAggregation(AggregationBuilders.sum("freightAmount").field("freight_money")))
				.get();

		Map<String, Aggregation> freightMap1 = freightResponse.getAggregations().asMap();
		StringTerms freightTerms1 = (StringTerms) freightMap1.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> freightBucketIt1 = freightTerms1
				.getBuckets().iterator();

		while (freightBucketIt1.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket freightBucket = freightBucketIt1
					.next();
			Terms doubleTerms = (Terms) freightBucket.getAggregations().asMap().get("freight");
			Iterator<? extends org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> bucketsIt = doubleTerms
					.getBuckets().iterator();
			while (bucketsIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket = bucketsIt.next();
				freight_money = freight_money + (Double) bucket.getKeyAsNumber();
			}
		}
		double totalUv =  (double)((InternalCardinality) viewResponse.getAggregations().asMap().get("uv")).getValue();
		double totalOrderAmount = (double)((InternalSum) orderResponse.getAggregations().asMap().get("orderAmount")).getValue() + freight_money;
		double totalOrderCnt = (double)((InternalCardinality) orderResponse.getAggregations().asMap().get("orderCnt")).getValue();
		double totalProductCnt = (double)((InternalSum) orderResponse.getAggregations().asMap().get("productCnt")).getValue();

		// 查询统计周期内uv、pv
		SearchResponse viewSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderViewBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("provinceId").field("province_id")
						.subAggregation(AggregationBuilders.cardinality("uv").field("user_id")))
				.get();
		
		// 按渠道查询订单数、订单金额、下单人数
		SearchResponse orderSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderRegionAb(requestMap)).get();

		//查询运费
		SearchResponse batchSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderBatchAb(requestMap)).get();
		
		SearchResponse freightSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderFreightAb(requestMap)).get();
		
		Map<String, Aggregation> batchtMap = batchSr.getAggregations().asMap();
		StringTerms batchTerms = (StringTerms) batchtMap.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> batchBucketIt = batchTerms
				.getBuckets().iterator();
		
		Map<String, Aggregation> freightMap = freightSr.getAggregations().asMap();
		StringTerms freightTerms = (StringTerms) freightMap.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> freightBucketIt = freightTerms
				.getBuckets().iterator();
		
		while (freightBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket freightBucket = freightBucketIt
					.next();
			Terms doubleTerms = (Terms) freightBucket.getAggregations().asMap().get("freight");
			Iterator<? extends org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> bucketsIt = doubleTerms
					.getBuckets().iterator();
			while (bucketsIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket = bucketsIt.next();
				freight_money = (Double) bucket.getKeyAsNumber();
			}
			freight.put(freightBucket.getKeyAsString(), freight_money);
		}
		
		while (batchBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket batchBucket = batchBucketIt
					.next();
			StringTerms sTerms = (StringTerms) batchBucket.getAggregations().asMap().get("channelCnt");
			Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> sIt = sTerms.getBuckets().iterator();
			while (sIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket sBucket = sIt.next();
				batch.put(batchBucket.getKeyAsString(), sBucket.getKeyAsString());
			}
		}
		
		for (Map.Entry<String, Object> frei : freight.entrySet()) {
			for (Map.Entry<String, Object> bat : batch.entrySet()) {
				if (frei.getKey().equals(bat.getKey())) {
					if (batch1.containsKey(bat.getValue())) {
						batch1.put(bat.getValue().toString(), (Double)frei.getValue() + (Double)batch1.get(bat.getValue().toString()));
					} else {
						batch1.put(bat.getValue().toString(), frei.getValue());
					}
				}
			}
		}
		
		Map<String, Aggregation> logonMap = viewSr.getAggregations().asMap();

		LongTerms logonTerms = (LongTerms) logonMap.get("provinceId");
		Iterator<Bucket> logonBucketIt = logonTerms
				.getBuckets().iterator();

		while (logonBucketIt.hasNext()) {
			Bucket logonBucket = logonBucketIt.next();
			Map<String, Aggregation> subAggMap = logonBucket.getAggregations().asMap();
			int uv = (int) ((InternalCardinality) subAggMap.get("uv")).getValue();
			logon.put(logonBucket.getKey().toString(), logonBucket.getDocCount() + "," + uv);
		}

		Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();

		LongTerms orderTerms = (LongTerms) orderMap.get("provinceId");
		Iterator<Bucket> orderBucketIt = orderTerms
				.getBuckets().iterator();
		while (orderBucketIt.hasNext()) {
			Bucket orderBucket = orderBucketIt.next();
			Map<String, Aggregation> subAggMap = orderBucket.getAggregations().asMap();
			// sum值获取方法
			double orderAmount = ((InternalSum) subAggMap.get("orderAmount")).getValue();
			// cardinality值获取方法
			int userCnt = (int) ((InternalCardinality) subAggMap.get("userCnt")).getValue();
			// count值获取方法
			int orderCnt = (int) ((InternalValueCount) subAggMap.get("orderCnt")).getValue();
			int productCnt = (int) ((InternalSum) subAggMap.get("productCnt")).getValue();
			order.put(orderBucket.getKey().toString(), orderAmount + "," + userCnt + "," + orderCnt + "," + productCnt);
		}
		
		List<RegionDistributionVo> regionList = new ArrayList<RegionDistributionVo>();
		for (Map.Entry<String, Object> or : order.entrySet()) {
			Iterator<Entry<String, Object>> log = logon.entrySet().iterator();
			Iterator<Entry<String, Object>> bat = batch1.entrySet().iterator();
			while (log.hasNext()) {
				Entry<String, Object> logEntry = log.next();
				if (or.getKey().equals(logEntry.getKey())) {
					RegionDistributionVo regionVo = new RegionDistributionVo();
					String[] array = (or.getValue() + "," + logEntry.getValue()).toString().split(",");
					regionVo.setRegionId(Integer.valueOf(or.getKey()));
					regionVo.setUv(Integer.valueOf(array[5])); //UV
					regionVo.setOrderCnt(Integer.valueOf(array[2])); //OrderCnt
					order_amount = Double.valueOf(array[0].toString()); 
					while (bat.hasNext()) {
						Entry<String, Object> batEntry = bat.next();
						if (or.getKey().equals(batEntry.getKey())) {
							order_amount = (double) batEntry.getValue() + Double.valueOf(array[0].toString());
						} 
					}
					regionVo.setOrderAmount(df.format(order_amount)); //OrderAmount
					regionVo.setProductCnt(Integer.valueOf(array[3])); //ProductCnt
					regionVo.setOrderAmountRate(DateUtil.numFormat((order_amount) / totalOrderAmount));
					regionVo.setOrderCntRate(DateUtil.numFormat(Integer.valueOf(array[2]) / totalOrderCnt));
					regionVo.setProductCntRate(DateUtil.numFormat(Integer.valueOf(array[3]) / totalProductCnt));
					regionVo.setUvRate(DateUtil.numFormat(Integer.valueOf(array[5]) / totalUv));
					regionList.add(regionVo);
					log.remove();
				}
			}
		}
		for (Map.Entry<String, Object> vw : logon.entrySet()) {
			RegionDistributionVo regionVo = new RegionDistributionVo();
			String[] array = (vw.getValue()).toString().split(",");
			regionVo.setRegionId(Integer.valueOf(vw.getKey()));
			regionVo.setUv(Integer.valueOf(array[1]));
			regionVo.setOrderCnt(0);
			regionVo.setOrderCntRate("0.00");
			regionVo.setOrderAmount("0.00");
			regionVo.setOrderAmountRate("0.00");
			regionVo.setProductCnt(0);
			regionVo.setProductCntRate("0.00");
			regionVo.setUvRate(DateUtil.numFormat(Integer.valueOf(array[1]) /totalUv));
			regionList.add(regionVo);
		}
		return regionList;
	}

	@Override
	public List<ClientCountVo> getClientDataByProductId(Map<String, Object> requestMap) {
		DecimalFormat df = new DecimalFormat("0.00");
		double order_amount = 0;
		double freight_money = 0;
		Map<String, Object> freight = new HashMap<String, Object>();
		Map<String, Object> batch = new HashMap<String, Object>();
		Map<String, Object> batch1 = new HashMap<String, Object>();
		Map<String, Object> logon = new HashMap<String, Object>();
		Map<String, Object> order = new HashMap<String, Object>();
		
		// 查询统计周期内uv、pv
		SearchResponse viewResponse = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderViewBqb(requestMap))
				.addAggregation(AggregationBuilders.cardinality("uv").field("user_id"))
				.get();

		// 查询统计周期内销售金额、成功订单数、累计用户数（以手机号为维度的购买人即为用户）
		SearchResponse orderResponse = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.sum("orderAmount").field("order_amount")) 
				.addAggregation(AggregationBuilders.cardinality("orderCnt").field("batch_id")) 
				.addAggregation(AggregationBuilders.cardinality("orderUser").field("user_id"))
				.addAggregation(AggregationBuilders.sum("productCnt").field("product_num"))
				.get();
		
		// 查询运费
		SearchResponse freightResponse = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("batch_id").field("batch_id").size(10000)
						.subAggregation(AggregationBuilders.terms("freight").field("freight_money"))
						.subAggregation(AggregationBuilders.sum("freightAmount").field("freight_money")))
				.get();

		Map<String, Aggregation> freightMap1 = freightResponse.getAggregations().asMap();
		StringTerms freightTerms1 = (StringTerms) freightMap1.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> freightBucketIt1 = freightTerms1
				.getBuckets().iterator();

		while (freightBucketIt1.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket freightBucket = freightBucketIt1
					.next();
			Terms doubleTerms = (Terms) freightBucket.getAggregations().asMap().get("freight");
			Iterator<? extends org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> bucketsIt = doubleTerms
					.getBuckets().iterator();
			while (bucketsIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket = bucketsIt.next();
				freight_money = freight_money + (Double) bucket.getKeyAsNumber();
			}
		}
		double totalUv =  (double)((InternalCardinality) viewResponse.getAggregations().asMap().get("uv")).getValue();
		double totalOrderAmount = (double)((InternalSum) orderResponse.getAggregations().asMap().get("orderAmount")).getValue() + freight_money;
		double totalOrderCnt = (double)((InternalCardinality) orderResponse.getAggregations().asMap().get("orderCnt")).getValue();
		double totalProductCnt = (double)((InternalSum) orderResponse.getAggregations().asMap().get("productCnt")).getValue();

		// 查询统计周期内uv、pv
		SearchResponse viewSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderViewBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("clientCnt").field("agent_type")
						.subAggregation(AggregationBuilders.cardinality("uv").field("user_id")))
				.get();
		
		// 按渠道查询订单数、订单金额、下单人数
		SearchResponse orderSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderClientAb(requestMap)).get();

		//查询运费
		SearchResponse batchSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderBatchAb(requestMap)).get();
		
		SearchResponse freightSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderFreightAb(requestMap)).get();
		
		Map<String, Aggregation> batchtMap = batchSr.getAggregations().asMap();
		StringTerms batchTerms = (StringTerms) batchtMap.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> batchBucketIt = batchTerms
				.getBuckets().iterator();
		
		Map<String, Aggregation> freightMap = freightSr.getAggregations().asMap();
		StringTerms freightTerms = (StringTerms) freightMap.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> freightBucketIt = freightTerms
				.getBuckets().iterator();
		
		while (freightBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket freightBucket = freightBucketIt
					.next();
			Terms doubleTerms = (Terms) freightBucket.getAggregations().asMap().get("freight");
			Iterator<? extends org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> bucketsIt = doubleTerms
					.getBuckets().iterator();
			while (bucketsIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket = bucketsIt.next();
				freight_money = (Double) bucket.getKeyAsNumber();
			}
			freight.put(freightBucket.getKeyAsString(), freight_money);
		}
		
		while (batchBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket batchBucket = batchBucketIt
					.next();
			StringTerms sTerms = (StringTerms) batchBucket.getAggregations().asMap().get("channelCnt");
			Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> sIt = sTerms.getBuckets().iterator();
			while (sIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket sBucket = sIt.next();
				batch.put(batchBucket.getKeyAsString(), sBucket.getKeyAsString());
			}
		}
		
		for (Map.Entry<String, Object> frei : freight.entrySet()) {
			for (Map.Entry<String, Object> bat : batch.entrySet()) {
				if (frei.getKey().equals(bat.getKey())) {
					if (batch1.containsKey(bat.getValue())) {
						batch1.put(bat.getValue().toString(), (Double)frei.getValue() + (Double)batch1.get(bat.getValue().toString()));
					} else {
						batch1.put(bat.getValue().toString(), frei.getValue());
					}
				}
			}
		}
		
		Map<String, Aggregation> logonMap = viewSr.getAggregations().asMap();

		LongTerms logonTerms = (LongTerms) logonMap.get("clientCnt");
		Iterator<Bucket> logonBucketIt = logonTerms
				.getBuckets().iterator();

		while (logonBucketIt.hasNext()) {
			Bucket logonBucket = logonBucketIt.next();
			Map<String, Aggregation> subAggMap = logonBucket.getAggregations().asMap();
			int uv = (int) ((InternalCardinality) subAggMap.get("uv")).getValue();
			logon.put(logonBucket.getKey().toString(), logonBucket.getDocCount() + "," + uv);
		}

		Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();

		LongTerms orderTerms = (LongTerms) orderMap.get("clientCnt");
		Iterator<Bucket> orderBucketIt = orderTerms
				.getBuckets().iterator();
		while (orderBucketIt.hasNext()) {
			Bucket orderBucket = orderBucketIt.next();
			Map<String, Aggregation> subAggMap = orderBucket.getAggregations().asMap();
			// sum值获取方法
			double orderAmount = ((InternalSum) subAggMap.get("orderAmount")).getValue();
			// cardinality值获取方法
			int userCnt = (int) ((InternalCardinality) subAggMap.get("userCnt")).getValue();
			// count值获取方法
			int orderCnt = (int) ((InternalValueCount) subAggMap.get("orderCnt")).getValue();
			int productCnt = (int) ((InternalSum) subAggMap.get("productCnt")).getValue();
			order.put(orderBucket.getKey().toString(), orderAmount + "," + userCnt + "," + orderCnt + "," + productCnt);
		}
		
		List<ClientCountVo> clientList = new ArrayList<ClientCountVo>();
		for (Map.Entry<String, Object> or : order.entrySet()) {
			Iterator<Entry<String, Object>> log = logon.entrySet().iterator();
			Iterator<Entry<String, Object>> bat = batch1.entrySet().iterator();
			while (log.hasNext()) {
				Entry<String, Object> logEntry = log.next();
				if (or.getKey().equals(logEntry.getKey())) {
					ClientCountVo clientVo = new ClientCountVo();
					String[] array = (or.getValue() + "," + logEntry.getValue()).toString().split(",");
					clientVo.setClient_id(Integer.valueOf(or.getKey()));
					clientVo.setUv(Integer.valueOf(array[5])); //UV
					clientVo.setOrderCnt(Integer.valueOf(array[2])); //OrderCnt
					order_amount = Double.valueOf(array[0].toString()); 
					while (bat.hasNext()) {
						Entry<String, Object> batEntry = bat.next();
						if (or.getKey().equals(batEntry.getKey())) {
							order_amount = (double) batEntry.getValue() + Double.valueOf(array[0].toString());
						} 
					}
					clientVo.setOrderAmount(df.format(order_amount)); //OrderAmount
					clientVo.setProductCnt(Integer.valueOf(array[3])); //ProductCnt
					clientVo.setOrderAmountRate(DateUtil.numFormat((order_amount) / totalOrderAmount));
					clientVo.setOrderCntRate(DateUtil.numFormat(Integer.valueOf(array[2]) / totalOrderCnt));
					clientVo.setProductCntRate(DateUtil.numFormat(Integer.valueOf(array[3]) / totalProductCnt));
					clientVo.setUvRate(DateUtil.numFormat(Integer.valueOf(array[5]) / totalUv));
					clientList.add(clientVo);
					log.remove();
				}
			}
		}
		for (Map.Entry<String, Object> vw : logon.entrySet()) {
			ClientCountVo clientVo = new ClientCountVo();
			String[] array = (vw.getValue()).toString().split(",");
			clientVo.setClient_id(Integer.valueOf(vw.getKey()));
			clientVo.setUv(Integer.valueOf(array[1]));
			clientVo.setOrderCnt(0);
			clientVo.setOrderCntRate("0.00");
			clientVo.setOrderAmount("0.00");
			clientVo.setOrderAmountRate("0.00");
			clientVo.setProductCnt(0);
			clientVo.setProductCntRate("0.00");
			clientVo.setUvRate(DateUtil.numFormat(Integer.valueOf(array[1]) /totalUv));
			clientList.add(clientVo);
		}
		return clientList;
	}

	@Override
	public List<ProductDataVo> getSourceDataByChannelId(Map<String, Object> requestMap) {
		DecimalFormat df = new DecimalFormat("0.00");
		double order_amount = 0;
		double freight_money = 0;
		Map<String, Object> freight = new HashMap<String, Object>();
		Map<String, Object> batch = new HashMap<String, Object>();
		Map<String, Object> batch1 = new HashMap<String, Object>();
		Map<String, Object> logon = new HashMap<String, Object>();
		Map<String, Object> order = new HashMap<String, Object>();
		
		// 查询统计周期内uv、pv
		SearchResponse logonResponse = EsClient.getConnect().prepareSearch("logon")
				.setQuery(EsUtil.logonBqb(requestMap))
				.addAggregation(AggregationBuilders.cardinality("uv").field("user_id"))
				.get();

		// 查询统计周期内销售金额、成功订单数、累计用户数（以手机号为维度的购买人即为用户）
		SearchResponse orderResponse = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.sum("orderAmount").field("order_amount")) 
				.addAggregation(AggregationBuilders.cardinality("orderCnt").field("batch_id")) 
				.addAggregation(AggregationBuilders.cardinality("orderUser").field("user_id"))
				.addAggregation(AggregationBuilders.sum("productCnt").field("product_num"))
				.get();
		
		// 查询运费
		SearchResponse freightResponse = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("batch_id").field("batch_id").size(10000)
						.subAggregation(AggregationBuilders.terms("freight").field("freight_money"))
						.subAggregation(AggregationBuilders.sum("freightAmount").field("freight_money")))
				.get();

		Map<String, Aggregation> freightMap1 = freightResponse.getAggregations().asMap();
		StringTerms freightTerms1 = (StringTerms) freightMap1.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> freightBucketIt1 = freightTerms1
				.getBuckets().iterator();

		while (freightBucketIt1.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket freightBucket = freightBucketIt1
					.next();
			Terms doubleTerms = (Terms) freightBucket.getAggregations().asMap().get("freight");
			Iterator<? extends org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> bucketsIt = doubleTerms
					.getBuckets().iterator();
			while (bucketsIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket = bucketsIt.next();
				freight_money = freight_money + (Double) bucket.getKeyAsNumber();
			}
		}
		double totalUv =  (double)((InternalCardinality) logonResponse.getAggregations().asMap().get("uv")).getValue();
		double totalOrderAmount = (double)((InternalSum) orderResponse.getAggregations().asMap().get("orderAmount")).getValue() + freight_money;
		double totalOrderCnt = (double)((InternalCardinality) orderResponse.getAggregations().asMap().get("orderCnt")).getValue();
		double totalProductCnt = (double)((InternalSum) orderResponse.getAggregations().asMap().get("productCnt")).getValue();

		// 查询统计周期内uv、pv
		SearchResponse viewSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderViewBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("productId").field("product_id").size(10000)
						.subAggregation(AggregationBuilders.cardinality("uv").field("user_id")))
				.get();
		
		// 按渠道查询订单数、订单金额、下单人数
		SearchResponse orderSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("productId").field("product_id").order(BucketOrder.aggregation("orderAmount", false)).size(10000)
						.subAggregation(AggregationBuilders.count("orderCnt").field("order_id"))
						.subAggregation(AggregationBuilders.sum("orderAmount").field("order_amount"))
						.subAggregation(AggregationBuilders.cardinality("userCnt").field("user_id"))
						.subAggregation(AggregationBuilders.sum("productCnt").field("product_num")))
				.get();
		
		//查询运费
		SearchResponse batchSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("batch_id").field("batch_id").size(10000)
						.subAggregation(AggregationBuilders.terms("productId").field("product_id"))
						.subAggregation(AggregationBuilders.sum("freightAmount").field("freight_money")))
				.get();
		
		SearchResponse freightSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderFreightAb(requestMap)).get();
		
		Map<String, Aggregation> batchtMap = batchSr.getAggregations().asMap();
		StringTerms batchTerms = (StringTerms) batchtMap.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> batchBucketIt = batchTerms
				.getBuckets().iterator();
		
		Map<String, Aggregation> freightMap = freightSr.getAggregations().asMap();
		StringTerms freightTerms = (StringTerms) freightMap.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> freightBucketIt = freightTerms
				.getBuckets().iterator();
		
		while (freightBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket freightBucket = freightBucketIt
					.next();
			Terms doubleTerms = (Terms) freightBucket.getAggregations().asMap().get("freight");
			Iterator<? extends org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> bucketsIt = doubleTerms
					.getBuckets().iterator();
			while (bucketsIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket = bucketsIt.next();
				freight_money = (Double) bucket.getKeyAsNumber();
			}
			freight.put(freightBucket.getKeyAsString(), freight_money);
		}
		
		while (batchBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket batchBucket = batchBucketIt
					.next();
			LongTerms sTerms = (LongTerms) batchBucket.getAggregations().asMap().get("productId");
			Iterator<Bucket> sIt = sTerms.getBuckets().iterator();
			while (sIt.hasNext()) {
				Bucket sBucket = sIt.next();
				batch.put(batchBucket.getKeyAsString(), sBucket.getKeyAsString());
			}
		}
		
		for (Map.Entry<String, Object> frei : freight.entrySet()) {
			for (Map.Entry<String, Object> bat : batch.entrySet()) {
				if (frei.getKey().equals(bat.getKey())) {
					if (batch1.containsKey(bat.getValue())) {
						batch1.put(bat.getValue().toString(), (Double)frei.getValue() + (Double)batch1.get(bat.getValue().toString()));
					} else {
						batch1.put(bat.getValue().toString(), frei.getValue());
					}
				}
			}
		}
		
		Map<String, Aggregation> logonMap = viewSr.getAggregations().asMap();

		LongTerms logonTerms = (LongTerms) logonMap.get("productId");
		Iterator<Bucket> logonBucketIt = logonTerms.getBuckets().iterator();

		while (logonBucketIt.hasNext()) {
			Bucket logonBucket = logonBucketIt.next();
			Map<String, Aggregation> subAggMap = logonBucket.getAggregations().asMap();
			int uv = (int) ((InternalCardinality) subAggMap.get("uv")).getValue();
			logon.put(logonBucket.getKey().toString(), logonBucket.getDocCount() + "," + uv);
		}

		Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();

		LongTerms orderTerms = (LongTerms) orderMap.get("productId");
		Iterator<Bucket> orderBucketIt = orderTerms.getBuckets().iterator();
		while (orderBucketIt.hasNext()) {
			Bucket orderBucket = orderBucketIt.next();
			Map<String, Aggregation> subAggMap = orderBucket.getAggregations().asMap();
			// sum值获取方法
			double orderAmount = ((InternalSum) subAggMap.get("orderAmount")).getValue();
			// cardinality值获取方法
			int userCnt = (int) ((InternalCardinality) subAggMap.get("userCnt")).getValue();
			// count值获取方法
			int orderCnt = (int) ((InternalValueCount) subAggMap.get("orderCnt")).getValue();
			int productCnt = (int) ((InternalSum) subAggMap.get("productCnt")).getValue();
			order.put(orderBucket.getKey().toString(), orderAmount + "," + userCnt + "," + orderCnt + "," + productCnt);
		}
		
		List<ProductDataVo> productList = new ArrayList<ProductDataVo>();
		for (Map.Entry<String, Object> or : order.entrySet()) {
			Iterator<Entry<String, Object>> log = logon.entrySet().iterator();
			Iterator<Entry<String, Object>> bat = batch1.entrySet().iterator();
			while (log.hasNext()) {
				Entry<String, Object> logEntry = log.next();
				if (or.getKey().equals(logEntry.getKey())) {
					ProductDataVo productVo = new ProductDataVo();
					String[] array = (or.getValue() + "," + logEntry.getValue()).toString().split(",");
					productVo.setProductId(or.getKey());
					productVo.setUv(Integer.valueOf(array[5])); //UV
					productVo.setOrderCnt(Integer.valueOf(array[2])); //OrderCnt
					order_amount = Double.valueOf(array[0].toString()); 
					while (bat.hasNext()) {
						Entry<String, Object> batEntry = bat.next();
						if (or.getKey().equals(batEntry.getKey())) {
							order_amount = (double) batEntry.getValue() + Double.valueOf(array[0].toString());
						} 
					}
					productVo.setOrderAmount(df.format(order_amount)); //OrderAmount
					productVo.setProductCnt(Integer.valueOf(array[3])); //ProductCnt
					productVo.setOrderAmountRate(DateUtil.numFormat((order_amount) /totalOrderAmount));
					productVo.setOrderCntRate(DateUtil.numFormat(Integer.valueOf(array[2]) /totalOrderCnt));
					productVo.setProductCntRate(DateUtil.numFormat(Integer.valueOf(array[3]) /totalProductCnt));
					productVo.setUvRate(DateUtil.numFormat(Integer.valueOf(array[5]) /totalUv));
					productList.add(productVo);
					log.remove();
				}
			}
		}
		for (Map.Entry<String, Object> vw : logon.entrySet()) {
			ProductDataVo productVo = new ProductDataVo();
			String[] array = (vw.getValue()).toString().split(",");
			productVo.setProductId(vw.getKey());
			productVo.setUv(Integer.valueOf(array[1]));
			productVo.setOrderCnt(0);
			productVo.setOrderCntRate("0.00");
			productVo.setOrderAmount("0.00");
			productVo.setOrderAmountRate("0.00");
			productVo.setProductCnt(0);
			productVo.setProductCntRate("0.00");
			productVo.setUvRate(DateUtil.numFormat(Integer.valueOf(array[1]) /totalUv));
			productList.add(productVo);
		}
		return productList;
	}
	
	@Override
	public List<ProductDataVo> getRegionDateByRegionId(Map<String, Object> requestMap) {
		DecimalFormat df = new DecimalFormat("0.00");
		double order_amount = 0;
		double freight_money = 0;
		Map<String, Object> freight = new HashMap<String, Object>();
		Map<String, Object> batch = new HashMap<String, Object>();
		Map<String, Object> batch1 = new HashMap<String, Object>();
		Map<String, Object> logon = new HashMap<String, Object>();
		Map<String, Object> order = new HashMap<String, Object>();
		
		// 查询统计周期内uv、pv
		SearchResponse logonResponse = EsClient.getConnect().prepareSearch("logon")
				.setQuery(EsUtil.logonBqb(requestMap))
				.addAggregation(AggregationBuilders.cardinality("uv").field("user_id"))
				.get();

		// 查询统计周期内销售金额、成功订单数、累计用户数（以手机号为维度的购买人即为用户）
		SearchResponse orderResponse = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.sum("orderAmount").field("order_amount")) 
				.addAggregation(AggregationBuilders.cardinality("orderCnt").field("batch_id")) 
				.addAggregation(AggregationBuilders.cardinality("orderUser").field("user_id"))
				.addAggregation(AggregationBuilders.sum("productCnt").field("product_num"))
				.get();
		
		// 查询运费
		SearchResponse freightResponse = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("batch_id").field("batch_id").size(10000)
						.subAggregation(AggregationBuilders.terms("freight").field("freight_money"))
						.subAggregation(AggregationBuilders.sum("freightAmount").field("freight_money")))
				.get();

		Map<String, Aggregation> freightMap1 = freightResponse.getAggregations().asMap();
		StringTerms freightTerms1 = (StringTerms) freightMap1.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> freightBucketIt1 = freightTerms1
				.getBuckets().iterator();

		while (freightBucketIt1.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket freightBucket = freightBucketIt1
					.next();
			Terms doubleTerms = (Terms) freightBucket.getAggregations().asMap().get("freight");
			Iterator<? extends org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> bucketsIt = doubleTerms
					.getBuckets().iterator();
			while (bucketsIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket = bucketsIt.next();
				freight_money = freight_money + (Double) bucket.getKeyAsNumber();
			}
		}
		double totalUv =  (double)((InternalCardinality) logonResponse.getAggregations().asMap().get("uv")).getValue();
		double totalOrderAmount = (double)((InternalSum) orderResponse.getAggregations().asMap().get("orderAmount")).getValue() + freight_money;
		double totalOrderCnt = (double)((InternalCardinality) orderResponse.getAggregations().asMap().get("orderCnt")).getValue();
		double totalProductCnt = (double)((InternalSum) orderResponse.getAggregations().asMap().get("productCnt")).getValue();

		// 查询统计周期内uv、pv
		SearchResponse viewSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderViewBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("productId").field("product_id").size(10000)
						.subAggregation(AggregationBuilders.cardinality("uv").field("user_id")))
				.get();
		
		// 按渠道查询订单数、订单金额、下单人数
		SearchResponse orderSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("productId").field("product_id").order(BucketOrder.aggregation("orderAmount", false)).size(10000)
						.subAggregation(AggregationBuilders.count("orderCnt").field("order_id"))
						.subAggregation(AggregationBuilders.sum("orderAmount").field("order_amount"))
						.subAggregation(AggregationBuilders.cardinality("userCnt").field("user_id"))
						.subAggregation(AggregationBuilders.sum("productCnt").field("product_num")))
				.get();
		
		//查询运费
		SearchResponse batchSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("batch_id").field("batch_id").size(10000)
						.subAggregation(AggregationBuilders.terms("productId").field("product_id"))
						.subAggregation(AggregationBuilders.sum("freightAmount").field("freight_money")))
				.get();
		
		SearchResponse freightSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderFreightAb(requestMap)).get();
		
		Map<String, Aggregation> batchtMap = batchSr.getAggregations().asMap();
		StringTerms batchTerms = (StringTerms) batchtMap.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> batchBucketIt = batchTerms
				.getBuckets().iterator();
		
		Map<String, Aggregation> freightMap = freightSr.getAggregations().asMap();
		StringTerms freightTerms = (StringTerms) freightMap.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> freightBucketIt = freightTerms
				.getBuckets().iterator();
		
		while (freightBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket freightBucket = freightBucketIt
					.next();
			Terms doubleTerms = (Terms) freightBucket.getAggregations().asMap().get("freight");
			Iterator<? extends org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> bucketsIt = doubleTerms
					.getBuckets().iterator();
			while (bucketsIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket = bucketsIt.next();
				freight_money = (Double) bucket.getKeyAsNumber();
			}
			freight.put(freightBucket.getKeyAsString(), freight_money);
		}
		
		while (batchBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket batchBucket = batchBucketIt
					.next();
			LongTerms sTerms = (LongTerms) batchBucket.getAggregations().asMap().get("productId");
			Iterator<Bucket> sIt = sTerms.getBuckets().iterator();
			while (sIt.hasNext()) {
				Bucket sBucket = sIt.next();
				batch.put(batchBucket.getKeyAsString(), sBucket.getKeyAsString());
			}
		}
		
		for (Map.Entry<String, Object> frei : freight.entrySet()) {
			for (Map.Entry<String, Object> bat : batch.entrySet()) {
				if (frei.getKey().equals(bat.getKey())) {
					if (batch1.containsKey(bat.getValue())) {
						batch1.put(bat.getValue().toString(), (Double)frei.getValue() + (Double)batch1.get(bat.getValue().toString()));
					} else {
						batch1.put(bat.getValue().toString(), frei.getValue());
					}
				}
			}
		}
		
		Map<String, Aggregation> logonMap = viewSr.getAggregations().asMap();

		LongTerms logonTerms = (LongTerms) logonMap.get("productId");
		Iterator<Bucket> logonBucketIt = logonTerms.getBuckets().iterator();

		while (logonBucketIt.hasNext()) {
			Bucket logonBucket = logonBucketIt.next();
			Map<String, Aggregation> subAggMap = logonBucket.getAggregations().asMap();
			int uv = (int) ((InternalCardinality) subAggMap.get("uv")).getValue();
			logon.put(logonBucket.getKey().toString(), logonBucket.getDocCount() + "," + uv);
		}

		Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();

		LongTerms orderTerms = (LongTerms) orderMap.get("productId");
		Iterator<Bucket> orderBucketIt = orderTerms.getBuckets().iterator();
		while (orderBucketIt.hasNext()) {
			Bucket orderBucket = orderBucketIt.next();
			Map<String, Aggregation> subAggMap = orderBucket.getAggregations().asMap();
			// sum值获取方法
			double orderAmount = ((InternalSum) subAggMap.get("orderAmount")).getValue();
			// cardinality值获取方法
			int userCnt = (int) ((InternalCardinality) subAggMap.get("userCnt")).getValue();
			// count值获取方法
			int orderCnt = (int) ((InternalValueCount) subAggMap.get("orderCnt")).getValue();
			int productCnt = (int) ((InternalSum) subAggMap.get("productCnt")).getValue();
			order.put(orderBucket.getKey().toString(), orderAmount + "," + userCnt + "," + orderCnt + "," + productCnt);
		}
		
		List<ProductDataVo> productList = new ArrayList<ProductDataVo>();
		for (Map.Entry<String, Object> or : order.entrySet()) {
			Iterator<Entry<String, Object>> log = logon.entrySet().iterator();
			Iterator<Entry<String, Object>> bat = batch1.entrySet().iterator();
			while (log.hasNext()) {
				Entry<String, Object> logEntry = log.next();
				if (or.getKey().equals(logEntry.getKey())) {
					ProductDataVo productVo = new ProductDataVo();
					String[] array = (or.getValue() + "," + logEntry.getValue()).toString().split(",");
					productVo.setProductId(or.getKey());
					productVo.setUv(Integer.valueOf(array[5])); //UV
					productVo.setOrderCnt(Integer.valueOf(array[2])); //OrderCnt
					order_amount = Double.valueOf(array[0].toString()); 
					while (bat.hasNext()) {
						Entry<String, Object> batEntry = bat.next();
						if (or.getKey().equals(batEntry.getKey())) {
							order_amount = (double) batEntry.getValue() + Double.valueOf(array[0].toString());
						} 
					}
					productVo.setOrderAmount(df.format(order_amount)); //OrderAmount
					productVo.setProductCnt(Integer.valueOf(array[3])); //ProductCnt
					productVo.setOrderAmountRate(DateUtil.numFormat((order_amount) /totalOrderAmount));
					productVo.setOrderCntRate(DateUtil.numFormat(Integer.valueOf(array[2]) /totalOrderCnt));
					productVo.setProductCntRate(DateUtil.numFormat(Integer.valueOf(array[3]) /totalProductCnt));
					productVo.setUvRate(DateUtil.numFormat(Integer.valueOf(array[5]) /totalUv));
					productList.add(productVo);
					log.remove();
				}
			}
		}
		for (Map.Entry<String, Object> vw : logon.entrySet()) {
			ProductDataVo productVo = new ProductDataVo();
			String[] array = (vw.getValue()).toString().split(",");
			productVo.setProductId(vw.getKey());
			productVo.setUv(Integer.valueOf(array[1]));
			productVo.setOrderCnt(0);
			productVo.setOrderCntRate("0.00");
			productVo.setOrderAmount("0.00");
			productVo.setOrderAmountRate("0.00");
			productVo.setProductCnt(0);
			productVo.setProductCntRate("0.00");
			productVo.setUvRate(DateUtil.numFormat(Integer.valueOf(array[1]) /totalUv));
			productList.add(productVo);
		}
		return productList;
	}

	@Override
	public List<ProductDataVo> getClientDataByClientId(Map<String, Object> requestMap) {
		DecimalFormat df = new DecimalFormat("0.00");
		double order_amount = 0;
		double freight_money = 0;
		Map<String, Object> freight = new HashMap<String, Object>();
		Map<String, Object> batch = new HashMap<String, Object>();
		Map<String, Object> batch1 = new HashMap<String, Object>();
		Map<String, Object> logon = new HashMap<String, Object>();
		Map<String, Object> order = new HashMap<String, Object>();
		
		// 查询统计周期内uv、pv
		SearchResponse logonResponse = EsClient.getConnect().prepareSearch("logon")
				.setQuery(EsUtil.logonBqb(requestMap))
				.addAggregation(AggregationBuilders.cardinality("uv").field("user_id"))
				.get();

		// 查询统计周期内销售金额、成功订单数、累计用户数（以手机号为维度的购买人即为用户）
		SearchResponse orderResponse = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.sum("orderAmount").field("order_amount")) 
				.addAggregation(AggregationBuilders.cardinality("orderCnt").field("batch_id")) 
				.addAggregation(AggregationBuilders.cardinality("orderUser").field("user_id"))
				.addAggregation(AggregationBuilders.sum("productCnt").field("product_num"))
				.get();
		
		// 查询运费
		SearchResponse freightResponse = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("batch_id").field("batch_id").size(10000)
						.subAggregation(AggregationBuilders.terms("freight").field("freight_money"))
						.subAggregation(AggregationBuilders.sum("freightAmount").field("freight_money")))
				.get();

		Map<String, Aggregation> freightMap1 = freightResponse.getAggregations().asMap();
		StringTerms freightTerms1 = (StringTerms) freightMap1.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> freightBucketIt1 = freightTerms1
				.getBuckets().iterator();

		while (freightBucketIt1.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket freightBucket = freightBucketIt1
					.next();
			Terms doubleTerms = (Terms) freightBucket.getAggregations().asMap().get("freight");
			Iterator<? extends org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> bucketsIt = doubleTerms
					.getBuckets().iterator();
			while (bucketsIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket = bucketsIt.next();
				freight_money = freight_money + (Double) bucket.getKeyAsNumber();
			}
		}
		double totalUv =  (double)((InternalCardinality) logonResponse.getAggregations().asMap().get("uv")).getValue();
		double totalOrderAmount = (double)((InternalSum) orderResponse.getAggregations().asMap().get("orderAmount")).getValue() + freight_money;
		double totalOrderCnt = (double)((InternalCardinality) orderResponse.getAggregations().asMap().get("orderCnt")).getValue();
		double totalProductCnt = (double)((InternalSum) orderResponse.getAggregations().asMap().get("productCnt")).getValue();

		// 查询统计周期内uv、pv
		SearchResponse viewSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderViewBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("productId").field("product_id").size(10000)
						.subAggregation(AggregationBuilders.cardinality("uv").field("user_id")))
				.get();
		
		// 按渠道查询订单数、订单金额、下单人数
		SearchResponse orderSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("productId").field("product_id").order(BucketOrder.aggregation("orderAmount", false)).size(10000)
						.subAggregation(AggregationBuilders.count("orderCnt").field("order_id"))
						.subAggregation(AggregationBuilders.sum("orderAmount").field("order_amount"))
						.subAggregation(AggregationBuilders.cardinality("userCnt").field("user_id"))
						.subAggregation(AggregationBuilders.sum("productCnt").field("product_num")))
				.get();
		
		//查询运费
		SearchResponse batchSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("batch_id").field("batch_id").size(10000)
						.subAggregation(AggregationBuilders.terms("productId").field("product_id"))
						.subAggregation(AggregationBuilders.sum("freightAmount").field("freight_money")))
				.get();
		
		SearchResponse freightSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderFreightAb(requestMap)).get();
		
		Map<String, Aggregation> batchtMap = batchSr.getAggregations().asMap();
		StringTerms batchTerms = (StringTerms) batchtMap.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> batchBucketIt = batchTerms
				.getBuckets().iterator();
		
		Map<String, Aggregation> freightMap = freightSr.getAggregations().asMap();
		StringTerms freightTerms = (StringTerms) freightMap.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> freightBucketIt = freightTerms
				.getBuckets().iterator();
		
		while (freightBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket freightBucket = freightBucketIt
					.next();
			Terms doubleTerms = (Terms) freightBucket.getAggregations().asMap().get("freight");
			Iterator<? extends org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> bucketsIt = doubleTerms
					.getBuckets().iterator();
			while (bucketsIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket = bucketsIt.next();
				freight_money = (Double) bucket.getKeyAsNumber();
			}
			freight.put(freightBucket.getKeyAsString(), freight_money);
		}
		
		while (batchBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket batchBucket = batchBucketIt
					.next();
			LongTerms sTerms = (LongTerms) batchBucket.getAggregations().asMap().get("productId");
			Iterator<Bucket> sIt = sTerms.getBuckets().iterator();
			while (sIt.hasNext()) {
				Bucket sBucket = sIt.next();
				batch.put(batchBucket.getKeyAsString(), sBucket.getKeyAsString());
			}
		}
		
		for (Map.Entry<String, Object> frei : freight.entrySet()) {
			for (Map.Entry<String, Object> bat : batch.entrySet()) {
				if (frei.getKey().equals(bat.getKey())) {
					if (batch1.containsKey(bat.getValue())) {
						batch1.put(bat.getValue().toString(), (Double)frei.getValue() + (Double)batch1.get(bat.getValue().toString()));
					} else {
						batch1.put(bat.getValue().toString(), frei.getValue());
					}
				}
			}
		}
		
		Map<String, Aggregation> logonMap = viewSr.getAggregations().asMap();

		LongTerms logonTerms = (LongTerms) logonMap.get("productId");
		Iterator<Bucket> logonBucketIt = logonTerms
				.getBuckets().iterator();

		while (logonBucketIt.hasNext()) {
			Bucket logonBucket = logonBucketIt.next();
			Map<String, Aggregation> subAggMap = logonBucket.getAggregations().asMap();
			int uv = (int) ((InternalCardinality) subAggMap.get("uv")).getValue();
			logon.put(logonBucket.getKey().toString(), logonBucket.getDocCount() + "," + uv);
		}

		Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();

		LongTerms orderTerms = (LongTerms) orderMap.get("productId");
		Iterator<Bucket> orderBucketIt = orderTerms
				.getBuckets().iterator();
		while (orderBucketIt.hasNext()) {
			Bucket orderBucket = orderBucketIt.next();
			Map<String, Aggregation> subAggMap = orderBucket.getAggregations().asMap();
			// sum值获取方法
			double orderAmount = ((InternalSum) subAggMap.get("orderAmount")).getValue();
			// cardinality值获取方法
			int userCnt = (int) ((InternalCardinality) subAggMap.get("userCnt")).getValue();
			// count值获取方法
			int orderCnt = (int) ((InternalValueCount) subAggMap.get("orderCnt")).getValue();
			int productCnt = (int) ((InternalSum) subAggMap.get("productCnt")).getValue();
			order.put(orderBucket.getKey().toString(), orderAmount + "," + userCnt + "," + orderCnt + "," + productCnt);
		}
		
		List<ProductDataVo> productList = new ArrayList<ProductDataVo>();
		for (Map.Entry<String, Object> or : order.entrySet()) {
			Iterator<Entry<String, Object>> log = logon.entrySet().iterator();
			Iterator<Entry<String, Object>> bat = batch1.entrySet().iterator();
			while (log.hasNext()) {
				Entry<String, Object> logEntry = log.next();
				if (or.getKey().equals(logEntry.getKey())) {
					ProductDataVo productVo = new ProductDataVo();
					String[] array = (or.getValue() + "," + logEntry.getValue()).toString().split(",");
					productVo.setProductId(or.getKey());
					productVo.setUv(Integer.valueOf(array[5])); //UV
					productVo.setOrderCnt(Integer.valueOf(array[2])); //OrderCnt
					order_amount = Double.valueOf(array[0].toString()); 
					while (bat.hasNext()) {
						Entry<String, Object> batEntry = bat.next();
						if (or.getKey().equals(batEntry.getKey())) {
							order_amount = (double) batEntry.getValue() + Double.valueOf(array[0].toString());
						} 
					}
					productVo.setOrderAmount(df.format(order_amount)); //OrderAmount
					productVo.setProductCnt(Integer.valueOf(array[3])); //ProductCnt
					productVo.setOrderAmountRate(DateUtil.numFormat((order_amount) / totalOrderAmount));
					productVo.setOrderCntRate(DateUtil.numFormat(Integer.valueOf(array[2]) / totalOrderCnt));
					productVo.setProductCntRate(DateUtil.numFormat(Integer.valueOf(array[3]) / totalProductCnt));
					productVo.setUvRate(DateUtil.numFormat(Integer.valueOf(array[5]) / totalUv));
					productList.add(productVo);
					log.remove();
				}
			}
		}
		for (Map.Entry<String, Object> vw : logon.entrySet()) {
			ProductDataVo productVo = new ProductDataVo();
			String[] array = (vw.getValue()).toString().split(",");
			productVo.setProductId(vw.getKey());
			productVo.setUv(Integer.valueOf(array[1]));
			productVo.setOrderCnt(0);
			productVo.setOrderCntRate("0.00");
			productVo.setOrderAmount("0.00");
			productVo.setOrderAmountRate("0.00");
			productVo.setProductCnt(0);
			productVo.setProductCntRate("0.00");
			productVo.setUvRate(DateUtil.numFormat(Integer.valueOf(array[1]) /totalUv));
			productList.add(productVo);
		}
		return productList;
	}
}

package com.hds.cn.bi.service.impl;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.Map.Entry;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.*;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms.Bucket;
import org.elasticsearch.search.aggregations.metrics.cardinality.InternalCardinality;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCount;
import org.springframework.stereotype.Service;

import com.hds.cn.bi.service.GetDataByEsService;
import com.hds.cn.bi.util.CommonConstant;
import com.hds.cn.bi.util.DateUtil;
import com.hds.cn.bi.util.EsClient;
import com.hds.cn.bi.util.EsUtil;
import com.hds.cn.bi.vo.BuyRateVo;
import com.hds.cn.bi.vo.ClientVo;
import com.hds.cn.bi.vo.PayVo;
import com.hds.cn.bi.vo.RegionVo;
import com.hds.cn.bi.vo.SourceVo;
import com.hds.cn.bi.vo.TotalVo;

@Service
public class IGetDataByEsServiceImpl implements GetDataByEsService {
	
	@Override
	public Map<String, Object> getTotalbyDay(Map<String, Object> requestMap) {
		
		double freight_money = 0;
		Map<String, Object> resultMap = new HashMap<String, Object>();
		SearchResponse logonRes = new SearchResponse();
		List<Object> list = new LinkedList<Object>();
		
		// 查询今日pv、uv
		if ("" != requestMap.get("productId") && -1 == Integer.valueOf(requestMap.get("productId").toString())) {
			SearchRequestBuilder logonSrb = EsClient.getConnect().prepareSearch(CommonConstant.LOGON)
					.setQuery(EsUtil.logonBqb(requestMap))
					.addAggregation(AggregationBuilders.cardinality("uv").field("user_id"))
					.addAggregation(AggregationBuilders.count("pv").field("user_id"));
			logonRes = logonSrb.get();
		}
		
		// 查询今日注册用户数
		SearchResponse registResponse = EsClient.getConnect().prepareSearch("regist")
				.setQuery(EsUtil.signBqb(requestMap))
				.addAggregation(AggregationBuilders.count("registCnt").field("user_id")).get();
		
		// 查询今日销售金额、成功订单数、成单用户数
		SearchResponse orderResponse = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.sum("orderAmount").field("order_amount"))
				.addAggregation(AggregationBuilders.cardinality("orderCnt").field("batch_id"))
				//.addAggregation(AggregationBuilders.cardinality("orderUser").field("user_id")) //.precisionThreshold(40000) 设置最大精确度，默认为100
				.addAggregation(AggregationBuilders.sum("productCnt").field("product_num")).get();
		
		//折叠实现的去重方式慢，而且没实现去重
		/*SearchResponse sr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.setCollapse(new CollapseBuilder("user_id.keyword"))
				.setSize(100000)
				.get();*/
		
		SearchResponse sr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.setExplain(false)
				.setFetchSource("user_id", "")
				.setSize(100000).get();
		
		for (SearchHit srHit : sr.getHits().getHits()) {
			list.add(srHit.getSourceAsMap().get("user_id"));
		}
		
		//去重
		List<Object> sortedList = list.parallelStream().distinct().collect(Collectors.toList());
		
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

		// 查询PV
		SearchResponse viewResponse = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderViewBqb(requestMap))
				.addAggregation(AggregationBuilders.count("pv").field("user_id"))
				.addAggregation(AggregationBuilders.cardinality("uv").field("user_id")).get();

		DecimalFormat df = new DecimalFormat("0.00");
		Map<String, Aggregation> registMap = registResponse.getAggregations().asMap();
		Map<String, Aggregation> orderMap = orderResponse.getAggregations().asMap();
		Map<String, Aggregation> viewMap = viewResponse.getAggregations().asMap();
		if ("" != requestMap.get("productId") && -1 == Integer.valueOf(requestMap.get("productId").toString())) {
			Map<String, Aggregation> logonMap = logonRes.getAggregations().asMap();
			resultMap.put("uv", ((InternalCardinality) logonMap.get("uv")).getValue());
			resultMap.put("pv", ((InternalValueCount) logonMap.get("pv")).getValue());
		} else if ("" != requestMap.get("productId") && -1 != Integer.valueOf(requestMap.get("productId").toString())) {
			resultMap.put("uv", ((InternalCardinality) viewMap.get("uv")).getValue());
			resultMap.put("pv", ((InternalValueCount) viewMap.get("pv")).getValue());
		}
		resultMap.put("orderAmount", df.format(((InternalSum) orderMap.get("orderAmount")).getValue() + freight_money));
		resultMap.put("orderCnt", ((InternalCardinality) orderMap.get("orderCnt")).getValue());
		resultMap.put("orderUser", sortedList.size());
		resultMap.put("registCnt", ((InternalValueCount) registMap.get("registCnt")).getValue());
		resultMap.put("productCnt", ((InternalSum) orderMap.get("productCnt")).getValue());
		return resultMap;
	}

	@Override
	public List<TotalVo> getTotalbyHour(Map<String, Object> requestMap) {
		double freight_money = 0;
		List<TotalVo> totalList = new ArrayList<TotalVo>();
		List<TotalVo> totalList1 = new ArrayList<TotalVo>();
		DecimalFormat df = new DecimalFormat("0.00");
		if (requestMap.get("type") == "hour" && "hour".equals(requestMap.get("type"))) {
			
			for (int i = 0; i < 24; i++) {
				TotalVo tv = new TotalVo();
				if (i < 10) {
					tv.setDate(DateUtil.toDate(requestMap.get("startDate").toString()) + " 0" + i + ":00:00");
				} else {
					tv.setDate(DateUtil.toDate(requestMap.get("startDate").toString()) + " " + i + ":00:00");
				}
				totalList1.add(tv);
			}
			
			SearchResponse sr = EsClient.getConnect().prepareSearch("bi_user_analysis_hour")
					.setQuery(EsUtil.orderHourBqb(requestMap)).setFrom(0).setSize(24).get();
			for (SearchHit srHit : sr.getHits().getHits()) {
				TotalVo totalVo = new TotalVo();
				int unique_visitor = Integer.parseInt(srHit.getSourceAsMap().get("unique_visitor").toString());
				totalVo.setDate(srHit.getSourceAsMap().get("stat_time").toString());
				totalVo.setUv(Integer.parseInt(srHit.getSourceAsMap().get("unique_visitor").toString()));
				totalVo.setPv(Integer.parseInt(srHit.getSourceAsMap().get("page_view").toString()));
				totalVo.setOrderAmount(
						df.format(Double.parseDouble(srHit.getSourceAsMap().get("order_amount").toString())));
				totalVo.setOrderCnt(Integer.parseInt(srHit.getSourceAsMap().get("order_count").toString()));
				totalVo.setUserCnt(Integer.parseInt(srHit.getSourceAsMap().get("success_user").toString()));
				totalVo.setConversionRate(
						df.format(Double.valueOf(srHit.getSourceAsMap().get("success_user").toString())
								/ (0 == unique_visitor ? 1 : unique_visitor)));
				totalVo.setProductCnt(Integer.parseInt(srHit.getSourceAsMap().get("product_number").toString()));
				totalList.add(totalVo);
			}
			if (DateUtil.dateAdd(DateUtil.dateStampToDay(System.currentTimeMillis()))
					.equals(requestMap.get("endDate"))) {
				
				TotalVo totalVo1 = new TotalVo();
				requestMap.put("today", DateUtil.dateStampToHour(System.currentTimeMillis()));
				SearchResponse logonSr = EsClient.getConnect().prepareSearch("logon")
						.setQuery(EsUtil.logonTodayBqb(requestMap))
						.addAggregation(AggregationBuilders.cardinality("uv").field("user_id"))
						.addAggregation(AggregationBuilders.count("pv").field("user_id")).get();

				SearchResponse orderSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderTodayBqb(requestMap))
						.addAggregation(AggregationBuilders.sum("orderAmount").field("order_amount"))
						.addAggregation(AggregationBuilders.count("orderCnt").field("batch_id"))
						.addAggregation(AggregationBuilders.cardinality("successUser").field("user_id"))
						.addAggregation(AggregationBuilders.sum("productCnt").field("product_num")).get();

				SearchResponse viewSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderTodayViewBqb(requestMap))
						.addAggregation(AggregationBuilders.count("pv").field("user_id"))
						.addAggregation(AggregationBuilders.cardinality("uv").field("user_id")).get();

				SearchResponse freightSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderTodayBqb(requestMap))
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
				
				totalVo1.setDate(DateUtil.dateStampToHour(System.currentTimeMillis()));
				Map<String, Aggregation> logonMap = logonSr.getAggregations().asMap();
				Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();
				Map<String, Aggregation> viewMap = viewSr.getAggregations().asMap();
				if ("" != requestMap.get("productId")
						&& -1 == Integer.valueOf(requestMap.get("productId").toString())) {
					totalVo1.setUv((int) ((InternalCardinality) logonMap.get("uv")).getValue());
					totalVo1.setPv((int) ((InternalValueCount) logonMap.get("pv")).getValue());
				} else if ("" != requestMap.get("productId")
						&& -1 != Integer.valueOf(requestMap.get("productId").toString())) {
					totalVo1.setUv((int) ((InternalCardinality) viewMap.get("uv")).getValue());
					totalVo1.setPv((int) ((InternalValueCount) viewMap.get("pv")).getValue());
				}
				totalVo1.setOrderAmount(
						df.format(((InternalSum) orderMap.get("orderAmount")).getValue() + freight_money));
				totalVo1.setOrderCnt((int) ((InternalValueCount) orderMap.get("orderCnt")).getValue());
				totalVo1.setUserCnt((int) ((InternalCardinality) orderMap.get("successUser")).getValue());
				if (((InternalCardinality) logonMap.get("uv")).getValueAsString().equals("0.0")) {
					totalVo1.setConversionRate("0");
				} else {
					totalVo1.setConversionRate(df.format(Double
							.parseDouble(String.valueOf(((InternalCardinality) orderMap.get("successUser")).getValue()))
							/ (int) (((InternalCardinality) logonMap.get("uv")).getValue())));
				}
				totalVo1.setProductCnt((int) ((InternalSum) orderMap.get("productCnt")).getValue());
				totalList.add(totalVo1);
			}
			Iterator<TotalVo> tlIt = totalList.iterator();
			while (tlIt.hasNext()) {
				TotalVo totalVo = (TotalVo) tlIt.next();
				Iterator<TotalVo> tvIt = totalList1.iterator();
				while (tvIt.hasNext()) {
					TotalVo totalVo2 = (TotalVo) tvIt.next();
					if (totalVo.getDate().equals(totalVo2.getDate())) {
						tvIt.remove();
					}
				}
			}
			totalList.addAll(totalList1);
		} else {
			if (DateUtil.dateAdd(DateUtil.dateStampToDay(System.currentTimeMillis()))
					.equals(requestMap.get("endDate"))) {
				TotalVo totalVo = new TotalVo();
				requestMap.put("today", DateUtil.dateStampToDay(System.currentTimeMillis()));
				SearchResponse logonSr = EsClient.getConnect().prepareSearch("logon")
						.setQuery(EsUtil.logonTodayBqb(requestMap))
						.addAggregation(AggregationBuilders.cardinality("uv").field("user_id"))
						.addAggregation(AggregationBuilders.count("pv").field("user_id")).get();

				SearchResponse orderSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderTodayBqb(requestMap))
						.addAggregation(AggregationBuilders.sum("orderAmount").field("order_amount"))
						.addAggregation(AggregationBuilders.count("orderCnt").field("batch_id"))
						.addAggregation(AggregationBuilders.cardinality("successUser").field("user_id"))
						.addAggregation(AggregationBuilders.sum("productCnt").field("product_num")).get();

				SearchResponse viewSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderTodayViewBqb(requestMap))
						.addAggregation(AggregationBuilders.count("pv").field("user_id"))
						.addAggregation(AggregationBuilders.cardinality("uv").field("user_id")).get();

				SearchResponse freightSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderTodayBqb(requestMap))
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

				totalVo.setDate(DateUtil.dateStampToDay(System.currentTimeMillis()));
				Map<String, Aggregation> logonMap = logonSr.getAggregations().asMap();
				Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();
				Map<String, Aggregation> viewMap = viewSr.getAggregations().asMap();
				if ("" != requestMap.get("productId")
						&& -1 == Integer.valueOf(requestMap.get("productId").toString())) {
					totalVo.setUv((int) ((InternalCardinality) logonMap.get("uv")).getValue());
					totalVo.setPv((int) ((InternalValueCount) logonMap.get("pv")).getValue());
				} else if ("" != requestMap.get("productId")
						&& -1 != Integer.valueOf(requestMap.get("productId").toString())) {
					totalVo.setUv((int) ((InternalCardinality) viewMap.get("uv")).getValue());
					totalVo.setPv((int) ((InternalValueCount) viewMap.get("pv")).getValue());
				}
				totalVo.setOrderAmount(
						df.format(((InternalSum) orderMap.get("orderAmount")).getValue() + freight_money));
				totalVo.setOrderCnt((int) ((InternalValueCount) orderMap.get("orderCnt")).getValue());
				totalVo.setUserCnt((int) ((InternalCardinality) orderMap.get("successUser")).getValue());
				if (((InternalCardinality) logonMap.get("uv")).getValueAsString().equals("0.0")) {
					totalVo.setConversionRate("0");
				} else {
					totalVo.setConversionRate(df.format(Double
							.parseDouble(String.valueOf(((InternalCardinality) orderMap.get("successUser")).getValue()))
							/ (int) (((InternalCardinality) logonMap.get("uv")).getValue())));
				}
				totalVo.setProductCnt((int) ((InternalSum) orderMap.get("productCnt")).getValue());
				totalList.add(totalVo);
			}
			SearchResponse sr = EsClient.getConnect().prepareSearch("bi_user_analysis_day")
					.setQuery(EsUtil.orderHourBqb(requestMap)).setFrom(0).setSize(9000).get();
			for (SearchHit srHit : sr.getHits().getHits()) {
				TotalVo totalVo = new TotalVo();
				totalVo.setDate(DateUtil.toDate(srHit.getSourceAsMap().get("stat_time").toString()));
				totalVo.setUv(Integer.parseInt(srHit.getSourceAsMap().get("unique_visitor").toString()));
				totalVo.setPv(Integer.parseInt(srHit.getSourceAsMap().get("page_view").toString()));
				totalVo.setOrderAmount(
						df.format(Double.parseDouble(srHit.getSourceAsMap().get("order_amount").toString())));
				totalVo.setOrderCnt(Integer.parseInt(srHit.getSourceAsMap().get("order_count").toString()));
				totalVo.setUserCnt(Integer.parseInt(srHit.getSourceAsMap().get("success_user").toString()));
				if (srHit.getSourceAsMap().get("unique_visitor").toString().equals("0")) {
					totalVo.setConversionRate("0");
				} else {
					totalVo.setConversionRate(
							df.format(Double.parseDouble(srHit.getSourceAsMap().get("success_user").toString())
									/ Integer.parseInt(srHit.getSourceAsMap().get("unique_visitor").toString())));
				}
				totalVo.setProductCnt(Integer.parseInt(srHit.getSourceAsMap().get("product_number").toString()));
				totalList.add(totalVo);
			}
		}
		return totalList;
	}
	
	@Override
	public List<SourceVo> getVisitorFrom(Map<String, Object> requestMap) {
		double freight_money = 0;
		double order_amount = 0;
		Map<String, Object> logon = new HashMap<String, Object>();
		Map<String, Object> order = new HashMap<String, Object>();
		Map<String, Object> view = new HashMap<String, Object>();
		Map<String, Object> freight = new HashMap<String, Object>();
		Map<String, Object> batch = new HashMap<String, Object>();
		Map<String, Object> batch1 = new HashMap<String, Object>();
		// 按渠道查询pv、uv
		SearchResponse logonSr = EsClient.getConnect().prepareSearch("logon").setQuery(EsUtil.logonBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("channelCnt").field("channel_id").size(1000)
						.subAggregation(AggregationBuilders.cardinality("uv").field("user_id")))
				.get();

		// 按渠道查询订单数、订单金额、下单人数
		SearchResponse orderSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderAb(requestMap)).get();

		SearchResponse viewSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderViewBqb(requestMap))
				.addAggregation(EsUtil.orderPageViewAb(requestMap)).get();
		
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
			if(batch.containsKey(frei.getKey())) {
				if (batch1.containsKey(batch.get(frei.getKey()))) {
					batch1.put(batch.get(frei.getKey()).toString(), (Double)frei.getValue() + (Double)batch1.get(batch.get(frei.getKey()).toString()));
				} else {
					batch1.put(batch.get(frei.getKey()).toString(), frei.getValue());
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
			int orderCnt = (int) ((InternalCardinality) subAggMap.get("orderCnt")).getValue();
			int productCnt = (int) ((InternalSum) subAggMap.get("productCnt")).getValue();
			order.put(orderBucket.getKey().toString(), orderAmount + "," + userCnt + "," + orderCnt + "," + productCnt);
		}

		Map<String, Aggregation> viewMap = viewSr.getAggregations().asMap();
		StringTerms viewTerms = (StringTerms) viewMap.get("channelCnt");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> viewBucketIt = viewTerms
				.getBuckets().iterator();
		while (viewBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket viewBucket = viewBucketIt.next();
			Map<String, Aggregation> subAggMap = viewBucket.getAggregations().asMap();
			// cardinality值获取方法
			int uv = (int) ((InternalCardinality) subAggMap.get("uv")).getValue();
			// count值获取方法
			int pv = (int) ((InternalValueCount) subAggMap.get("pv")).getValue();
			view.put(viewBucket.getKey().toString(), pv + "," + uv);
		}

		List<SourceVo> sourceList = new ArrayList<SourceVo>();
		DecimalFormat df = new DecimalFormat("0.00");
		if ("" != requestMap.get("productId") && -1 == Integer.valueOf(requestMap.get("productId").toString())) {
			for (Map.Entry<String, Object> or : order.entrySet()) {
				Iterator<Entry<String, Object>> log = logon.entrySet().iterator();
				Iterator<Entry<String, Object>> bat = batch1.entrySet().iterator();
				while (log.hasNext()) {
					Entry<String, Object> logEntry = log.next();
					if (or.getKey().equals(logEntry.getKey())) {
						SourceVo sourceVo = new SourceVo();
						String[] array = (or.getValue() + "," + logEntry.getValue()).toString().split(",");
						sourceVo.setChannel_id(or.getKey());
						sourceVo.setPv(Integer.valueOf(array[4]));
						sourceVo.setUv(Integer.valueOf(array[5]));
						sourceVo.setOrderCnt(Integer.valueOf(array[2]));
						order_amount = Double.valueOf(array[0].toString());
						while (bat.hasNext()) {
							Entry<String, Object> batEntry = bat.next();
							if (or.getKey().equals(batEntry.getKey())) {
								order_amount = (double) batEntry.getValue() + Double.valueOf(array[0].toString());
							} 
						}
						sourceVo.setOrderAmount(df.format(order_amount));
						sourceVo.setConversionRate(df.format((Double.valueOf(array[1]) / Double.valueOf(array[5]))));
						sourceVo.setProductCnt(Integer.valueOf(array[3]));
						sourceList.add(sourceVo);
						log.remove();
					}
				}
				while (bat.hasNext()) {
					Entry<String, Object> batEntry = bat.next();
					if (or.getKey().equals(batEntry.getKey())) {
						SourceVo sourceVo = new SourceVo();
						String[] array = or.getValue().toString().split(",");
						sourceVo.setOrderAmount(df.format(Double.valueOf(array[0].toString())));
					}
				}
			}
			if ("" != requestMap.get("size") && -1 == Integer.valueOf(requestMap.get("size").toString())) {
				for (Map.Entry<String, Object> log : logon.entrySet()) {
					if (sourceList.size() < 5) {
						SourceVo sourceVo = new SourceVo();
						String[] array = (log.getValue()).toString().split(",");
						sourceVo.setChannel_id(log.getKey());
						sourceVo.setPv(Integer.valueOf(array[0]));
						sourceVo.setUv(Integer.valueOf(array[1]));
						sourceVo.setOrderCnt(0);
						sourceVo.setOrderAmount("0.00");
						sourceVo.setConversionRate("0");
						sourceVo.setProductCnt(0);
						sourceList.add(sourceVo);
					}
				}
			} else {
				for (Map.Entry<String, Object> log : logon.entrySet()) {
					SourceVo sourceVo = new SourceVo();
					String[] array = (log.getValue()).toString().split(",");
					sourceVo.setChannel_id(log.getKey());
					sourceVo.setPv(Integer.valueOf(array[0]));
					sourceVo.setUv(Integer.valueOf(array[1]));
					sourceVo.setOrderCnt(0);
					sourceVo.setOrderAmount("0.00");
					sourceVo.setConversionRate("0");
					sourceVo.setProductCnt(0);
					sourceList.add(sourceVo);
				}
			}
		} else if ("" != requestMap.get("productId") && -1 != Integer.valueOf(requestMap.get("productId").toString())) {
			for (Map.Entry<String, Object> or : order.entrySet()) {
				Iterator<Entry<String, Object>> vw = view.entrySet().iterator();
				Iterator<Entry<String, Object>> bat = batch1.entrySet().iterator();
				while (vw.hasNext()) {
					Entry<String, Object> vwEntry = vw.next();
					if (or.getKey().equals(vwEntry.getKey())) {
						SourceVo sourceVo = new SourceVo();
						String[] array = (or.getValue() + "," + vwEntry.getValue()).toString().split(",");
						sourceVo.setChannel_id(or.getKey());
						sourceVo.setPv(Integer.valueOf(array[4]));
						sourceVo.setUv(Integer.valueOf(array[5]));
						sourceVo.setOrderCnt(Integer.valueOf(array[2]));
						order_amount = Double.valueOf(array[0].toString());
						while (bat.hasNext()) {
							Entry<String, Object> batEntry = bat.next();
							if (or.getKey().equals(batEntry.getKey())) {
								order_amount = (double) batEntry.getValue() + Double.valueOf(array[0].toString());
							} 
						}
						sourceVo.setOrderAmount(df.format(order_amount));
						sourceVo.setConversionRate(df.format((Double.valueOf(array[1]) / Double.valueOf(array[5]))));
						sourceVo.setProductCnt(Integer.valueOf(array[3]));
						sourceList.add(sourceVo);
						vw.remove();
					}
				}
			}
			if ("" != requestMap.get("size") && -1 == Integer.valueOf(requestMap.get("size").toString())) {
				for (Map.Entry<String, Object> vw : view.entrySet()) {
					if (sourceList.size() < 5) {
						SourceVo sourceVo = new SourceVo();
						String[] array = (vw.getValue()).toString().split(",");
						sourceVo.setChannel_id(vw.getKey());
						sourceVo.setPv(Integer.valueOf(array[0]));
						sourceVo.setUv(Integer.valueOf(array[1]));
						sourceVo.setOrderCnt(0);
						sourceVo.setOrderAmount("0.00");
						sourceVo.setConversionRate("0");
						sourceVo.setProductCnt(0);
						sourceList.add(sourceVo);
					}
				}
			} else {
				for (Map.Entry<String, Object> vw : view.entrySet()) {
					SourceVo sourceVo = new SourceVo();
					String[] array = (vw.getValue()).toString().split(",");
					sourceVo.setChannel_id(vw.getKey());
					sourceVo.setPv(Integer.valueOf(array[0]));
					sourceVo.setUv(Integer.valueOf(array[1]));
					sourceVo.setOrderCnt(0);
					sourceVo.setOrderAmount("0.00");
					sourceVo.setConversionRate("0");
					sourceVo.setProductCnt(0);
					sourceList.add(sourceVo);
				}
			}
		}
		return sourceList;
	}

	@Override
	public List<ClientVo> getClientFrom(Map<String, Object> requestMap) {
		double freight_money = 0;
		double order_amount = 0;
		Map<String, Object> freight = new HashMap<String, Object>();
		Map<String, Object> batch = new HashMap<String, Object>();
		Map<String, Object> batch1 = new HashMap<String, Object>();
		Map<String, Object> logon = new HashMap<String, Object>();
		Map<String, Object> order = new HashMap<String, Object>();
		Map<String, Object> view = new HashMap<String, Object>();
		// 查询每个终端的pv、uv
		SearchResponse logonSr = EsClient.getConnect().prepareSearch("logon").setQuery(EsUtil.logonBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("clientCnt").field("agent_type")
						.subAggregation(AggregationBuilders.cardinality("uv").field("user_id")))
				.get();
		// 查询每个终端的订单金额、订单数量、下单人数
		SearchResponse orderSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderClientAb(requestMap)).get();

		SearchResponse viewSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderViewBqb(requestMap))
				.addAggregation(EsUtil.orderClientViewAb(requestMap)).get();
		
		//查询运费
		SearchResponse batchSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderClientBatchAb(requestMap)).get();
		
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
			LongTerms sTerms = (LongTerms) batchBucket.getAggregations().asMap().get("clientCnt");
			Iterator<Bucket> sIt = sTerms.getBuckets().iterator();
			while (sIt.hasNext()) {
				Bucket sBucket = sIt.next();
				batch.put(batchBucket.getKeyAsString(), sBucket.getKeyAsString());
			}
		}
		
		for (Map.Entry<String, Object> frei : freight.entrySet()) {
			if(batch.containsKey(frei.getKey())) {
				if (batch1.containsKey(batch.get(frei.getKey()))) {
					batch1.put(batch.get(frei.getKey()).toString(), (Double)frei.getValue() + (Double)batch1.get(batch.get(frei.getKey()).toString()));
				} else {
					batch1.put(batch.get(frei.getKey()).toString(), frei.getValue());
				}
			}
		}
		
		Map<String, Aggregation> logonMap = logonSr.getAggregations().asMap();

		LongTerms longTerms = (LongTerms) logonMap.get("clientCnt");
		Iterator<Bucket> logonBucketIt = longTerms.getBuckets().iterator();

		while (logonBucketIt.hasNext()) {
			Bucket stringBucket = logonBucketIt.next();
			Map<String, Aggregation> subAggMap = stringBucket.getAggregations().asMap();
			int uv = (int) ((InternalCardinality) subAggMap.get("uv")).getValue();
			logon.put(stringBucket.getKey().toString(), stringBucket.getDocCount() + "," + uv);
		}

		Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();

		LongTerms orderTerms = (LongTerms) orderMap.get("clientCnt");
		Iterator<Bucket> orderBucketIt = orderTerms.getBuckets().iterator();
		while (orderBucketIt.hasNext()) {
			Bucket orderBucket = orderBucketIt.next();
			Map<String, Aggregation> subAggMap = orderBucket.getAggregations().asMap();
			// sum值获取方法
			double orderAmount = ((InternalSum) subAggMap.get("orderAmount")).getValue();
			// cardinality值获取方法
			int userCnt = (int) ((InternalCardinality) subAggMap.get("userCnt")).getValue();
			// count值获取方法
			int orderCnt = (int) ((InternalCardinality) subAggMap.get("orderCnt")).getValue();
			int productCnt = (int) ((InternalSum) subAggMap.get("productCnt")).getValue();
			order.put(orderBucket.getKey().toString(), orderAmount + "," + userCnt + "," + orderCnt + "," + productCnt);
		}

		Map<String, Aggregation> viewMap = viewSr.getAggregations().asMap();

		LongTerms viewTerms = (LongTerms) viewMap.get("clientCnt");
		Iterator<Bucket> viewBucketIt = viewTerms.getBuckets().iterator();
		while (viewBucketIt.hasNext()) {
			Bucket viewBucket = viewBucketIt.next();
			Map<String, Aggregation> subAggMap = viewBucket.getAggregations().asMap();
			// cardinality值获取方法
			int uv = (int) ((InternalCardinality) subAggMap.get("uv")).getValue();
			// count值获取方法
			int pv = (int) ((InternalValueCount) subAggMap.get("pv")).getValue();
			view.put(viewBucket.getKey().toString(), pv + "," + uv);
		}

		List<ClientVo> sourceList = new ArrayList<ClientVo>();
		DecimalFormat df = new DecimalFormat("0.00");
		if ("" != requestMap.get("productId") && -1 == Integer.valueOf(requestMap.get("productId").toString())) {
			for (Map.Entry<String, Object> or : order.entrySet()) {
				Iterator<Entry<String, Object>> log = logon.entrySet().iterator();
				Iterator<Entry<String, Object>> bat = batch1.entrySet().iterator();
				while (log.hasNext()) {
					Entry<String, Object> logEntry = log.next();
					if (or.getKey().equals(logEntry.getKey())) {
						ClientVo clientVo = new ClientVo();
						String[] array = (or.getValue() + "," + logEntry.getValue()).toString().split(",");
						clientVo.setClient_id(Integer.valueOf(or.getKey()));
						clientVo.setPv(Integer.valueOf(array[4]));
						clientVo.setUv(Integer.valueOf(array[5]));
						clientVo.setOrderCnt(Integer.valueOf(array[2]));
						order_amount = Double.valueOf(array[0].toString());
						while (bat.hasNext()) {
							Entry<String, Object> batEntry = bat.next();
							if (or.getKey().equals(batEntry.getKey())) {
								order_amount = (double) batEntry.getValue() + Double.valueOf(array[0].toString());
							} 
						}
						clientVo.setOrderAmount(df.format(order_amount));
						clientVo.setConversionRate(df.format((Double.valueOf(array[1]) / Double.valueOf(array[5]))));
						clientVo.setProductCnt(Integer.valueOf(array[3]));
						sourceList.add(clientVo);
						log.remove();
					}
				}
			}
			if ("" != requestMap.get("size") && -1 == Integer.valueOf(requestMap.get("size").toString())) {
				for (Map.Entry<String, Object> log : logon.entrySet()) {
					if (sourceList.size() < 5) {
						ClientVo clientVo = new ClientVo();
						String[] array = (log.getValue()).toString().split(",");
						clientVo.setClient_id(Integer.valueOf(log.getKey()));
						clientVo.setPv(Integer.valueOf(array[0]));
						clientVo.setUv(Integer.valueOf(array[1]));
						clientVo.setOrderCnt(0);
						clientVo.setOrderAmount("0.00");
						clientVo.setConversionRate("0");
						clientVo.setProductCnt(0);
						sourceList.add(clientVo);
					}
				}
			} else {
				for (Map.Entry<String, Object> log : logon.entrySet()) {
					ClientVo clientVo = new ClientVo();
					String[] array = (log.getValue()).toString().split(",");
					clientVo.setClient_id(Integer.valueOf(log.getKey()));
					clientVo.setPv(Integer.valueOf(array[0]));
					clientVo.setUv(Integer.valueOf(array[1]));
					clientVo.setOrderCnt(0);
					clientVo.setOrderAmount("0.00");
					clientVo.setConversionRate("0");
					clientVo.setProductCnt(0);
					sourceList.add(clientVo);
				}
			}
		} else if ("" != requestMap.get("productId") && -1 != Integer.valueOf(requestMap.get("productId").toString())) {
			for (Map.Entry<String, Object> or : order.entrySet()) {
				Iterator<Entry<String, Object>> vw = view.entrySet().iterator();
				Iterator<Entry<String, Object>> bat = batch1.entrySet().iterator();
				while (vw.hasNext()) {
					Entry<String, Object> vwEntry = vw.next();
					if (or.getKey().equals(vwEntry.getKey())) {
						ClientVo clientVo = new ClientVo();
						String[] array = (or.getValue() + "," + vwEntry.getValue()).toString().split(",");
						clientVo.setClient_id(Integer.valueOf(or.getKey()));
						clientVo.setPv(Integer.valueOf(array[4]));
						clientVo.setUv(Integer.valueOf(array[5]));
						clientVo.setOrderCnt(Integer.valueOf(array[2]));
						order_amount = Double.valueOf(array[0].toString());
						while (bat.hasNext()) {
							Entry<String, Object> batEntry = bat.next();
							if (or.getKey().equals(batEntry.getKey())) {
								order_amount = (double) batEntry.getValue() + Double.valueOf(array[0].toString());
							} 
						}
						clientVo.setOrderAmount(df.format(order_amount));
						clientVo.setConversionRate(df.format((Double.valueOf(array[1]) / Double.valueOf(array[5]))));
						clientVo.setProductCnt(Integer.valueOf(array[3]));
						sourceList.add(clientVo);
						vw.remove();
					}
				}
			}
			if ("" != requestMap.get("size") && -1 == Integer.valueOf(requestMap.get("size").toString())) {
				for (Map.Entry<String, Object> vw : view.entrySet()) {
					if (sourceList.size() < 5) {
						ClientVo clientVo = new ClientVo();
						String[] array = (vw.getValue()).toString().split(",");
						clientVo.setClient_id(Integer.valueOf(vw.getKey()));
						clientVo.setPv(Integer.valueOf(array[0]));
						clientVo.setUv(Integer.valueOf(array[1]));
						clientVo.setOrderCnt(0);
						clientVo.setOrderAmount("0.00");
						clientVo.setConversionRate("0");
						clientVo.setProductCnt(0);
						sourceList.add(clientVo);
					}
				}
			} else {
				for (Map.Entry<String, Object> vw : view.entrySet()) {
					ClientVo clientVo = new ClientVo();
					String[] array = (vw.getValue()).toString().split(",");
					clientVo.setClient_id(Integer.valueOf(vw.getKey()));
					clientVo.setPv(Integer.valueOf(array[0]));
					clientVo.setUv(Integer.valueOf(array[1]));
					clientVo.setOrderCnt(0);
					clientVo.setOrderAmount("0.00");
					clientVo.setConversionRate("0");
					clientVo.setProductCnt(0);
					sourceList.add(clientVo);
				}
			}
		}

		return sourceList;
	}

	@Override
	public List<PayVo> getPayFrom(Map<String, Object> requestMap) {
		double freight_money = 0;
		double order_amount = 0;
		Map<String, Object> freight = new HashMap<String, Object>();
		Map<String, Object> batch = new HashMap<String, Object>();
		Map<String, Object> batch1 = new HashMap<String, Object>();
		Map<String, Object> pay = new HashMap<String, Object>();
		Map<String, Object> paid = new HashMap<String, Object>();
		// 查询每种支付方式下的每个事件的订单金额、订单数量、下单人数
		SearchResponse payOrderSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqbPay(requestMap))
				.addAggregation(AggregationBuilders.terms("payType").field("pay_way").size(1000)
						.subAggregation(AggregationBuilders.cardinality("userCnt").field("user_id")))
				.get();
		
		SearchResponse paidOrderSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderPaidAb(requestMap)).get();
		
		//查询运费
		SearchResponse batchSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderPaidBatchAb(requestMap)).get();
		
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
			StringTerms sTerms = (StringTerms) batchBucket.getAggregations().asMap().get("payType");
			Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> sIt = sTerms.getBuckets().iterator();
			while (sIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket sBucket = sIt.next();
				batch.put(batchBucket.getKeyAsString(), sBucket.getKeyAsString());
			}
		}
		
		for (Map.Entry<String, Object> frei : freight.entrySet()) {
			if(batch.containsKey(frei.getKey())) {
				if (batch1.containsKey(batch.get(frei.getKey()))) {
					batch1.put(batch.get(frei.getKey()).toString(), (Double)frei.getValue() + (Double)batch1.get(batch.get(frei.getKey()).toString()));
				} else {
					batch1.put(batch.get(frei.getKey()).toString(), frei.getValue());
				}
			}
		}
		
		Map<String, Aggregation> payOrderMap = payOrderSr.getAggregations().asMap();

		StringTerms payOrderTerms = (StringTerms) payOrderMap.get("payType");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> makeOrderBucket = payOrderTerms
				.getBuckets().iterator();
		while (makeOrderBucket.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket orderBucket = makeOrderBucket.next();
			Map<String, Aggregation> subAggMap = orderBucket.getAggregations().asMap();
			// cardinality值获取方法
			int userCnt = (int) ((InternalCardinality) subAggMap.get("userCnt")).getValue();
			pay.put(orderBucket.getKey().toString(), userCnt);
		}

		Map<String, Aggregation> paidOrderMap = paidOrderSr.getAggregations().asMap();

		StringTerms paidOrderTerms = (StringTerms) paidOrderMap.get("payType");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> paidOrderBucket = paidOrderTerms
				.getBuckets().iterator();
		while (paidOrderBucket.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket orderBucket = paidOrderBucket.next();
			Map<String, Aggregation> subAggMap = orderBucket.getAggregations().asMap();
			// sum值获取方法
			double orderAmount = ((InternalSum) subAggMap.get("orderAmount")).getValue();
			// cardinality值获取方法
			int userCnt = (int) ((InternalCardinality) subAggMap.get("userCnt")).getValue();
			int productCnt = (int) ((InternalSum) subAggMap.get("productCnt")).getValue();
			// count值获取方法
			paid.put(orderBucket.getKey().toString(), orderAmount + "," + userCnt + "," + productCnt);
		}

		List<PayVo> payList = new ArrayList<PayVo>();
		DecimalFormat df = new DecimalFormat("0.00");
		for (Map.Entry<String, Object> or : paid.entrySet()) {
			Iterator<Entry<String, Object>> bat = batch1.entrySet().iterator();
			for (Map.Entry<String, Object> log : pay.entrySet()) {
				if (null != or.getKey() && or.getKey().equals(log.getKey())) {
					PayVo payVo = new PayVo();
					String[] array = (or.getValue() + "," + log.getValue()).toString().split(",");
					payVo.setPay_way(or.getKey().toString());
					order_amount = Double.valueOf(array[0].toString());
					while (bat.hasNext()) {
						Entry<String, Object> batEntry = bat.next();
						if (or.getKey().equals(batEntry.getKey())) {
							order_amount = (double) batEntry.getValue() + Double.valueOf(array[0].toString());
						} 
					}
					payVo.setOrderAmount(df.format(order_amount));
					payVo.setProductCnt(Integer.valueOf(array[2]));
					payVo.setConversionRate(df.format((Double.valueOf(array[1]) / Double.valueOf(array[3]))));
					payList.add(payVo);
				}
			}
		}
		return payList;
	}

	@Override
	public List<RegionVo> getRegionDistribution(Map<String, Object> requestMap) {
		double freight_money = 0;
		double order_amount = 0;
		Map<String, Object> freight = new HashMap<String, Object>();
		Map<String, Object> batch = new HashMap<String, Object>();
		Map<String, Object> batch1 = new HashMap<String, Object>();
		Map<String, Object> logon = new HashMap<String, Object>();
		Map<String, Object> order = new HashMap<String, Object>();
		Map<String, Object> view = new HashMap<String, Object>();
		// 查询每个终端的pv、uv
		SearchResponse logonSr = EsClient.getConnect().prepareSearch("logon").setQuery(EsUtil.logonBqb(requestMap))
				.addAggregation(EsUtil.regionLogonAb(requestMap)).get();
		// 查询每个终端的订单金额、订单数量、下单人数
		SearchResponse orderSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.regionOrderAb(requestMap)).get();

		SearchResponse viewSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderViewBqb(requestMap))
				.addAggregation(EsUtil.regionViewAb(requestMap)).get();
		
		//查询运费
		SearchResponse batchSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderRegionBatchAb(requestMap)).get();
		
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
			LongTerms sTerms = (LongTerms) batchBucket.getAggregations().asMap().get("region");
			Iterator<Bucket> sIt = sTerms.getBuckets().iterator();
			while (sIt.hasNext()) {
				Bucket sBucket = sIt.next();
				batch.put(batchBucket.getKeyAsString(), sBucket.getKeyAsString());
			}
		}
		
		for (Map.Entry<String, Object> frei : freight.entrySet()) {
			if(batch.containsKey(frei.getKey())) {
				if (batch1.containsKey(batch.get(frei.getKey()))) {
					batch1.put(batch.get(frei.getKey()).toString(), (Double)frei.getValue() + (Double)batch1.get(batch.get(frei.getKey()).toString()));
				} else {
					batch1.put(batch.get(frei.getKey()).toString(), frei.getValue());
				}
			}
		}
		Map<String, Aggregation> logonMap = logonSr.getAggregations().asMap();

		LongTerms logonTerms = (LongTerms) logonMap.get("region");
		Iterator<Bucket> logonBucketIt = logonTerms.getBuckets().iterator();

		while (logonBucketIt.hasNext()) {
			Bucket stringBucket = logonBucketIt.next();
			Map<String, Aggregation> subAggMap = stringBucket.getAggregations().asMap();
			int uv = (int) ((InternalCardinality) subAggMap.get("uv")).getValue();
			logon.put(stringBucket.getKey().toString(), stringBucket.getDocCount() + "," + uv);
		}

		Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();

		LongTerms orderTerms = (LongTerms) orderMap.get("region");
		Iterator<Bucket> orderBucketIt = orderTerms.getBuckets().iterator();
		while (orderBucketIt.hasNext()) {
			Bucket orderBucket = orderBucketIt.next();
			Map<String, Aggregation> subAggMap = orderBucket.getAggregations().asMap();
			// sum值获取方法
			double orderAmount = ((InternalSum) subAggMap.get("orderAmount")).getValue();
			// cardinality值获取方法
			int userCnt = (int) ((InternalCardinality) subAggMap.get("userCnt")).getValue();
			// count值获取方法
			int orderCnt = (int) ((InternalCardinality) subAggMap.get("orderCnt")).getValue();
			int productCnt = (int) ((InternalSum) subAggMap.get("productCnt")).getValue();
			order.put(orderBucket.getKey().toString(), orderAmount + "," + userCnt + "," + orderCnt + "," + productCnt);
		}

		Map<String, Aggregation> viewMap = viewSr.getAggregations().asMap();

		LongTerms viewTerms = (LongTerms) viewMap.get("region");
		Iterator<Bucket> viewBucketIt = viewTerms.getBuckets().iterator();

		while (viewBucketIt.hasNext()) {
			Bucket stringBucket = viewBucketIt.next();
			Map<String, Aggregation> subAggMap = stringBucket.getAggregations().asMap();
			int uv = (int) ((InternalCardinality) subAggMap.get("uv")).getValue();
			int pv = (int) ((InternalValueCount) subAggMap.get("pv")).getValue();
			view.put(stringBucket.getKey().toString(), pv + "," + uv);
		}

		List<RegionVo> regionList = new ArrayList<RegionVo>();
		DecimalFormat df = new DecimalFormat("0.00");
		if ("" != requestMap.get("productId") && -1 == Integer.valueOf(requestMap.get("productId").toString())) {
			for (Map.Entry<String, Object> or : order.entrySet()) {
				Iterator<Entry<String, Object>> bat = batch1.entrySet().iterator();
				for (Map.Entry<String, Object> log : logon.entrySet()) {
					if (log.getKey().equals(or.getKey())) {
						RegionVo regionVo = new RegionVo();
						String[] array = (or.getValue() + "," + log.getValue()).toString().split(",");
						regionVo.setRegionId(Integer.valueOf(or.getKey()));
						regionVo.setPv(Integer.valueOf(array[4]));
						regionVo.setUv(Integer.valueOf(array[5]));
						regionVo.setProductCnt(Integer.valueOf(array[3]));
						regionVo.setOrderCnt(Integer.valueOf(array[2]));
						order_amount = Double.valueOf(array[0].toString());
						while (bat.hasNext()) {
							Entry<String, Object> batEntry = bat.next();
							if (or.getKey().equals(batEntry.getKey())) {
								order_amount = (double) batEntry.getValue() + Double.valueOf(array[0].toString());
							} 
						}
						regionVo.setOrderAmount(df.format(order_amount));
						regionVo.setConversionRate(df.format((Double.valueOf(array[1]) / Double.valueOf(array[5]))));
						regionList.add(regionVo);
					}
				}
			}
		} else if ("" != requestMap.get("productId") && -1 != Integer.valueOf(requestMap.get("productId").toString())) {
			for (Map.Entry<String, Object> or : order.entrySet()) {
				Iterator<Entry<String, Object>> bat = batch1.entrySet().iterator();
				for (Map.Entry<String, Object> vw : view.entrySet()) {
					if (vw.getKey().equals(or.getKey())) {
						RegionVo regionVo = new RegionVo();
						String[] array = (or.getValue() + "," + vw.getValue()).toString().split(",");
						regionVo.setRegionId(Integer.valueOf(or.getKey()));
						regionVo.setPv(Integer.valueOf(array[4]));
						regionVo.setUv(Integer.valueOf(array[5]));
						regionVo.setProductCnt(Integer.valueOf(array[3]));
						regionVo.setOrderCnt(Integer.valueOf(array[2]));
						order_amount = Double.valueOf(array[0].toString());
						while (bat.hasNext()) {
							Entry<String, Object> batEntry = bat.next();
							if (or.getKey().equals(batEntry.getKey())) {
								order_amount = (double) batEntry.getValue() + Double.valueOf(array[0].toString());
							} 
						}
						regionVo.setOrderAmount(df.format(order_amount));
						regionVo.setConversionRate(df.format((Double.valueOf(array[1]) / Double.valueOf(array[5]))));
						regionList.add(regionVo);
					}
				}
			}
		}
		
		return regionList;
	}

	@Override
	public List<BuyRateVo> getBuyRate(Map<String, Object> requestMap) {
		SearchResponse orderSr;
		DecimalFormat df = new DecimalFormat("0.00");
		Map<String, Object> order = new HashMap<String, Object>();
		List<BuyRateVo> buyList = new ArrayList<BuyRateVo>();
		if (-1 == Integer.parseInt(requestMap.get("size").toString())) {
			orderSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqbBuyRate(requestMap))
					.addAggregation(AggregationBuilders.terms("eventType").field("event_type")
							.subAggregation(AggregationBuilders.cardinality("userCnt").field("user_id")))
					.get();
			Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();

			StringTerms orderTerms = (StringTerms) orderMap.get("eventType");
			Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> orderBucketIt = orderTerms
					.getBuckets().iterator();
			while (orderBucketIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket orderBucket = orderBucketIt
						.next();
				Map<String, Aggregation> subAggMap = orderBucket.getAggregations().asMap();
				// cardinality值获取方法
				int userCnt = (int) ((InternalCardinality) subAggMap.get("userCnt")).getValue();
				order.put(orderBucket.getKey().toString(), userCnt);
			}

			BuyRateVo buyRateVo = new BuyRateVo();

			int viewproduct = Integer
					.valueOf(null == order.get("viewproduct") ? "0" : order.get("viewproduct").toString());
			int checkout = Integer.valueOf(null == order.get("makeorder") ? "0" : order.get("makeorder").toString());
			int payorder = Integer.valueOf(null == order.get("payorder") ? "0" : order.get("payorder").toString());
			int paidorder = Integer.valueOf(null == order.get("paidorder") ? "0" : order.get("paidorder").toString());
			buyRateVo.setDate(requestMap.get("endDate").toString());
			buyRateVo.setViewDetailCnt(viewproduct);// 浏览详情页
			buyRateVo.setConfirmOrderCnt(checkout);// 确认订单页
			buyRateVo.setPayOrderCnt(payorder);// 支付订单页
			buyRateVo.setSuccessPaylCnt(paidorder);// 完成支付
			buyRateVo.setConfirmOrderRate(df.format(checkout / (Double.valueOf(viewproduct == 0 ? 1 : viewproduct))));
			buyRateVo.setPayOrderRate(df.format(payorder / (Double.valueOf(checkout == 0 ? 1 : checkout))));
			buyRateVo.setSuccessOrderRate(df.format(paidorder / (Double.valueOf(payorder == 0 ? 1 : payorder))));
			buyRateVo.setTotalRate(df.format(paidorder / (Double.valueOf(viewproduct == 0 ? 1 : viewproduct))));
			buyList.add(buyRateVo);
		} else {
			if (DateUtil.dateAdd(DateUtil.dateStampToDay(System.currentTimeMillis()))
					.equals(requestMap.get("endDate"))) {
				BuyRateVo buyRateVo = new BuyRateVo();
				requestMap.put("today", DateUtil.dateStampToDay(System.currentTimeMillis()));

				orderSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderBqbTodayBuyRate(requestMap))
						.addAggregation(AggregationBuilders.terms("eventType").field("event_type")
								.subAggregation(AggregationBuilders.cardinality("userCnt").field("user_id")))
						.get();

				Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();
				StringTerms stringTerm = (StringTerms) orderMap.get("eventType");
				Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> orderBucketIt = stringTerm
						.getBuckets().iterator();
				while (orderBucketIt.hasNext()) {
					org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket orderBucket = orderBucketIt
							.next();
					Map<String, Aggregation> subAggMap = orderBucket.getAggregations().asMap();
					// cardinality值获取方法
					int userCnt = (int) ((InternalCardinality) subAggMap.get("userCnt")).getValue();
					order.put(orderBucket.getKey().toString(), userCnt);
				}
				int viewproduct = Integer
						.valueOf(null == order.get("viewproduct") ? "0" : order.get("viewproduct").toString());
				int checkout = Integer
						.valueOf(null == order.get("makeorder") ? "0" : order.get("makeorder").toString());
				int payorder = Integer.valueOf(null == order.get("payorder") ? "0" : order.get("payorder").toString());
				int paidorder = Integer
						.valueOf(null == order.get("paidorder") ? "0" : order.get("paidorder").toString());

				buyRateVo.setDate(DateUtil.dateStampToDay(System.currentTimeMillis()));
				buyRateVo.setViewDetailCnt(viewproduct);// 浏览详情页
				buyRateVo.setConfirmOrderCnt(checkout);// 确认订单页
				buyRateVo.setPayOrderCnt(payorder);// 支付订单页
				buyRateVo.setSuccessPaylCnt(paidorder);// 完成支付
				buyRateVo.setConfirmOrderRate(
						df.format(checkout / (Double.valueOf(viewproduct == 0 ? 1 : viewproduct))));
				buyRateVo.setPayOrderRate(df.format(payorder / (Double.valueOf(checkout == 0 ? 1 : checkout))));
				buyRateVo.setSuccessOrderRate(df.format(paidorder / (Double.valueOf(payorder == 0 ? 1 : payorder))));
				buyRateVo.setTotalRate(df.format(paidorder / (Double.valueOf(viewproduct == 0 ? 1 : viewproduct))));

				buyList.add(buyRateVo);
			}

			SearchResponse sr = EsClient.getConnect().prepareSearch("bi_day_trans_ratio")
					.setQuery(EsUtil.orderBqbDayBuyRate(requestMap)).setFrom(0).setSize(5000).get();
			for (SearchHit srHit : sr.getHits().getHits()) {
				BuyRateVo buyRateVo = new BuyRateVo();
				int viewproduct = Integer.valueOf(null == srHit.getSourceAsMap().get("view_detail_cnt") ? "0"
						: srHit.getSourceAsMap().get("view_detail_cnt").toString());
				int checkout = Integer.valueOf(null == srHit.getSourceAsMap().get("make_order_cnt") ? "0"
						: srHit.getSourceAsMap().get("make_order_cnt").toString());
				int payorder = Integer.valueOf(null == srHit.getSourceAsMap().get("pay_order_cnt") ? "0"
						: srHit.getSourceAsMap().get("pay_order_cnt").toString());
				int paidorder = Integer.valueOf(null == srHit.getSourceAsMap().get("paid_order_cnt") ? "0"
						: srHit.getSourceAsMap().get("paid_order_cnt").toString());

				buyRateVo.setDate(DateUtil.toDate((srHit.getSourceAsMap().get("stat_time").toString())));
				buyRateVo.setViewDetailCnt(viewproduct);// 浏览详情页
				buyRateVo.setConfirmOrderCnt(checkout);// 确认订单页
				buyRateVo.setPayOrderCnt(payorder);// 支付订单页
				buyRateVo.setSuccessPaylCnt(paidorder);// 完成支付
				buyRateVo.setConfirmOrderRate(
						df.format(checkout / (Double.valueOf(viewproduct == 0 ? 1 : viewproduct))));
				buyRateVo.setPayOrderRate(df.format(payorder / (Double.valueOf(checkout == 0 ? 1 : checkout))));
				buyRateVo.setSuccessOrderRate(df.format(paidorder / (Double.valueOf(payorder == 0 ? 1 : payorder))));
				buyRateVo.setTotalRate(df.format(paidorder / (Double.valueOf(viewproduct == 0 ? 1 : viewproduct))));
				
				buyList.add(buyRateVo);
			}
		}
		return buyList;
	}

}

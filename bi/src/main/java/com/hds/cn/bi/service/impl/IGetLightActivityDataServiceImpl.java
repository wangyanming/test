package com.hds.cn.bi.service.impl;

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
import org.elasticsearch.search.aggregations.bucket.terms.*;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms.Bucket;
import org.elasticsearch.search.aggregations.metrics.cardinality.InternalCardinality;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCount;
import org.springframework.stereotype.Service;

import com.hds.cn.bi.service.GetLightActivityDataService;
import com.hds.cn.bi.util.DateUtil;
import com.hds.cn.bi.util.EsClient;
import com.hds.cn.bi.util.EsUtil;
import com.hds.cn.bi.vo.AgentDataVo;
import com.hds.cn.bi.vo.AgentVo;
import com.hds.cn.bi.vo.MultiActivityVo;
import com.hds.cn.bi.vo.ProductDataVo;

@Service
public class IGetLightActivityDataServiceImpl implements GetLightActivityDataService{
	
	private DecimalFormat df = new DecimalFormat("0.00");
	
	@Override
	public List<MultiActivityVo> getMultiActivity(Map<String, Object> requestMap) {
		double freight_money = 0;
		double order_amount = 0;
		List<MultiActivityVo> activityList = new ArrayList<MultiActivityVo>();
		Map<String, Object> view = new HashMap<String, Object>();
		Map<String, Object> order = new HashMap<String, Object>();
		Map<String, Object> batch = new HashMap<String, Object>();
		Map<String, Object> freight = new HashMap<String, Object>();
		Map<String, Object> batch1 = new HashMap<String, Object>();
		//查询UV，PV
		SearchResponse viewSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.multiViewBqb(requestMap))
				.addAggregation(EsUtil.multiViewAb(requestMap))
				.get();
		
		//查询订单数、金额、商品数量
		SearchResponse orderSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderMultiBqb(requestMap))
				.addAggregation(EsUtil.orderMultiAb(requestMap))
				.get();

		//查询运费
		SearchResponse batchSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderMultiBqb(requestMap))
				.addAggregation(EsUtil.multiBatchAb(requestMap))
				.get();
		
		SearchResponse freightSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderMultiBqb(requestMap))
				.addAggregation(EsUtil.orderFreightAb(requestMap))
				.get();
		
		Map<String, Aggregation> viewMap = viewSr.getAggregations().asMap();
		LongTerms viewTerm = (LongTerms) viewMap.get("product");
		Iterator<Bucket> viewIt = viewTerm.getBuckets().iterator();
		while (viewIt.hasNext()) {
			Bucket viewBucket =  viewIt.next();
			Map<String, Aggregation> bucketMap = viewBucket.getAggregations().asMap();
			int userCnt = (int) ((InternalCardinality) bucketMap.get("uv")).getValue();
			int pv = (int)((InternalValueCount)bucketMap.get("pv")).getValue();
			view.put(viewBucket.getKeyAsString(), userCnt + "," + pv);
		}
		
		Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();
		LongTerms orderTerm = (LongTerms) orderMap.get("product");
		Iterator<Bucket> orderIt = orderTerm.getBuckets().iterator();
		while (orderIt.hasNext()) {
			Bucket orderBucket =  orderIt.next();
			Map<String, Aggregation> bucketMap = orderBucket.getAggregations().asMap();
			double orderAmount = ((InternalSum) bucketMap.get("orderAmount")).getValue();
			int orderCnt = (int) ((InternalCardinality) bucketMap.get("orderCnt")).getValue();
			int productCnt = (int) ((InternalSum)bucketMap.get("productCnt")).getValue();
			order.put(orderBucket.getKeyAsString(), orderAmount + "," + orderCnt + "," + productCnt);
		}
		
		Map<String, Aggregation> batchtMap = batchSr.getAggregations().asMap();
		StringTerms batchTerms = (StringTerms) batchtMap.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> batchBucketIt = batchTerms
				.getBuckets().iterator();
		
		while (batchBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket batchBucket = batchBucketIt
					.next();
			LongTerms sTerms = (LongTerms) batchBucket.getAggregations().asMap().get("product");
			Iterator<Bucket> sIt = sTerms.getBuckets().iterator();
			while (sIt.hasNext()) {
				Bucket sBucket = sIt.next();
				batch.put(batchBucket.getKeyAsString(), sBucket.getKeyAsString());
			}
		}
		
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
		
		for (Map.Entry<String, Object> or : order.entrySet()) {
			Iterator<Entry<String, Object>> log = view.entrySet().iterator();
			Iterator<Entry<String, Object>> bat = batch1.entrySet().iterator();
			while (log.hasNext()) {
				Entry<String, Object> logEntry = log.next();
				if (logEntry.getKey().equals(or.getKey())) {
					MultiActivityVo activityVo = new MultiActivityVo();
					String[] array = (or.getValue() + "," + logEntry.getValue()).toString().split(",");
					activityVo.setProductId(Integer.valueOf(or.getKey()));
					activityVo.setUv(Integer.valueOf(array[3]));
					activityVo.setPv(Integer.valueOf(array[4]));
					activityVo.setOrderCnt(Integer.valueOf(array[1]));
					order_amount = Double.valueOf(array[0].toString());
					while (bat.hasNext()) {
						Entry<String, Object> batEntry = bat.next();
						if (or.getKey().equals(batEntry.getKey())) {
							order_amount = (double) batEntry.getValue() + Double.valueOf(array[0].toString());
						} 
					}
					activityVo.setOrderAmount(df.format(order_amount));
					activityVo.setProductCnt(Integer.valueOf(array[2]));
					activityList.add(activityVo);
					log.remove();
				}
			}
		}
		for (Map.Entry<String, Object> vw : view.entrySet()) {
			MultiActivityVo activityVo = new MultiActivityVo();
			String[] array = (vw.getValue()).toString().split(",");
			activityVo.setProductId(Integer.valueOf(vw.getKey()));
			activityVo.setUv(Integer.valueOf(array[0]));
			activityVo.setPv(Integer.valueOf(array[1]));
			activityVo.setOrderCnt(0);
			activityVo.setOrderAmount("0.00");
			activityList.add(activityVo);
		}
		return activityList;
	}

	@Override
	public Map<String, Object> getTotalData(Map<String, Object> requestMap) {
		Map<String, Object> resultMap = new HashMap<String, Object>();
		double freight_money = 0;
		
		String startDate = DateUtil.dateStampToDay(System.currentTimeMillis());
		String endDate = DateUtil.dateAdd(DateUtil.dateStampToDay(System.currentTimeMillis()));
		requestMap.put("startDate", startDate);
		requestMap.put("endDate", endDate);
		
		//今日新增用户数
		SearchResponse newUserSr = EsClient.getConnect().prepareSearch("regist")
				.setQuery(EsUtil.signBqb(requestMap))
				.addAggregation(AggregationBuilders.cardinality("registCnt").field("user_id"))
				.get();
		
		//累计新增用户数
		SearchResponse totalUserSr = EsClient.getConnect().prepareSearch("regist")
				.setQuery(EsUtil.signTotalBqb(requestMap))
				.addAggregation(AggregationBuilders.cardinality("registCnt").field("user_id"))
				.get();
		
		//今日成功订单数、金额、商品数量
		SearchResponse newOrderSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.sum("orderAmount").field("order_amount")) 
				.addAggregation(AggregationBuilders.cardinality("orderCnt").field("batch_id"))
				.get();
		
		//累计成功订单数
		SearchResponse totalOrderSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderTotalBqb(requestMap))
				.addAggregation(AggregationBuilders.count("orderCnt").field("batch_id"))
				.get();
		
		//查询今日运费
		SearchResponse newFreightSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(EsUtil.orderFreightAb(requestMap))
				.get();
		
		Map<String, Aggregation> newFreightMap = newFreightSr.getAggregations().asMap();
		StringTerms newFreightTerm = (StringTerms) newFreightMap.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> freightBucketIt = newFreightTerm
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
		double orderAmount = (double)((InternalSum)newOrderSr.getAggregations().asMap().get("orderAmount")).getValue();
		
		//今日新增用户
		resultMap.put("newUser", ((InternalCardinality)newUserSr.getAggregations().asMap().get("registCnt")).getValue());
		//累计新增用户
		resultMap.put("totalUser", ((InternalCardinality)totalUserSr.getAggregations().asMap().get("registCnt")).getValue());
		//今日订单数
		resultMap.put("newOrderCnt", ((InternalCardinality)newOrderSr.getAggregations().asMap().get("orderCnt")).getValue());
		//今日订单金额
		resultMap.put("newOrderAmount", df.format(orderAmount + freight_money));
		//累计订单数
		resultMap.put("totalOrderCnt", ((InternalValueCount)totalOrderSr.getAggregations().asMap().get("orderCnt")).getValue());
		return resultMap;
	}

	@Override
	public List<AgentVo> getAgentData(Map<String, Object> requestMap) {
		double freight_money = 0;
		double order_amount = 0;
		List<AgentVo> agentList = new ArrayList<AgentVo>();
		Map<String, Object> view = new HashMap<String, Object>();
		Map<String, Object> order = new HashMap<String, Object>();
		Map<String, Object> batch = new HashMap<String, Object>();
		Map<String, Object> freight = new HashMap<String, Object>();
		Map<String, Object> batch1 = new HashMap<String, Object>();
		
		//查詢UV、PV
		SearchResponse viewSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.multiAgentBqb(requestMap))
				.addAggregation(EsUtil.multiAgentAb(requestMap))
				.get();
		
		//查询订单数、金额、商品数量
		SearchResponse orderSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderAgentBqb(requestMap))
				.addAggregation(EsUtil.orderAgentAb(requestMap))
				.get();
		
		//查询运费
		SearchResponse batchSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderAgentBqb(requestMap))
				.addAggregation(EsUtil.agentAb(requestMap))
				.get();
		
		SearchResponse freightSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderAgentBqb(requestMap))
				.addAggregation(EsUtil.orderFreightAb(requestMap))
				.get();
		
		Map<String, Aggregation> viewMap = viewSr.getAggregations().asMap();
		StringTerms viewTerm = (StringTerms) viewMap.get("agent");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> viewIt = viewTerm.getBuckets().iterator();
		while (viewIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket viewBucket =  viewIt.next();
			Map<String, Aggregation> bucketMap = viewBucket.getAggregations().asMap();
			int userCnt = (int) ((InternalCardinality) bucketMap.get("uv")).getValue();
			int pv = (int)((InternalValueCount)bucketMap.get("pv")).getValue();
			view.put(viewBucket.getKeyAsString(), userCnt + "," + pv);
		}
		
		Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();
		StringTerms orderTerm = (StringTerms) orderMap.get("agent");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> orderIt = orderTerm.getBuckets().iterator();
		while (orderIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket orderBucket =  orderIt.next();
			Map<String, Aggregation> bucketMap = orderBucket.getAggregations().asMap();
			double orderAmount = ((InternalSum) bucketMap.get("orderAmount")).getValue();
			int orderCnt = (int) ((InternalCardinality) bucketMap.get("orderCnt")).getValue();
			int productCnt = (int) ((InternalSum)bucketMap.get("productCnt")).getValue();
			order.put(orderBucket.getKeyAsString(), orderAmount + "," + orderCnt + "," + productCnt);
		}
		
		Map<String, Aggregation> batchtMap = batchSr.getAggregations().asMap();
		StringTerms batchTerms = (StringTerms) batchtMap.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> batchBucketIt = batchTerms
				.getBuckets().iterator();
		
		while (batchBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket batchBucket = batchBucketIt
					.next();
			StringTerms sTerms = (StringTerms) batchBucket.getAggregations().asMap().get("agent");
			Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> sIt = sTerms.getBuckets().iterator();
			while (sIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket sBucket = sIt.next();
				batch.put(batchBucket.getKeyAsString(), sBucket.getKeyAsString());
			}
		}
		
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
		
		for (Map.Entry<String, Object> or : order.entrySet()) {
			Iterator<Entry<String, Object>> log = view.entrySet().iterator();
			Iterator<Entry<String, Object>> bat = batch1.entrySet().iterator();
			while (log.hasNext()) {
				Entry<String, Object> logEntry = log.next();
				if (logEntry.getKey().equals(or.getKey())) {
					AgentVo agentVo = new AgentVo();
					String[] array = (or.getValue() + "," + logEntry.getValue()).toString().split(",");
					agentVo.setAgentId(Integer.valueOf(or.getKey()));
					agentVo.setUv(Integer.valueOf(array[3]));
					agentVo.setPv(Integer.valueOf(array[4]));
					agentVo.setOrderCnt(Integer.valueOf(array[1]));
					order_amount = Double.valueOf(array[0].toString());
					while (bat.hasNext()) {
						Entry<String, Object> batEntry = bat.next();
						if (or.getKey().equals(batEntry.getKey())) {
							order_amount = (double) batEntry.getValue() + Double.valueOf(array[0].toString());
						} 
					}
					agentVo.setOrderAmount(df.format(order_amount));
					agentVo.setProductCnt(Integer.valueOf(array[2]));
					agentVo.setConversionRate(df.format(Double.valueOf(array[3])/Integer.valueOf(array[1])));
					agentList.add(agentVo);
					log.remove();
				}
			}
		}
		
		for (Map.Entry<String, Object> vw : view.entrySet()) {
			AgentVo agentVo = new AgentVo();
			String[] array = (vw.getValue()).toString().split(",");
			agentVo.setAgentId(Integer.valueOf(vw.getKey()));
			agentVo.setUv(Integer.valueOf(array[0]));
			agentVo.setPv(Integer.valueOf(array[1]));
			agentVo.setOrderCnt(0);
			agentVo.setProductCnt(0);
			agentVo.setOrderAmount("0.00");
			agentVo.setConversionRate("0");
			agentList.add(agentVo);
		}
		
		return agentList;
	}

	@Override
	public List<AgentDataVo> getAgentRegionData(Map<String, Object> requestMap) {
		double freight_money = 0;
		double order_amount = 0;
		Map<String, Object> freight = new HashMap<String, Object>();
		Map<String, Object> batch = new HashMap<String, Object>();
		Map<String, Object> batch1 = new HashMap<String, Object>();
		Map<String, Object> order = new HashMap<String, Object>();
		Map<String, Object> view = new HashMap<String, Object>();
		List<AgentDataVo> regionList = new ArrayList<AgentDataVo>();
		
		//查詢UV、PV
		SearchResponse viewSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.agentRegionBqb(requestMap))
				.addAggregation(EsUtil.agentRegionAb(requestMap))
				.get();

		// 查询每个终端的订单金额、订单数量、下单人数、商品数量
		SearchResponse orderSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderAgentRegionBqb(requestMap))
				.addAggregation(EsUtil.orderAgentRegionAb(requestMap))
				.get();
		
		//查询运费
		SearchResponse batchSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderAgentRegionBqb(requestMap))
				.addAggregation(EsUtil.orderRegionAgentAb(requestMap)).get();
		
		SearchResponse freightSr = EsClient.getConnect().prepareSearch("order").setQuery(EsUtil.orderAgentRegionBqb(requestMap))
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

		for (Map.Entry<String, Object> or : order.entrySet()) {
			Iterator<Entry<String, Object>> bat = batch1.entrySet().iterator();
			Iterator<Entry<String, Object>> log = view.entrySet().iterator();
			while (log.hasNext()) {
				Entry<String, Object> vw = log.next();
				if (vw.getKey().equals(or.getKey())) {
					AgentDataVo agentVo = new AgentDataVo();
					String[] array = (or.getValue() + "," + vw.getValue()).toString().split(",");
					agentVo.setOrgId(Integer.parseInt(requestMap.get("orgId").toString()));
					agentVo.setProductType(requestMap.get("productType").toString());
					agentVo.setAgentId(Integer.parseInt(requestMap.get("agentId").toString()));
					agentVo.setId(or.getKey());
					agentVo.setPv(Integer.valueOf(array[4]));
					agentVo.setUv(Integer.valueOf(array[5]));
					agentVo.setProductCnt(Integer.valueOf(array[3]));
					agentVo.setOrderCnt(Integer.valueOf(array[2]));
					order_amount = Double.valueOf(array[0].toString());
					while (bat.hasNext()) {
						Entry<String, Object> batEntry = bat.next();
						if (or.getKey().equals(batEntry.getKey())) {
							order_amount = (double) batEntry.getValue() + Double.valueOf(array[0].toString());
						} 
					}
					agentVo.setOrderAmount(df.format(order_amount));
					agentVo.setConversionRate(df.format((Double.valueOf(array[1]) / Double.valueOf(array[5]))));
					regionList.add(agentVo);
					log.remove();
				}
			}
		}
		
		for (Map.Entry<String, Object> log1 : view.entrySet()) {
			AgentDataVo agentVo = new AgentDataVo();
			String[] array = log1.getValue().toString().split(",");
			agentVo.setOrgId(Integer.parseInt(requestMap.get("orgId").toString()));
			agentVo.setProductType(requestMap.get("productType").toString());
			agentVo.setAgentId(Integer.parseInt(requestMap.get("agentId").toString()));
			agentVo.setId(log1.getKey());
			agentVo.setPv(Integer.parseInt(array[0].toString()));
			agentVo.setUv(Integer.parseInt(array[1].toString()));
			agentVo.setOrderCnt(0);
			agentVo.setOrderAmount("0");
			agentVo.setConversionRate("0");
			regionList.add(agentVo);
		}
		return regionList;
	}
	
	@Override
	public List<AgentDataVo> getAgentSaleData(Map<String, Object> requestMap) {
		double freight_money = 0;
		List<AgentDataVo> totalList = new ArrayList<AgentDataVo>();
		List<AgentDataVo> totalList1 = new ArrayList<AgentDataVo>();
		//查询小时索引
		if (DateUtil.dateAdd(requestMap.get("startDate").toString()).equals(requestMap.get("endDate"))) {
			//计算24个小时
			for (int i = 0; i < 24; i++) {
				AgentDataVo tv = new AgentDataVo();
				if (i < 10) {
					tv.setStatTime(DateUtil.toDate(requestMap.get("startDate").toString()) + " 0" + i + ":00:00");
				} else {
					tv.setStatTime(DateUtil.toDate(requestMap.get("startDate").toString()) + " " + i + ":00:00");
				}
				totalList1.add(tv);
			}
			
			SearchResponse sr = EsClient.getConnect().prepareSearch("bi_hour_agent_data")
					.setQuery(EsUtil.orderAgentHourBqb(requestMap)).setFrom(0).setSize(24).get();
			for (SearchHit srHit : sr.getHits().getHits()) {
				AgentDataVo agentVo = new AgentDataVo();
				agentVo.setOrgId(Integer.parseInt(requestMap.get("orgId").toString()));
				agentVo.setProductType(requestMap.get("productType").toString());
				agentVo.setAgentId(Integer.parseInt(requestMap.get("agentId").toString()));
				agentVo.setStatTime(srHit.getSourceAsMap().get("stat_time").toString());
				agentVo.setUv(Integer.parseInt(srHit.getSourceAsMap().get("uv").toString()));
				agentVo.setPv(Integer.parseInt(srHit.getSourceAsMap().get("pv").toString()));
				agentVo.setOrderAmount(df.format(Double.parseDouble(srHit.getSourceAsMap().get("order_amount").toString())));
				agentVo.setOrderCnt(Integer.parseInt(srHit.getSourceAsMap().get("order_count").toString()));
				agentVo.setSuccessUser(Integer.parseInt(srHit.getSourceAsMap().get("success_user").toString()));
				agentVo.setConversionRate(
						df.format(Double.valueOf(srHit.getSourceAsMap().get("success_user").toString())
								/ (0 == Integer.parseInt(srHit.getSourceAsMap().get("uv").toString()) ? 1 
										: Integer.parseInt(srHit.getSourceAsMap().get("uv").toString()))));
				agentVo.setProductCnt(Integer.parseInt(srHit.getSourceAsMap().get("product_number").toString()));
				totalList.add(agentVo);
			}
			
			if (DateUtil.dateAdd(DateUtil.dateStampToDay(System.currentTimeMillis()))
			.equals(requestMap.get("endDate"))) {
				//获取当前小时
				String currentHour = DateUtil.dateStampToHour(System.currentTimeMillis());
				AgentDataVo agentVo = new AgentDataVo();
				requestMap.put("today", currentHour);
				
				SearchResponse orderSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderAgentTodayBqb(requestMap))
						.addAggregation(AggregationBuilders.sum("orderAmount").field("order_amount"))
						.addAggregation(AggregationBuilders.count("orderCnt").field("batch_id"))
						.addAggregation(AggregationBuilders.cardinality("successUser").field("user_id"))
						.addAggregation(AggregationBuilders.sum("productCnt").field("product_num")).get();

				SearchResponse viewSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderAgentTodayViewBqb(requestMap))
						.addAggregation(AggregationBuilders.count("pv").field("user_id"))
						.addAggregation(AggregationBuilders.cardinality("uv").field("user_id")).get();

				SearchResponse freightSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderAgentTodayBqb(requestMap))
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

				agentVo.setStatTime(currentHour);
				Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();
				Map<String, Aggregation> viewMap = viewSr.getAggregations().asMap();
				agentVo.setOrgId(Integer.parseInt(requestMap.get("orgId").toString()));
				agentVo.setProductType(requestMap.get("productType").toString());
				agentVo.setAgentId(Integer.parseInt(requestMap.get("agentId").toString()));
				agentVo.setUv((int) ((InternalCardinality) viewMap.get("uv")).getValue());
				agentVo.setPv((int) ((InternalValueCount) viewMap.get("pv")).getValue());
				agentVo.setOrderAmount(
						df.format(((InternalSum) orderMap.get("orderAmount")).getValue() + freight_money));
				agentVo.setOrderCnt((int) ((InternalValueCount) orderMap.get("orderCnt")).getValue());
				agentVo.setSuccessUser((int) ((InternalCardinality) orderMap.get("successUser")).getValue());
				agentVo.setProductCnt((int) ((InternalSum) orderMap.get("productCnt")).getValue());
				totalList.add(agentVo);
			}
			
			//删除提前添加的小时
			Iterator<AgentDataVo> tlIt = totalList.iterator();
			while (tlIt.hasNext()) {
				AgentDataVo totalVo = (AgentDataVo) tlIt.next();
				Iterator<AgentDataVo> tvIt = totalList1.iterator();
				while (tvIt.hasNext()) {
					AgentDataVo totalVo2 = (AgentDataVo) tvIt.next();
					if (totalVo.getStatTime().equals(totalVo2.getStatTime())) {
						tvIt.remove();
					}
				}
			}
			totalList.addAll(totalList1);
		} else {
			if (DateUtil.dateAdd(DateUtil.dateStampToDay(System.currentTimeMillis()))
					.equals(requestMap.get("endDate"))) {

				AgentDataVo agentVo = new AgentDataVo();
				requestMap.put("today", DateUtil.dateStampToDay(System.currentTimeMillis()));

				SearchResponse orderSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderAgentTodayBqb(requestMap))
						.addAggregation(AggregationBuilders.sum("orderAmount").field("order_amount"))
						.addAggregation(AggregationBuilders.count("orderCnt").field("batch_id"))
						.addAggregation(AggregationBuilders.cardinality("successUser").field("user_id"))
						.addAggregation(AggregationBuilders.sum("productCnt").field("product_num")).get();

				SearchResponse viewSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderAgentTodayViewBqb(requestMap))
						.addAggregation(AggregationBuilders.count("pv").field("user_id"))
						.addAggregation(AggregationBuilders.cardinality("uv").field("user_id")).get();

				SearchResponse freightSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderAgentTodayBqb(requestMap))
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

				agentVo.setStatTime(DateUtil.dateStampToDay(System.currentTimeMillis()));
				Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();
				Map<String, Aggregation> viewMap = viewSr.getAggregations().asMap();
				agentVo.setOrgId(Integer.parseInt(requestMap.get("orgId").toString()));
				agentVo.setProductType(requestMap.get("productType").toString());
				agentVo.setAgentId(Integer.parseInt(requestMap.get("agentId").toString()));
				agentVo.setUv((int) ((InternalCardinality) viewMap.get("uv")).getValue());
				agentVo.setPv((int) ((InternalValueCount) viewMap.get("pv")).getValue());
				agentVo.setOrderAmount(
						df.format(((InternalSum) orderMap.get("orderAmount")).getValue() + freight_money));
				agentVo.setOrderCnt((int) ((InternalValueCount) orderMap.get("orderCnt")).getValue());
				agentVo.setSuccessUser((int) ((InternalCardinality) orderMap.get("successUser")).getValue());
				agentVo.setProductCnt((int) ((InternalSum) orderMap.get("productCnt")).getValue());
				totalList.add(agentVo);
			}
			SearchResponse sr = EsClient.getConnect().prepareSearch("bi_day_agent_data")
					.setQuery(EsUtil.orderAgentHourBqb(requestMap)).setSize(10000).get();
			for (SearchHit srHit : sr.getHits().getHits()) {
				AgentDataVo agentVo = new AgentDataVo();
				agentVo.setOrgId(Integer.parseInt(requestMap.get("orgId").toString()));
				agentVo.setProductType(requestMap.get("productType").toString());
				agentVo.setAgentId(Integer.parseInt(requestMap.get("agentId").toString()));
				agentVo.setStatTime(srHit.getSourceAsMap().get("stat_time").toString());
				agentVo.setUv(Integer.parseInt(srHit.getSourceAsMap().get("uv").toString()));
				agentVo.setPv(Integer.parseInt(srHit.getSourceAsMap().get("pv").toString()));
				agentVo.setOrderAmount(df.format(Double.parseDouble(srHit.getSourceAsMap().get("order_amount").toString())));
				agentVo.setOrderCnt(Integer.parseInt(srHit.getSourceAsMap().get("order_count").toString()));
				agentVo.setSuccessUser(Integer.parseInt(srHit.getSourceAsMap().get("success_user").toString()));
				agentVo.setConversionRate(
						df.format(Double.valueOf(srHit.getSourceAsMap().get("success_user").toString())
								/ (0 == Integer.parseInt(srHit.getSourceAsMap().get("uv").toString()) ? 1 
										: Integer.parseInt(srHit.getSourceAsMap().get("uv").toString()))));
				agentVo.setProductCnt(Integer.parseInt(srHit.getSourceAsMap().get("product_number").toString()));
				totalList.add(agentVo);
			}
		}
		return totalList;
	}
	
	@Override
	public List<AgentDataVo> getAgentFromData(Map<String, Object> requestMap) {
		double freight_money = 0;
		double order_amount = 0;
		List<AgentDataVo> totalList = new ArrayList<AgentDataVo>();
		List<AgentDataVo> totalList1 = new ArrayList<AgentDataVo>();
		Map<String, Object> order = new HashMap<String, Object>();
		Map<String, Object> view = new HashMap<String, Object>();
		Map<String, Object> freight = new HashMap<String, Object>();
		Map<String, Object> batch = new HashMap<String, Object>();
		Map<String, Object> batch1 = new HashMap<String, Object>();
		//查询小时索引
		if (DateUtil.dateAdd(requestMap.get("startDate").toString()).equals(requestMap.get("endDate"))) {
			for (int i = 0; i < 24; i++) {
				AgentDataVo tv = new AgentDataVo();
				if (i < 10) {
					tv.setStatTime(DateUtil.toDate(requestMap.get("startDate").toString()) + " 0" + i + ":00:00");
				} else {
					tv.setStatTime(DateUtil.toDate(requestMap.get("startDate").toString()) + " " + i + ":00:00");
				}
				totalList1.add(tv);
			}
			SearchResponse sr = EsClient.getConnect().prepareSearch("bi_hour_agent_data")
					.setQuery(EsUtil.orderAgentFromBqb(requestMap)).setFrom(0).setSize(24).get();
			for (SearchHit srHit : sr.getHits().getHits()) {
				AgentDataVo agentVo = new AgentDataVo();
				agentVo.setId(srHit.getSourceAsMap().get("id").toString());
				agentVo.setAgentId(Integer.parseInt(requestMap.get("agentId").toString()));
				agentVo.setStatTime(srHit.getSourceAsMap().get("stat_time").toString());
				agentVo.setUv(Integer.parseInt(srHit.getSourceAsMap().get("uv").toString()));
				agentVo.setPv(Integer.parseInt(srHit.getSourceAsMap().get("pv").toString()));
				agentVo.setOrderAmount(df.format(Double.parseDouble(srHit.getSourceAsMap().get("order_amount").toString())));
				agentVo.setOrderCnt(Integer.parseInt(srHit.getSourceAsMap().get("order_count").toString()));
				agentVo.setSuccessUser(Integer.parseInt(srHit.getSourceAsMap().get("success_user").toString()));
				agentVo.setConversionRate(
						df.format(Double.valueOf(srHit.getSourceAsMap().get("success_user").toString())
								/ (0 == Integer.parseInt(srHit.getSourceAsMap().get("uv").toString()) ? 1 
										: Integer.parseInt(srHit.getSourceAsMap().get("uv").toString()))));
				agentVo.setProductCnt(Integer.parseInt(srHit.getSourceAsMap().get("product_number").toString()));
				totalList.add(agentVo);
			}
			
			if (DateUtil.dateAdd(DateUtil.dateStampToDay(System.currentTimeMillis()))
					.equals(requestMap.get("endDate"))) {
						//获取当前小时
				String currentHour = DateUtil.dateStampToHour(System.currentTimeMillis());
				requestMap.put("today", currentHour);

				SearchResponse orderSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderAgentTodayBqb(requestMap))
						.addAggregation(AggregationBuilders.terms("channelId").field("channel_id")
								.subAggregation(AggregationBuilders.sum("orderAmount").field("order_amount"))
								.subAggregation(AggregationBuilders.count("orderCnt").field("batch_id"))
								.subAggregation(AggregationBuilders.cardinality("successUser").field("user_id"))
								.subAggregation(AggregationBuilders.sum("productCnt").field("product_num")))
						.get();

				SearchResponse viewSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderAgentTodayViewBqb(requestMap))
						.addAggregation(AggregationBuilders.terms("channelId").field("channel_id")
								.subAggregation(AggregationBuilders.count("pv").field("user_id"))
								.subAggregation(AggregationBuilders.cardinality("uv").field("user_id"))).get();

				SearchResponse freightSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderAgentTodayBqb(requestMap))
						.addAggregation(AggregationBuilders.terms("batch_id").field("batch_id").size(10000)
								.subAggregation(AggregationBuilders.terms("freight").field("freight_money"))
								.subAggregation(AggregationBuilders.sum("freightAmount").field("freight_money")))
						.get();
				
				SearchResponse batchSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderAgentTodayBqb(requestMap))
						.addAggregation(EsUtil.orderBatchAb(requestMap)).get();
				
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
				
				Map<String, Aggregation> batchtMap = batchSr.getAggregations().asMap();
				StringTerms batchTerms = (StringTerms) batchtMap.get("batch_id");
				Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> batchBucketIt = batchTerms
						.getBuckets().iterator();
				
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
				
				Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();
				StringTerms orderTerms = (StringTerms) orderMap.get("channelId");
				Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> orderBucketIt = orderTerms
						.getBuckets().iterator();
				
				while (orderBucketIt.hasNext()) {
					org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket orderBucket = orderBucketIt
							.next();
					
					Map<String, Aggregation> subAggMap = orderBucket.getAggregations().asMap();
					double orderAmount = (Double) ((InternalSum) subAggMap.get("orderAmount")).getValue();
					int orderCnt = (int) ((InternalValueCount) subAggMap.get("orderCnt")).getValue();
					int successUser = (int) ((InternalCardinality) subAggMap.get("successUser")).getValue();
					int productCnt = (int) ((InternalSum) subAggMap.get("productCnt")).getValue();
					order.put(orderBucket.getKey().toString(), orderAmount + "," + orderCnt + "," + successUser + "," + productCnt);
				}
				
				Map<String, Aggregation> viewMap = viewSr.getAggregations().asMap();
				StringTerms viewTerms = (StringTerms) viewMap.get("channelId");
				Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> viewBucketIt = viewTerms
						.getBuckets().iterator();
				
				while (viewBucketIt.hasNext()) {
					org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket viewBucket = viewBucketIt
							.next();
					
					Map<String, Aggregation> subAggMap = viewBucket.getAggregations().asMap();
					int uv = (int) ((InternalCardinality) subAggMap.get("uv")).getValue();
					int pv = (int) ((InternalValueCount) subAggMap.get("pv")).getValue();
					view.put(viewBucket.getKey().toString(), pv + "," + uv);
				}
				
				for (Map.Entry<String, Object> or : order.entrySet()) {
					Iterator<Entry<String, Object>> bat = batch1.entrySet().iterator();
					Iterator<Entry<String, Object>> log = view.entrySet().iterator();
					while (log.hasNext()) {
						Entry<String, Object> vw = log.next();
						if (vw.getKey().equals(or.getKey())) {
							AgentDataVo agentVo = new AgentDataVo();
							String[] array = (or.getValue() + "," + vw.getValue()).toString().split(",");
							agentVo.setStatTime(currentHour);
							agentVo.setOrgId(Integer.parseInt(requestMap.get("orgId").toString()));
							agentVo.setProductType(requestMap.get("productType").toString());
							agentVo.setAgentId(Integer.parseInt(requestMap.get("agentId").toString()));
							agentVo.setId(or.getKey());
							agentVo.setPv(Integer.valueOf(array[4]));
							agentVo.setUv(Integer.valueOf(array[5]));
							agentVo.setProductCnt(Integer.valueOf(array[3]));
							agentVo.setOrderCnt(Integer.valueOf(array[1]));
							agentVo.setSuccessUser(Integer.valueOf(array[2]));
							order_amount = Double.valueOf(array[0].toString());
							while (bat.hasNext()) {
								Entry<String, Object> batEntry = bat.next();
								if (or.getKey().equals(batEntry.getKey())) {
									order_amount = (double) batEntry.getValue() + Double.valueOf(array[0].toString());
								} 
							}
							agentVo.setOrderAmount(df.format(order_amount));
							agentVo.setConversionRate(df.format((Double.valueOf(array[2]) / Double.valueOf(array[5]))));
							totalList.add(agentVo);
							log.remove();
						}
					}
				}
				
				for (Map.Entry<String, Object> log1 : view.entrySet()) {
					AgentDataVo agentVo = new AgentDataVo();
					String[] array = log1.getValue().toString().split(",");
					agentVo.setStatTime(currentHour);
					agentVo.setOrgId(Integer.parseInt(requestMap.get("orgId").toString()));
					agentVo.setProductType(requestMap.get("productType").toString());
					agentVo.setAgentId(Integer.parseInt(requestMap.get("agentId").toString()));
					agentVo.setId(log1.getKey());
					agentVo.setPv(Integer.parseInt(array[0].toString()));
					agentVo.setUv(Integer.parseInt(array[1].toString()));
					agentVo.setOrderCnt(0);
					agentVo.setOrderAmount("0");
					agentVo.setConversionRate("0");
					totalList.add(agentVo);
				}
			
			}
			
			Iterator<AgentDataVo> tlIt = totalList.iterator();
			while (tlIt.hasNext()) {
				AgentDataVo totalVo = (AgentDataVo) tlIt.next();
				Iterator<AgentDataVo> tvIt = totalList1.iterator();
				while (tvIt.hasNext()) {
					AgentDataVo totalVo2 = (AgentDataVo) tvIt.next();
					if (totalVo.getStatTime().equals(totalVo2.getStatTime())) {
						tvIt.remove();
					}
				}
			}
			totalList.addAll(totalList1);
		} else {
			if (DateUtil.dateAdd(DateUtil.dateStampToDay(System.currentTimeMillis()))
					.equals(requestMap.get("endDate"))) {

				requestMap.put("today", DateUtil.dateStampToDay(System.currentTimeMillis()));

				SearchResponse orderSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderAgentTodayBqb(requestMap))
						.addAggregation(AggregationBuilders.terms("channelId").field("channel_id")
								.subAggregation(AggregationBuilders.sum("orderAmount").field("order_amount"))
								.subAggregation(AggregationBuilders.count("orderCnt").field("batch_id"))
								.subAggregation(AggregationBuilders.cardinality("successUser").field("user_id"))
								.subAggregation(AggregationBuilders.sum("productCnt").field("product_num")))
						.get();

				SearchResponse viewSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderAgentTodayViewBqb(requestMap))
						.addAggregation(AggregationBuilders.terms("channelId").field("channel_id")
								.subAggregation(AggregationBuilders.count("pv").field("user_id"))
								.subAggregation(AggregationBuilders.cardinality("uv").field("user_id"))).get();

				SearchResponse freightSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderAgentTodayBqb(requestMap))
						.addAggregation(AggregationBuilders.terms("batch_id").field("batch_id").size(10000)
								.subAggregation(AggregationBuilders.terms("freight").field("freight_money"))
								.subAggregation(AggregationBuilders.sum("freightAmount").field("freight_money")))
						.get();
				
				SearchResponse batchSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderAgentTodayBqb(requestMap))
						.addAggregation(EsUtil.orderBatchAb(requestMap)).get();
				
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
				
				Map<String, Aggregation> batchtMap = batchSr.getAggregations().asMap();
				StringTerms batchTerms = (StringTerms) batchtMap.get("batch_id");
				Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> batchBucketIt = batchTerms
						.getBuckets().iterator();
				
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
				
				Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();
				StringTerms orderTerms = (StringTerms) orderMap.get("channelId");
				Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> orderBucketIt = orderTerms
						.getBuckets().iterator();
				
				while (orderBucketIt.hasNext()) {
					org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket orderBucket = orderBucketIt
							.next();
					
					Map<String, Aggregation> subAggMap = orderBucket.getAggregations().asMap();
					double orderAmount = (Double) ((InternalSum) subAggMap.get("orderAmount")).getValue();
					int orderCnt = (int) ((InternalValueCount) subAggMap.get("orderCnt")).getValue();
					int successUser = (int) ((InternalCardinality) subAggMap.get("successUser")).getValue();
					int productCnt = (int) ((InternalSum) subAggMap.get("productCnt")).getValue();
					order.put(orderBucket.getKey().toString(), orderAmount + "," + orderCnt + "," + successUser + "," + productCnt);
				}
				
				Map<String, Aggregation> viewMap = viewSr.getAggregations().asMap();
				StringTerms viewTerms = (StringTerms) viewMap.get("channelId");
				Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> viewBucketIt = viewTerms
						.getBuckets().iterator();
				
				while (viewBucketIt.hasNext()) {
					org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket viewBucket = viewBucketIt
							.next();
					
					Map<String, Aggregation> subAggMap = viewBucket.getAggregations().asMap();
					int uv = (int) ((InternalCardinality) subAggMap.get("uv")).getValue();
					int pv = (int) ((InternalValueCount) subAggMap.get("pv")).getValue();
					view.put(viewBucket.getKey().toString(), pv + "," + uv);
				}
				
				for (Map.Entry<String, Object> or : order.entrySet()) {
					Iterator<Entry<String, Object>> bat = batch1.entrySet().iterator();
					Iterator<Entry<String, Object>> log = view.entrySet().iterator();
					while (log.hasNext()) {
						Entry<String, Object> vw = log.next();
						if (vw.getKey().equals(or.getKey())) {
							AgentDataVo agentVo = new AgentDataVo();
							String[] array = (or.getValue() + "," + vw.getValue()).toString().split(",");
							agentVo.setStatTime(DateUtil.dateStampToDay(System.currentTimeMillis()));
							agentVo.setOrgId(Integer.parseInt(requestMap.get("orgId").toString()));
							agentVo.setProductType(requestMap.get("productType").toString());
							agentVo.setAgentId(Integer.parseInt(requestMap.get("agentId").toString()));
							agentVo.setId(or.getKey());
							agentVo.setPv(Integer.valueOf(array[4]));
							agentVo.setUv(Integer.valueOf(array[5]));
							agentVo.setProductCnt(Integer.valueOf(array[3]));
							agentVo.setOrderCnt(Integer.valueOf(array[1]));
							agentVo.setSuccessUser(Integer.valueOf(array[2]));
							order_amount = Double.valueOf(array[0].toString());
							while (bat.hasNext()) {
								Entry<String, Object> batEntry = bat.next();
								if (or.getKey().equals(batEntry.getKey())) {
									order_amount = (double) batEntry.getValue() + Double.valueOf(array[0].toString());
								} 
							}
							agentVo.setOrderAmount(df.format(order_amount));
							agentVo.setConversionRate(df.format((Double.valueOf(array[2]) / Double.valueOf(array[5]))));
							totalList.add(agentVo);
							log.remove();
						}
					}
				}
				
				for (Map.Entry<String, Object> log1 : view.entrySet()) {
					AgentDataVo agentVo = new AgentDataVo();
					String[] array = log1.getValue().toString().split(",");
					agentVo.setStatTime(DateUtil.dateStampToDay(System.currentTimeMillis()));
					agentVo.setOrgId(Integer.parseInt(requestMap.get("orgId").toString()));
					agentVo.setProductType(requestMap.get("productType").toString());
					agentVo.setAgentId(Integer.parseInt(requestMap.get("agentId").toString()));
					agentVo.setId(log1.getKey());
					agentVo.setPv(Integer.parseInt(array[0].toString()));
					agentVo.setUv(Integer.parseInt(array[1].toString()));
					agentVo.setOrderCnt(0);
					agentVo.setOrderAmount("0");
					agentVo.setConversionRate("0");
					totalList.add(agentVo);
				}
			}
			SearchResponse sr = EsClient.getConnect().prepareSearch("bi_day_agent_data")
					.setQuery(EsUtil.orderAgentFromBqb(requestMap)).setSize(10000).get();
			for (SearchHit srHit : sr.getHits().getHits()) {
				AgentDataVo agentVo = new AgentDataVo();
				agentVo.setId(srHit.getSourceAsMap().get("id").toString());
				agentVo.setStatTime(srHit.getSourceAsMap().get("stat_time").toString());
				agentVo.setUv(Integer.parseInt(srHit.getSourceAsMap().get("uv").toString()));
				agentVo.setPv(Integer.parseInt(srHit.getSourceAsMap().get("pv").toString()));
				agentVo.setOrderAmount(df.format(Double.parseDouble(srHit.getSourceAsMap().get("order_amount").toString())));
				agentVo.setOrderCnt(Integer.parseInt(srHit.getSourceAsMap().get("order_count").toString()));
				agentVo.setSuccessUser(Integer.parseInt(srHit.getSourceAsMap().get("success_user").toString()));
				agentVo.setConversionRate(
						df.format(Double.valueOf(srHit.getSourceAsMap().get("success_user").toString())
								/ (0 == Integer.parseInt(srHit.getSourceAsMap().get("uv").toString()) ? 1 
										: Integer.parseInt(srHit.getSourceAsMap().get("uv").toString()))));
				agentVo.setProductCnt(Integer.parseInt(srHit.getSourceAsMap().get("product_number").toString()));
				totalList.add(agentVo);
			}
		}
		return totalList;
	}
	
	@Override
	public List<AgentDataVo> getAgentClientData(Map<String, Object> requestMap) {
		double freight_money = 0;
		double order_amount = 0;
		List<AgentDataVo> totalList = new ArrayList<AgentDataVo>();
		List<AgentDataVo> totalList1 = new ArrayList<AgentDataVo>();
		Map<String, Object> batch = new HashMap<String, Object>();
		Map<String, Object> batch1 = new HashMap<String, Object>();
		Map<String, Object> freight = new HashMap<String, Object>();
		Map<String, Object> order = new HashMap<String, Object>();
		Map<String, Object> view = new HashMap<String, Object>();
		//查询小时索引
		if (DateUtil.dateAdd(requestMap.get("startDate").toString()).equals(requestMap.get("endDate"))) {
			for (int i = 0; i < 24; i++) {
				AgentDataVo tv = new AgentDataVo();
				if (i < 10) {
					tv.setStatTime(DateUtil.toDate(requestMap.get("startDate").toString()) + " 0" + i + ":00:00");
				} else {
					tv.setStatTime(DateUtil.toDate(requestMap.get("startDate").toString()) + " " + i + ":00:00");
				}
				totalList1.add(tv);
			}
			SearchResponse sr = EsClient.getConnect().prepareSearch("bi_hour_agent_data")
					.setQuery(EsUtil.orderAgentclientBqb(requestMap)).setFrom(0).setSize(24).get();
			for (SearchHit srHit : sr.getHits().getHits()) {
				AgentDataVo agentVo = new AgentDataVo();
				agentVo.setId(srHit.getSourceAsMap().get("id").toString());
				agentVo.setStatTime(srHit.getSourceAsMap().get("stat_time").toString());
				agentVo.setUv(Integer.parseInt(srHit.getSourceAsMap().get("uv").toString()));
				agentVo.setPv(Integer.parseInt(srHit.getSourceAsMap().get("pv").toString()));
				agentVo.setOrderAmount(df.format(Double.parseDouble(srHit.getSourceAsMap().get("order_amount").toString())));
				agentVo.setOrderCnt(Integer.parseInt(srHit.getSourceAsMap().get("order_count").toString()));
				agentVo.setSuccessUser(Integer.parseInt(srHit.getSourceAsMap().get("success_user").toString()));
				agentVo.setConversionRate(
						df.format(Double.valueOf(srHit.getSourceAsMap().get("success_user").toString())
								/ (0 == Integer.parseInt(srHit.getSourceAsMap().get("uv").toString()) ? 1 
										: Integer.parseInt(srHit.getSourceAsMap().get("uv").toString()))));
				agentVo.setProductCnt(Integer.parseInt(srHit.getSourceAsMap().get("product_number").toString()));
				totalList.add(agentVo);
			}
			
			if (DateUtil.dateAdd(DateUtil.dateStampToDay(System.currentTimeMillis()))
					.equals(requestMap.get("endDate"))) {
						//获取当前小时
				String currentHour = DateUtil.dateStampToHour(System.currentTimeMillis());
				requestMap.put("today", currentHour);
				
				SearchResponse orderSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderAgentTodayBqb(requestMap))
						.addAggregation(AggregationBuilders.terms("agentType").field("agent_type")
								.subAggregation(AggregationBuilders.sum("orderAmount").field("order_amount"))
								.subAggregation(AggregationBuilders.count("orderCnt").field("batch_id"))
								.subAggregation(AggregationBuilders.cardinality("successUser").field("user_id"))
								.subAggregation(AggregationBuilders.sum("productCnt").field("product_num"))
								).get();
						
				SearchResponse viewSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderAgentTodayViewBqb(requestMap))
						.addAggregation(AggregationBuilders.terms("agentType").field("agent_type")
								.subAggregation(AggregationBuilders.count("pv").field("user_id"))
								.subAggregation(AggregationBuilders.cardinality("uv").field("user_id")))
								.get();

				SearchResponse freightSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderAgentTodayBqb(requestMap))
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
				
				SearchResponse batchSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderAgentTodayBqb(requestMap))
						.addAggregation(EsUtil.orderClientBatchAb(requestMap)).get();
				
				Map<String, Aggregation> batchtMap = batchSr.getAggregations().asMap();
				StringTerms batchTerms = (StringTerms) batchtMap.get("batch_id");
				Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> batchBucketIt = batchTerms
						.getBuckets().iterator();
				
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
				
				Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();
				LongTerms orderTerms = (LongTerms) orderMap.get("agentType");
				Iterator<Bucket> orderBucketIt = orderTerms
						.getBuckets().iterator();
				
				while (orderBucketIt.hasNext()) {
					Bucket orderBucket = orderBucketIt.next();
					
					Map<String, Aggregation> subAggMap = orderBucket.getAggregations().asMap();
					double orderAmount = (Double) ((InternalSum) subAggMap.get("orderAmount")).getValue();
					int orderCnt = (int) ((InternalValueCount) subAggMap.get("orderCnt")).getValue();
					int successUser = (int) ((InternalCardinality) subAggMap.get("successUser")).getValue();
					int productCnt = (int) ((InternalSum) subAggMap.get("productCnt")).getValue();
					order.put(orderBucket.getKey().toString(), orderAmount + "," + orderCnt + "," + successUser + "," + productCnt);
				}
				
				Map<String, Aggregation> viewMap = viewSr.getAggregations().asMap();
				LongTerms viewTerms = (LongTerms) viewMap.get("agentType");
				Iterator<Bucket> viewBucketIt = viewTerms.getBuckets().iterator();
				
				while (viewBucketIt.hasNext()) {
					Bucket viewBucket = viewBucketIt.next();
					
					Map<String, Aggregation> subAggMap = viewBucket.getAggregations().asMap();
					int uv = (int) ((InternalCardinality) subAggMap.get("uv")).getValue();
					int pv = (int) ((InternalValueCount) subAggMap.get("pv")).getValue();
					view.put(viewBucket.getKey().toString(), pv + "," + uv);
				}
				
				for (Map.Entry<String, Object> or : order.entrySet()) {
					Iterator<Entry<String, Object>> bat = batch1.entrySet().iterator();
					Iterator<Entry<String, Object>> log = view.entrySet().iterator();
					while (log.hasNext()) {
						Entry<String, Object> vw = log.next();
						if (vw.getKey().equals(or.getKey())) {
							AgentDataVo agentVo = new AgentDataVo();
							String[] array = (or.getValue() + "," + vw.getValue()).toString().split(",");
							agentVo.setStatTime(currentHour);
							agentVo.setOrgId(Integer.parseInt(requestMap.get("orgId").toString()));
							agentVo.setProductType(requestMap.get("productType").toString());
							agentVo.setAgentId(Integer.parseInt(requestMap.get("agentId").toString()));
							agentVo.setId(or.getKey());
							agentVo.setPv(Integer.valueOf(array[4]));
							agentVo.setUv(Integer.valueOf(array[5]));
							agentVo.setProductCnt(Integer.valueOf(array[3]));
							agentVo.setOrderCnt(Integer.valueOf(array[1]));
							agentVo.setSuccessUser(Integer.valueOf(array[2]));
							order_amount = Double.valueOf(array[0].toString());
							while (bat.hasNext()) {
								Entry<String, Object> batEntry = bat.next();
								if (or.getKey().equals(batEntry.getKey())) {
									order_amount = (double) batEntry.getValue() + Double.valueOf(array[0].toString());
								} 
							}
							agentVo.setOrderAmount(df.format(order_amount));
							agentVo.setConversionRate(df.format((Double.valueOf(array[2]) / Double.valueOf(array[5]))));
							totalList.add(agentVo);
							log.remove();
						}
					}
					for (Map.Entry<String, Object> log1 : view.entrySet()) {
						AgentDataVo agentVo = new AgentDataVo();
						String[] array = log1.getValue().toString().split(",");
						agentVo.setOrgId(Integer.parseInt(requestMap.get("orgId").toString()));
						agentVo.setStatTime(currentHour);
						agentVo.setProductType(requestMap.get("productType").toString());
						agentVo.setAgentId(Integer.parseInt(requestMap.get("agentId").toString()));
						agentVo.setId(log1.getKey());
						agentVo.setPv(Integer.parseInt(array[0].toString()));
						agentVo.setUv(Integer.parseInt(array[1].toString()));
						agentVo.setSuccessUser(0);
						agentVo.setOrderCnt(0);
						agentVo.setOrderAmount("0");
						agentVo.setConversionRate("0");
						totalList.add(agentVo);
					}
				}
			
			}
			
			Iterator<AgentDataVo> tlIt = totalList.iterator();
			while (tlIt.hasNext()) {
				AgentDataVo totalVo = (AgentDataVo) tlIt.next();
				Iterator<AgentDataVo> tvIt = totalList1.iterator();
				while (tvIt.hasNext()) {
					AgentDataVo totalVo2 = (AgentDataVo) tvIt.next();
					if (totalVo.getStatTime().equals(totalVo2.getStatTime())) {
						tvIt.remove();
					}
				}
			}
			totalList.addAll(totalList1);
		} else {
			if (DateUtil.dateAdd(DateUtil.dateStampToDay(System.currentTimeMillis()))
					.equals(requestMap.get("endDate"))) {

				requestMap.put("today", DateUtil.dateStampToDay(System.currentTimeMillis()));

				SearchResponse orderSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderAgentTodayBqb(requestMap))
						.addAggregation(AggregationBuilders.terms("agentType").field("agent_type")
								.subAggregation(AggregationBuilders.sum("orderAmount").field("order_amount"))
								.subAggregation(AggregationBuilders.count("orderCnt").field("batch_id"))
								.subAggregation(AggregationBuilders.cardinality("successUser").field("user_id"))
								.subAggregation(AggregationBuilders.sum("productCnt").field("product_num"))
								).get();
						
				SearchResponse viewSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderAgentTodayViewBqb(requestMap))
						.addAggregation(AggregationBuilders.terms("agentType").field("agent_type")
								.subAggregation(AggregationBuilders.count("pv").field("user_id"))
								.subAggregation(AggregationBuilders.cardinality("uv").field("user_id")))
								.get();

				SearchResponse freightSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderAgentTodayBqb(requestMap))
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
				
				SearchResponse batchSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderAgentTodayBqb(requestMap))
						.addAggregation(EsUtil.orderClientBatchAb(requestMap)).get();
				
				Map<String, Aggregation> batchtMap = batchSr.getAggregations().asMap();
				StringTerms batchTerms = (StringTerms) batchtMap.get("batch_id");
				Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> batchBucketIt = batchTerms
						.getBuckets().iterator();
				
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
				
				Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();
				StringTerms orderTerms = (StringTerms) orderMap.get("channelId");
				Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> orderBucketIt = orderTerms
						.getBuckets().iterator();
				
				while (orderBucketIt.hasNext()) {
					org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket orderBucket = orderBucketIt
							.next();
					
					Map<String, Aggregation> subAggMap = orderBucket.getAggregations().asMap();
					double orderAmount = (Double) ((InternalSum) subAggMap.get("orderAmount")).getValue();
					int orderCnt = (int) ((InternalValueCount) subAggMap.get("orderCnt")).getValue();
					int successUser = (int) ((InternalCardinality) subAggMap.get("successUser")).getValue();
					int productCnt = (int) ((InternalSum) subAggMap.get("productCnt")).getValue();
					order.put(orderBucket.getKey().toString(), orderAmount + "," + orderCnt + "," + successUser + "," + productCnt);
				}
				
				Map<String, Aggregation> viewMap = viewSr.getAggregations().asMap();
				StringTerms viewTerms = (StringTerms) viewMap.get("channelId");
				Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> viewBucketIt = viewTerms
						.getBuckets().iterator();
				
				while (viewBucketIt.hasNext()) {
					org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket viewBucket = viewBucketIt
							.next();
					
					Map<String, Aggregation> subAggMap = viewBucket.getAggregations().asMap();
					int uv = (int) ((InternalCardinality) subAggMap.get("uv")).getValue();
					int pv = (int) ((InternalValueCount) subAggMap.get("pv")).getValue();
					order.put(viewBucket.getKey().toString(), pv + "," + uv);
				}
				
				for (Map.Entry<String, Object> or : order.entrySet()) {
					Iterator<Entry<String, Object>> bat = batch1.entrySet().iterator();
					Iterator<Entry<String, Object>> log = view.entrySet().iterator();
					while (log.hasNext()) {
						Entry<String, Object> vw = log.next();
						if (vw.getKey().equals(or.getKey())) {
							AgentDataVo agentVo = new AgentDataVo();
							String[] array = (or.getValue() + "," + vw.getValue()).toString().split(",");
							agentVo.setStatTime(DateUtil.dateStampToDay(System.currentTimeMillis()));
							agentVo.setOrgId(Integer.parseInt(requestMap.get("orgId").toString()));
							agentVo.setProductType(requestMap.get("productType").toString());
							agentVo.setAgentId(Integer.parseInt(requestMap.get("agentId").toString()));
							agentVo.setId(or.getKey());
							agentVo.setPv(Integer.valueOf(array[4]));
							agentVo.setUv(Integer.valueOf(array[5]));
							agentVo.setProductCnt(Integer.valueOf(array[3]));
							agentVo.setOrderCnt(Integer.valueOf(array[1]));
							agentVo.setSuccessUser(Integer.valueOf(array[2]));
							order_amount = Double.valueOf(array[0].toString());
							while (bat.hasNext()) {
								Entry<String, Object> batEntry = bat.next();
								if (or.getKey().equals(batEntry.getKey())) {
									order_amount = (double) batEntry.getValue() + Double.valueOf(array[0].toString());
								} 
							}
							agentVo.setOrderAmount(df.format(order_amount));
							agentVo.setConversionRate(df.format((Double.valueOf(array[2]) / Double.valueOf(array[5]))));
							totalList.add(agentVo);
							log.remove();
						}
					}
					for (Map.Entry<String, Object> log1 : view.entrySet()) {
						AgentDataVo agentVo = new AgentDataVo();
						String[] array = log1.getValue().toString().split(",");
						agentVo.setOrgId(Integer.parseInt(requestMap.get("orgId").toString()));
						agentVo.setStatTime(DateUtil.dateStampToDay(System.currentTimeMillis()));
						agentVo.setProductType(requestMap.get("productType").toString());
						agentVo.setAgentId(Integer.parseInt(requestMap.get("agentId").toString()));
						agentVo.setId(log1.getKey());
						agentVo.setPv(Integer.parseInt(array[0].toString()));
						agentVo.setUv(Integer.parseInt(array[1].toString()));
						agentVo.setSuccessUser(0);
						agentVo.setOrderCnt(0);
						agentVo.setOrderAmount("0");
						agentVo.setConversionRate("0");
						totalList.add(agentVo);
					}
				}	
			}
			SearchResponse sr = EsClient.getConnect().prepareSearch("bi_day_agent_data")
					.setQuery(EsUtil.orderAgentclientBqb(requestMap)).setSize(10000).get();
			for (SearchHit srHit : sr.getHits().getHits()) {
				AgentDataVo agentVo = new AgentDataVo();
				agentVo.setId(srHit.getSourceAsMap().get("id").toString());
				agentVo.setStatTime(srHit.getSourceAsMap().get("stat_time").toString());
				agentVo.setUv(Integer.parseInt(srHit.getSourceAsMap().get("uv").toString()));
				agentVo.setPv(Integer.parseInt(srHit.getSourceAsMap().get("pv").toString()));
				agentVo.setOrderAmount(df.format(Double.parseDouble(srHit.getSourceAsMap().get("order_amount").toString())));
				agentVo.setOrderCnt(Integer.parseInt(srHit.getSourceAsMap().get("order_count").toString()));
				agentVo.setSuccessUser(Integer.parseInt(srHit.getSourceAsMap().get("success_user").toString()));
				agentVo.setConversionRate(
						df.format(Double.valueOf(srHit.getSourceAsMap().get("success_user").toString())
								/ (0 == Integer.parseInt(srHit.getSourceAsMap().get("uv").toString()) ? 1 
										: Integer.parseInt(srHit.getSourceAsMap().get("uv").toString()))));
				agentVo.setProductCnt(Integer.parseInt(srHit.getSourceAsMap().get("product_number").toString()));
				totalList.add(agentVo);
			}
		}
		return totalList;
	}
	
	@Override
	public List<ProductDataVo> getProductData(Map<String, Object> requestMap) {
		List<ProductDataVo> productDataList = new ArrayList<ProductDataVo>();
		SearchResponse orderSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderAgentRegionBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("productId").field("product_id").size(10000)
						.subAggregation(AggregationBuilders.terms("productSubId").field("product_sub_id")
								.subAggregation(AggregationBuilders.terms("productRelId").field("product_rel_id")
										.subAggregation(AggregationBuilders.sum("productCnt").field("product_num"))
										.subAggregation(AggregationBuilders.sum("orderAmount").field("order_amount")))))
				.get();
		
		Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();
		LongTerms orderTerms = (LongTerms) orderMap.get("productId");
		Iterator<Bucket> orderBucketIt = orderTerms.getBuckets().iterator();
		
		while (orderBucketIt.hasNext()) {
			Bucket productBucket = orderBucketIt.next();
			Map<String, Aggregation> productMap = productBucket.getAggregations().asMap();
			StringTerms productTerms = (StringTerms) productMap.get("productSubId");
			Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> productBucketIt = productTerms.getBuckets().iterator();
			while (productBucketIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket productSubBucket = productBucketIt.next();
				Map<String, Aggregation> productSubMap = productSubBucket.getAggregations().asMap();
				StringTerms productSubTerms = (StringTerms) productSubMap.get("productRelId");
				Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> productSubBucketIt = productSubTerms.getBuckets().iterator();
				while (productSubBucketIt.hasNext()) {
					ProductDataVo productVo = new ProductDataVo();
					org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket productRelBucket = productSubBucketIt.next();
					Map<String, Aggregation> productRelMap = productRelBucket.getAggregations().asMap();
					int productCnt = (int)((InternalSum)(productRelMap.get("productCnt"))).getValue();
					double orderAmount = (Double)((InternalSum)(productRelMap.get("orderAmount"))).getValue();
					productVo.setProductId(Integer.valueOf(productBucket.getKeyAsString()));
					productVo.setOrgId(Integer.parseInt(requestMap.get("orgId").toString()));
					productVo.setAgentId(Integer.parseInt(requestMap.get("agentId").toString()));
					productVo.setProductType(requestMap.get("productType").toString());
					productVo.setProductSubId(productSubBucket.getKeyAsString());
					productVo.setProductRelId(productRelBucket.getKeyAsString());
					productVo.setProductCnt(productCnt);
					productVo.setOrderAmount(String.valueOf(orderAmount));
					productDataList.add(productVo);
				}
			}
		}
		
		return productDataList;
	}
	
	@Override
	public List<MultiActivityVo> getProductDateByProductId(Map<String, Object> requestMap) {
		double freight_money = 0;
		double order_amount = 0;
		List<MultiActivityVo> activityList = new ArrayList<MultiActivityVo>();
		Map<String, Object> view = new HashMap<String, Object>();
		Map<String, Object> order = new HashMap<String, Object>();
		Map<String, Object> batch = new HashMap<String, Object>();
		Map<String, Object> freight = new HashMap<String, Object>();
		Map<String, Object> batch1 = new HashMap<String, Object>();
		//查询UV，PV
		SearchResponse viewSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.multiViewBqb(requestMap))
				.addAggregation(EsUtil.multiViewAb(requestMap))
				.get();
		
		//查询订单数、金额、商品数量
		SearchResponse orderSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderMultiBqb(requestMap))
				.addAggregation(EsUtil.orderMultiAb(requestMap))
				.get();

		//查询运费
		SearchResponse batchSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderMultiBqb(requestMap))
				.addAggregation(EsUtil.multiBatchAb(requestMap))
				.get();
		
		SearchResponse freightSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderMultiBqb(requestMap))
				.addAggregation(EsUtil.orderFreightAb(requestMap))
				.get();
		
		Map<String, Aggregation> viewMap = viewSr.getAggregations().asMap();
		LongTerms viewTerm = (LongTerms) viewMap.get("product");
		Iterator<Bucket> viewIt = viewTerm.getBuckets().iterator();
		while (viewIt.hasNext()) {
			Bucket viewBucket =  viewIt.next();
			Map<String, Aggregation> bucketMap = viewBucket.getAggregations().asMap();
			int userCnt = (int) ((InternalCardinality) bucketMap.get("uv")).getValue();
			int pv = (int)((InternalValueCount)bucketMap.get("pv")).getValue();
			view.put(viewBucket.getKeyAsString(), userCnt + "," + pv);
		}
		
		Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();
		LongTerms orderTerm = (LongTerms) orderMap.get("product");
		Iterator<Bucket> orderIt = orderTerm.getBuckets().iterator();
		while (orderIt.hasNext()) {
			Bucket orderBucket =  orderIt.next();
			Map<String, Aggregation> bucketMap = orderBucket.getAggregations().asMap();
			double orderAmount = ((InternalSum) bucketMap.get("orderAmount")).getValue();
			int orderCnt = (int) ((InternalCardinality) bucketMap.get("orderCnt")).getValue();
			int productCnt = (int) ((InternalSum)bucketMap.get("productCnt")).getValue();
			order.put(orderBucket.getKeyAsString(), orderAmount + "," + orderCnt + "," + productCnt);
		}
		
		Map<String, Aggregation> batchtMap = batchSr.getAggregations().asMap();
		StringTerms batchTerms = (StringTerms) batchtMap.get("batch_id");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> batchBucketIt = batchTerms
				.getBuckets().iterator();
		
		while (batchBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket batchBucket = batchBucketIt
					.next();
			LongTerms sTerms = (LongTerms) batchBucket.getAggregations().asMap().get("product");
			Iterator<Bucket> sIt = sTerms.getBuckets().iterator();
			while (sIt.hasNext()) {
				Bucket sBucket = sIt.next();
				batch.put(batchBucket.getKeyAsString(), sBucket.getKeyAsString());
			}
		}
		
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
		
		for (Map.Entry<String, Object> or : order.entrySet()) {
			Iterator<Entry<String, Object>> log = view.entrySet().iterator();
			Iterator<Entry<String, Object>> bat = batch1.entrySet().iterator();
			while (log.hasNext()) {
				Entry<String, Object> logEntry = log.next();
				if (logEntry.getKey().equals(or.getKey())) {
					MultiActivityVo activityVo = new MultiActivityVo();
					String[] array = (or.getValue() + "," + logEntry.getValue()).toString().split(",");
					activityVo.setProductId(Integer.valueOf(or.getKey()));
					activityVo.setUv(Integer.valueOf(array[3]));
					activityVo.setPv(Integer.valueOf(array[4]));
					activityVo.setOrderCnt(Integer.valueOf(array[1]));
					order_amount = Double.valueOf(array[0].toString());
					while (bat.hasNext()) {
						Entry<String, Object> batEntry = bat.next();
						if (or.getKey().equals(batEntry.getKey())) {
							order_amount = (double) batEntry.getValue() + Double.valueOf(array[0].toString());
						} 
					}
					activityVo.setOrderAmount(df.format(order_amount));
					activityVo.setProductCnt(Integer.valueOf(array[2]));
					activityList.add(activityVo);
					log.remove();
				}
			}
		}
		for (Map.Entry<String, Object> vw : view.entrySet()) {
			MultiActivityVo activityVo = new MultiActivityVo();
			String[] array = (vw.getValue()).toString().split(",");
			activityVo.setProductId(Integer.valueOf(vw.getKey()));
			activityVo.setUv(Integer.valueOf(array[0]));
			activityVo.setPv(Integer.valueOf(array[1]));
			activityVo.setOrderCnt(0);
			activityVo.setOrderAmount("0.00");
			activityList.add(activityVo);
		}
		return activityList;
	}
}

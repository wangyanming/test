package com.hds.cn.bi.service.impl;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms.Bucket;
import org.elasticsearch.search.aggregations.metrics.cardinality.InternalCardinality;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCount;
import org.springframework.stereotype.Service;

import com.hds.cn.bi.service.GetUserDataService;
import com.hds.cn.bi.util.DateUtil;
import com.hds.cn.bi.util.EsClient;
import com.hds.cn.bi.util.EsUtil;
import com.hds.cn.bi.vo.AgeVo;
import com.hds.cn.bi.vo.PayHabitVo;
import com.hds.cn.bi.vo.UserRegistVo;

@Service
public class IGetUserDataServiceImpl implements GetUserDataService{
	
	@Override
	public Map<String, Object> getTotalData(Map<String, Object> requestMap) {
		Map<String, Object> resultMap = new HashMap<String, Object>();
		List<Object> list = new LinkedList<Object>();
		
		SearchResponse orderSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders
						.terms("userCnt")
						.field("user_id").size(1000000))
				.get();
		
		for (SearchHit srHit : orderSr.getHits().getHits()) {
			list.add(srHit.getSourceAsMap().get("user_id"));
		}
		
		SearchResponse orderSr1 = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders
						.terms("userCnt")
						.field("user_id").size(1000000))
				.get();
		
		Terms agg = orderSr1.getAggregations().get("userCnt");
		System.out.println(agg.getBuckets().size());
		System.out.println(agg.getSumOfOtherDocCounts());
		
		//去重
		List<Object> sortedList = list.stream().distinct().collect(Collectors.toList());
		
		SearchResponse registResponse = EsClient.getConnect().prepareSearch("regist")
				.setQuery(EsUtil.signBqb(requestMap))
				.addAggregation(AggregationBuilders.count("registCnt").field("user_id"))
				.get();
		
		resultMap.put("registCnt", ((InternalValueCount)registResponse.getAggregations().asMap().get("registCnt")).getValue());
		resultMap.put("payCnt", sortedList.size());
		
		return resultMap;
	}

	@Override
	public List<UserRegistVo> getRegistFrom(Map<String, Object> requestMap) {
		List<UserRegistVo> userRegistList = new ArrayList<UserRegistVo>();
		List<UserRegistVo> totalList1 = new ArrayList<UserRegistVo>();
		if(requestMap.get("type") == "hour" && "hour".equals(requestMap.get("type"))) {
			for (int i = 0; i < 24; i++) {
				UserRegistVo ur = new UserRegistVo();
				if (i < 10) {
					ur.setDate(DateUtil.toDate(requestMap.get("startDate").toString()) + " 0" + i + ":00:00");
				} else {
					ur.setDate(DateUtil.toDate(requestMap.get("startDate").toString()) + " " + i + ":00:00");
				}
				totalList1.add(ur);
			}
			
			SearchResponse sr = EsClient.getConnect().prepareSearch("bi_user_analysis_hour")
					.setQuery(EsUtil.orderUserHourBqb(requestMap)).setFrom(0).setSize(24)
					.get();
			for (SearchHit srHit : sr.getHits().getHits()) {
				UserRegistVo userRegistVo = new UserRegistVo();
				userRegistVo.setDate(DateUtil.toTime(srHit.getSourceAsMap().get("stat_time").toString()));
				userRegistVo.setUserCnt(Integer.parseInt(srHit.getSourceAsMap().get("success_user").toString()));
				userRegistVo.setRegistCnt(Integer.parseInt(srHit.getSourceAsMap().get("regist_user").toString()));
				userRegistList.add(userRegistVo);
			}
			if(DateUtil.dateAdd(DateUtil.dateStampToDay(System.currentTimeMillis())).equals(requestMap.get("endDate"))) {
				UserRegistVo userRegistVo = new UserRegistVo();
				requestMap.put("today", DateUtil.dateStampToHour(System.currentTimeMillis()));
				SearchResponse registSr = EsClient.getConnect().prepareSearch("regist")
						.setQuery(EsUtil.signTodayBqb(requestMap))
						.addAggregation(AggregationBuilders.count("registCnt").field("user_id"))
						.get();
				
				SearchResponse orderSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderUserTodayBqb(requestMap))
						.addAggregation(AggregationBuilders.cardinality("successUser").field("user_id"))
						.get();
				userRegistVo.setDate(DateUtil.dateStampToHour(System.currentTimeMillis()));
				userRegistVo.setRegistCnt((int)((InternalValueCount)registSr.getAggregations().asMap().get("registCnt")).getValue());
				userRegistVo.setUserCnt((int)((InternalCardinality)orderSr.getAggregations().asMap().get("successUser")).getValue());
				userRegistList.add(userRegistVo);
			}
			
			Iterator<UserRegistVo> tlIt = userRegistList.iterator();
			while (tlIt.hasNext()) {
				UserRegistVo totalVo = (UserRegistVo) tlIt.next();
				Iterator<UserRegistVo> tvIt = totalList1.iterator();
				while (tvIt.hasNext()) {
					UserRegistVo totalVo2 = (UserRegistVo) tvIt.next();
					if (totalVo.getDate().equals(totalVo2.getDate())) {
						tvIt.remove();
					}
				}
			}
			userRegistList.addAll(totalList1);
		} else {
			if(DateUtil.dateAdd(DateUtil.dateStampToDay(System.currentTimeMillis())).equals(requestMap.get("endDate"))) {
				UserRegistVo userRegistVo = new UserRegistVo();
				requestMap.put("today", DateUtil.dateStampToDay(System.currentTimeMillis()));
				SearchResponse registSr = EsClient.getConnect().prepareSearch("regist")
						.setQuery(EsUtil.signTodayBqb(requestMap))
						.addAggregation(AggregationBuilders.count("registCnt").field("user_id"))
						.get();
				
				SearchResponse orderSr = EsClient.getConnect().prepareSearch("order")
						.setQuery(EsUtil.orderUserTodayBqb(requestMap))
						.addAggregation(AggregationBuilders.cardinality("successUser").field("user_id"))
						.get();
				userRegistVo.setDate(DateUtil.dateStampToDay(System.currentTimeMillis()));
				userRegistVo.setRegistCnt((int)((InternalValueCount)registSr.getAggregations().asMap().get("registCnt")).getValue());
				userRegistVo.setUserCnt((int)((InternalCardinality)orderSr.getAggregations().asMap().get("successUser")).getValue());
				userRegistList.add(userRegistVo);
			}
			
			SearchResponse sr = EsClient.getConnect().prepareSearch("bi_user_analysis_day")
					.setQuery(EsUtil.orderUserHourBqb(requestMap)).setFrom(0).setSize(9000)
					.get();
			for (SearchHit srHit : sr.getHits().getHits()) {
				UserRegistVo userRegistVo = new UserRegistVo();
				userRegistVo.setDate(DateUtil.toDate(srHit.getSourceAsMap().get("stat_time").toString()));
				userRegistVo.setUserCnt(Integer.parseInt(srHit.getSourceAsMap().get("success_user").toString()));
				userRegistVo.setRegistCnt(Integer.parseInt(srHit.getSourceAsMap().get("regist_user").toString()));
				userRegistList.add(userRegistVo);
			}
		}
		return userRegistList;
	}

	@Override
	public Map<String, Object> getRegistChannel(Map<String, Object> requestMap) {
		Map<String, Object> regist = new HashMap<String, Object>();
		//按渠道注册人数
		SearchResponse registSr = EsClient.getConnect().prepareSearch("regist")
				.setQuery(EsUtil.signBqb(requestMap))
				.addAggregation(EsUtil.registChannelAb(requestMap))
				.get();
		
		Map<String, Aggregation> registMap = registSr.getAggregations().asMap();
		
		StringTerms registTerms = (StringTerms) registMap.get("channelCnt");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> registBucketIt = registTerms.getBuckets().iterator();
		
		while(registBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket registBucket = registBucketIt.next();
			Map<String, Aggregation> subAggMap = registBucket.getAggregations().asMap();
			int registCnt = (int)((InternalValueCount) subAggMap.get("registCnt")).getValue();
			regist.put(registBucket.getKey().toString(), registCnt);
		}
		return regist;
	}

	@Override
	public Map<String, Object> getRegistRegion(Map<String, Object> requestMap) {
		Map<String, Object> regist = new HashMap<String, Object>();
		//查询每个区域的注册用户数
		SearchResponse registSr = EsClient.getConnect().prepareSearch("regist")
				.setQuery(EsUtil.signBqb(requestMap))
				.addAggregation(EsUtil.regionSignAb(requestMap))
				.get();
		
		Map<String, Aggregation> registMap = registSr.getAggregations().asMap();
		
		LongTerms registTerms = (LongTerms) registMap.get("region");
		Iterator<Bucket> registBucketIt = registTerms.getBuckets().iterator();
		
		while(registBucketIt.hasNext()) {
			Bucket stringBucket = registBucketIt.next();
			Map<String, Aggregation> subAggMap = stringBucket.getAggregations().asMap();
			int registCnt = (int)((InternalValueCount) subAggMap.get("registCnt")).getValue();
			regist.put(stringBucket.getKey().toString(), registCnt);
		}
		return regist;
	}

	@Override
	public Map<String, Object> getPayFrom(Map<String, Object> requestMap) {
		PayHabitVo pay;
		String[] str = {"user_id", "pay_way"};
		Map<String, Object> paid = new HashMap<String, Object>();
		List<PayHabitVo> payList = new ArrayList<PayHabitVo>();
		//必须为LinkedHashMap
		Map<String, Long> sortedMap = new LinkedHashMap<String, Long>();
		SearchResponse orderSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.setExplain(false)
				.setFetchSource(str, new String[] {""})
				.setSize(100000).get();
		
		for (SearchHit srHit : orderSr.getHits().getHits()) {
			pay = new PayHabitVo();
			pay.setPayWayId(srHit.getSourceAsMap().get("user_id") + "," + srHit.getSourceAsMap().get("pay_way"));
			pay.setPayWay(srHit.getSourceAsMap().get("pay_way").toString());
			payList.add(pay);
		}
		
		//按user_id+pay_way去重
		List<PayHabitVo> unique = payList.stream().collect(
                Collectors.collectingAndThen(Collectors.toCollection(
                        () -> new TreeSet<PayHabitVo>(Comparator.comparing(o -> o.payWayId))), ArrayList::new)
        );
		
		//分组
		Map<String, Long> groupMap = unique.stream()
				.collect(Collectors.groupingBy(PayHabitVo::getPayWay, Collectors.counting()));
		
		//排序,reversed降序,如果size为-1,limit前五条
		if (Integer.valueOf(requestMap.get("size").toString()) == -1) {
			groupMap.entrySet().stream().sorted(Map.Entry.<String, Long>comparingByValue().reversed()).limit(5)
			.forEachOrdered(x -> sortedMap.put(x.getKey(), x.getValue()));
		} else {
			groupMap.entrySet().stream().sorted(Map.Entry.<String, Long>comparingByValue().reversed())
			.forEachOrdered(x -> sortedMap.put(x.getKey(), x.getValue()));
		}
		
		//遍历sortedMap
		sortedMap.forEach((k, v) -> paid.put(k, v));
		
		return paid;
	}

	@Override
	public Map<String, Object> getClientFrom(Map<String, Object> requestMap) {
		Map<String, Object> order = new HashMap<String, Object>();
		PayHabitVo pay;
		List<PayHabitVo> payList = new ArrayList<PayHabitVo>();
		String[] str = {"user_id", "agent_type"};
		Map<String, Long> sortedMap = new LinkedHashMap<String, Long>();
		
		SearchResponse orderSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.setExplain(false)
				.setFetchSource(str, new String[] {""})
				.setSize(10000).get();
		
		for (SearchHit srHit : orderSr.getHits().getHits()) {
			pay = new PayHabitVo();
			pay.setPayWayId(srHit.getSourceAsMap().get("user_id") + "," + srHit.getSourceAsMap().get("agent_type"));
			pay.setPayWay(srHit.getSourceAsMap().get("agent_type").toString());
			payList.add(pay);
		}
		
		//去重
		List<PayHabitVo> uniqueList = payList.stream()
				.collect(Collectors.collectingAndThen(
						Collectors.toCollection(() -> new TreeSet<PayHabitVo>(Comparator.comparing(o -> o.payWayId))),
						ArrayList::new));
		//分组
		Map<String, Long> groupMap = uniqueList.stream()
				.collect(Collectors.groupingBy(PayHabitVo::getPayWay, Collectors.counting()));
		
		//排序,reversed降序,如果size为-1,limit前5条
		if (Integer.valueOf(requestMap.get("size").toString()) == -1) {
			groupMap.entrySet().stream().sorted(Map.Entry.<String, Long>comparingByValue().reversed()).limit(5)
			.forEachOrdered(x -> sortedMap.put(x.getKey(), x.getValue()));
		} else {
			groupMap.entrySet().stream().sorted(Map.Entry.<String, Long>comparingByValue().reversed())
			.forEachOrdered(x -> sortedMap.put(x.getKey(), x.getValue()));
		}
		
		//遍历sortedMap
		sortedMap.forEach((k, v) -> order.put(k, v));
		
		return order;
	}

	@Override
	public Map<String, Object> getConsumeCnt(Map<String, Object> requestMap) {
		List<PayHabitVo> list = new LinkedList<PayHabitVo>();
		int type1 = 0, type2 = 0, type3 = 0, type4 = 0, type5 = 0;
		Map<String, Object> ord = new HashMap<String, Object>();
		//查询每个终端的订单金额、订单数量、下单人数
		SearchResponse orderSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("userId").field("user_id").size(100000)
						.subAggregation(AggregationBuilders.count("userCnt").field("user_id")))
				.get();
		
		Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();
		
		StringTerms orderTerms = (StringTerms) orderMap.get("userId");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> orderBucketIt = orderTerms.getBuckets().iterator();
		while(orderBucketIt.hasNext()) {
			PayHabitVo pay = new PayHabitVo();
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket orderBucket = orderBucketIt.next();
			Map<String, Aggregation> subAggMap = orderBucket.getAggregations().asMap();
			String userCnt = String.valueOf(((InternalValueCount) subAggMap.get("userCnt")).getValue());
			//order.put(orderBucket.getKey().toString(), userCnt);
			pay.setPayWayId(orderBucket.getKey().toString());
			pay.setPayWay(userCnt);
			list.add(pay);
		}
		
		//分组，计数
		Map<String, Long> countMap = list.stream().collect(Collectors.groupingBy(PayHabitVo::getPayWay, Collectors.counting()));
		
		for (Map.Entry<String, Long> or : countMap.entrySet()) {
			if(Integer.parseInt(or.getKey().toString()) < 3) {
				type1 += or.getValue();
			} else if(Integer.parseInt(or.getKey().toString()) >= 3 && Integer.parseInt(or.getKey().toString()) < 6) {
				type2 += or.getValue();
			} else if(Integer.parseInt(or.getKey().toString()) >= 6 && Integer.parseInt(or.getKey().toString()) < 11) {
				type3 += or.getValue();
			} else if(Integer.parseInt(or.getKey().toString()) >= 11 && Integer.parseInt(or.getKey().toString()) < 31) {
				type4 += or.getValue();
			} else if(Integer.parseInt(or.getKey().toString()) >= 31) {
				type5 += or.getValue();
			}
		}
		ord.put("type1", type1);
		ord.put("type2", type2);
		ord.put("type3", type3);
		ord.put("type4", type4);
		ord.put("type5", type5);
		return ord;
	}
	
	@Override
	public Map<String, Object> getConsumeAmount(Map<String, Object> requestMap) {
		int type1 = 0, type2 = 0, type3 = 0, type4 = 0, type5 = 0;
		Map<String, Object> order = new HashMap<String, Object>();
		Map<String, Object> ord = new HashMap<String, Object>();
		//查询每个终端的订单金额、订单数量、下单人数
		SearchResponse orderSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("userId").field("user_id").size(10000)
						.subAggregation(AggregationBuilders.sum("orderAmount").field("order_amount")))
				.get();
		
		Map<String, Aggregation> orderMap = orderSr.getAggregations().asMap();
		
		StringTerms orderTerms = (StringTerms) orderMap.get("userId");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket> orderBucketIt = orderTerms.getBuckets().iterator();
		while(orderBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket orderBucket = orderBucketIt.next();
			Map<String, Aggregation> subAggMap = orderBucket.getAggregations().asMap();
			//sum值获取方法
			//cardinality值获取方法
			int userCnt = (int) ((InternalSum) subAggMap.get("orderAmount")).getValue();
			//count值获取方法
			order.put(orderBucket.getKey().toString(), userCnt);
		}
		for (Map.Entry<String, Object> or : order.entrySet()) {
			if(Integer.parseInt(or.getValue().toString()) < 1001) {
				type1 ++;
			} else if(Integer.parseInt(or.getValue().toString()) >= 1001 && Integer.parseInt(or.getValue().toString()) < 2001) {
				type2 ++;
			} else if(Integer.parseInt(or.getValue().toString()) >= 2001 && Integer.parseInt(or.getValue().toString()) < 3001) {
				type3 ++;
			} else if(Integer.parseInt(or.getValue().toString()) >= 3001 && Integer.parseInt(or.getValue().toString()) < 5001) {
				type4 ++;
			} else if(Integer.parseInt(or.getValue().toString()) >= 5001) {
				type5 ++;
			}
		}
		ord.put("type1", type1);
		ord.put("type2", type2);
		ord.put("type3", type3);
		ord.put("type4", type4);
		ord.put("type5", type5);
		return ord;
	}

	@Override
	public Map<String, Object> getLogonCnt(Map<String, Object> requestMap) {
		Map<String, Object> order = new HashMap<String, Object>();
		List<String> list = new LinkedList<String>();
		
		SearchResponse orderSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqb(requestMap))
				.setExplain(false)
				.setFetchSource("user_id", "")
				.setSize(100000).get();
		
		for (SearchHit srHit : orderSr.getHits().getHits()) {
			list.add(srHit.getSourceAsMap().get("user_id").toString());
		}
		
		//去重
		List<String> sortedList =  list.stream().distinct().collect(Collectors.toList());
		
		//活跃用户
		SearchResponse sleepSr = EsClient.getConnect().prepareSearch("order")
				.setQuery(EsUtil.orderBqbLogon(requestMap))
				.addAggregation(AggregationBuilders.cardinality("sleepCnt").field("user_id"))
				.get();
		
		order.put("logonCnt", sortedList.size());
		
		Map<String, Aggregation> sleepMap = sleepSr.getAggregations().asMap();
		int sleepCnt = (int) ((InternalCardinality) sleepMap.get("sleepCnt")).getValue();
		order.put("sleepCnt", sleepCnt);
		
		return order;
	}
	
	@Override
	public Map<String, Object> getGenderCnt(Map<String, Object> requestMap) {
		Map<String, Object> regist = new HashMap<String, Object>();
		//查询每个区域的注册用户数
		SearchResponse registSr = EsClient.getConnect().prepareSearch("evente_user_extend")
				.setQuery(EsUtil.eventUserBqb(requestMap))
				.addAggregation(AggregationBuilders.terms("sex").field("sex")
						.subAggregation(AggregationBuilders.count("registCnt").field("id")))
				.get();
		
		Map<String, Aggregation> registMap = registSr.getAggregations().asMap();
		
		LongTerms registTerms = (LongTerms) registMap.get("sex");
		Iterator<Bucket> registBucketIt = registTerms.getBuckets().iterator();
		
		while(registBucketIt.hasNext()) {
			Bucket stringBucket = registBucketIt.next();
			Map<String, Aggregation> subAggMap = stringBucket.getAggregations().asMap();
			int registCnt = (int)((InternalValueCount) subAggMap.get("registCnt")).getValue();
			regist.put(stringBucket.getKey().toString(), registCnt);
		}
		return regist;
	}
	
	@Override
	public Map<String, Object> getAgeCnt(Map<String, Object> requestMap) {
		int birth = 0, userCnt = 0 ,type1 = 0, type2 = 0, type3 = 0, type4 = 0, type5 = 0;
		List<AgeVo> ageList = new ArrayList<AgeVo>();
		Map<String, Object> regist = new HashMap<String, Object>();
		Map<String, Object> reg = new HashMap<String, Object>();
		//查询每个区域的注册用户数
		SearchResponse registSr = EsClient.getConnect().prepareSearch("evente_user_extend")
				.setQuery(EsUtil.eventUserBqb(requestMap)).setFrom(0).setSize(9000)
				.addAggregation(AggregationBuilders.terms("age").field("birthday").format("yyyy-MM-dd")
						.subAggregation(AggregationBuilders.count("registCnt").field("id")))
				.get();
		
		Map<String, Aggregation> registMap = registSr.getAggregations().asMap();
		
		LongTerms registTerms = (LongTerms) registMap.get("age");
		Iterator<Bucket> registBucketIt = registTerms.getBuckets().iterator();
		
		while(registBucketIt.hasNext()) {
			Bucket stringBucket = registBucketIt.next();
			Map<String, Aggregation> subAggMap = stringBucket.getAggregations().asMap();
			int registCnt = (int)((InternalValueCount) subAggMap.get("registCnt")).getValue();
			regist.put(stringBucket.getKeyAsString(), registCnt);
		}
		
		for (Map.Entry<String, Object> re : regist.entrySet()) {
			AgeVo ageVo = new AgeVo();
			if(!re.getKey().equals("1900-01-01")) {
				birth = DateUtil.getAge(re.getKey().toString());
				userCnt = Integer.parseInt(re.getValue().toString());
				ageVo.setAge(birth);
				ageVo.setUserCnt(userCnt);
				ageList.add(ageVo);
			}
		}
		
		for (AgeVo ageVo : ageList) {
			if(ageVo.getAge() < 20) {
				type1 = type1 + ageVo.getUserCnt();
			} else if(ageVo.getAge() >= 20 && ageVo.getAge() < 26) {
				type2 = type2 + ageVo.getUserCnt();
			} else if(ageVo.getAge() >= 26 && ageVo.getAge() < 31) {
				type3 = type3 + ageVo.getUserCnt();
			} else if(ageVo.getAge() >= 31 && ageVo.getAge() < 41) {
				type4 = type4 + ageVo.getUserCnt();
			} else if(ageVo.getAge() >= 41) {
				type5 = type5 + ageVo.getUserCnt();
			}
		}
		reg.put("type1", type1);
		reg.put("type2", type2);
		reg.put("type3", type3);
		reg.put("type4", type4);
		reg.put("type5", type5);
		return reg;
	}
	
}

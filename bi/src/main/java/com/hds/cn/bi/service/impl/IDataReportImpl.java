package com.hds.cn.bi.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.springframework.stereotype.Service;

import com.hds.cn.bi.service.DataReportService;
import com.hds.cn.bi.util.EsClient;
import com.hds.cn.bi.util.EsUtil;
import com.hds.cn.bi.vo.LogonOrgVo;
import com.hds.cn.bi.vo.NewOrgVo;
import com.hds.cn.bi.vo.OrgRankVo;
import com.hds.cn.bi.vo.TotalOrgVo;

@Service
public class IDataReportImpl implements DataReportService{

	@Override
	public List<TotalOrgVo> getTotalData(Map<String, Object> requestMap) {
		List<TotalOrgVo> totalList = new ArrayList<TotalOrgVo>();
		
		// 查询整体数据
		SearchResponse sr = EsClient.getConnect().prepareSearch("report4hds")
				.setQuery(EsUtil.signBqb(requestMap)).setSize(10000).get();
		
		for (SearchHit srHit : sr.getHits().getHits()) {
			TotalOrgVo totalVo = new TotalOrgVo();
			totalVo.setDate(srHit.getSourceAsMap().get("stat_time").toString());
			totalVo.setSellAmount(srHit.getSourceAsMap().get("sell_amount").toString());
			totalVo.setSellCnt(Integer.parseInt(srHit.getSourceAsMap().get("sell_cnt").toString()));
			totalVo.setUv(Integer.parseInt(srHit.getSourceAsMap().get("uv").toString()));
			totalVo.setSuccessUserCnt(Integer.parseInt(srHit.getSourceAsMap().get("success_user_cnt").toString()));
			totalVo.setIncrementCnt(Integer.parseInt(srHit.getSourceAsMap().get("increment_cnt").toString()));
			totalList.add(totalVo);
		}
		
		return totalList;
	}
	
	@Override
	public List<NewOrgVo> getNewOrgData(Map<String, Object> requestMap) {
		List<NewOrgVo> list = new ArrayList<NewOrgVo>();
		return list;
	}

	@Override
	public List<LogonOrgVo> getLogonOrgData(Map<String, Object> requestMap) {
		List<LogonOrgVo> list = new ArrayList<LogonOrgVo>();
		
		return list;
	}

	@Override
	public Map<String, Object> getOrgRankData(Map<String, Object> requestMap) {
		Map<String, Object> resultMap = new HashMap<String, Object>();
		List<OrgRankVo> sellAmountList = new ArrayList<OrgRankVo>();
		List<OrgRankVo> userCntList = new ArrayList<OrgRankVo>();
		List<OrgRankVo> incrementCntList = new ArrayList<OrgRankVo>();
		
		SearchResponse sellAmountSr = EsClient.getConnect().prepareSearch("report4vip")
				.setQuery(QueryBuilders.boolQuery()
						.must(QueryBuilders.matchQuery("time_dim", requestMap.get("dateType")))
						.must(QueryBuilders.rangeQuery("stat_time.keyword").gte(requestMap.get("startDate")).lte(requestMap.get("endDate")))
						.must(QueryBuilders.matchQuery("data_dim", "sell_amount")))
				.setSize(10000).get();
		
		SearchResponse userCntSr = EsClient.getConnect().prepareSearch("report4vip")
				.setQuery(QueryBuilders.boolQuery()
						.must(QueryBuilders.matchQuery("time_dim", requestMap.get("dateType")))
						.must(QueryBuilders.rangeQuery("stat_time.keyword").gte(requestMap.get("startDate")).lte(requestMap.get("endDate")))
						.must(QueryBuilders.matchQuery("data_dim", "uv")))
				.setSize(10000).get();
		
		SearchResponse incrementCntSr = EsClient.getConnect().prepareSearch("report4vip")
				.setQuery(QueryBuilders.boolQuery()
						.must(QueryBuilders.matchQuery("time_dim", requestMap.get("dateType")))
						.must(QueryBuilders.rangeQuery("stat_time.keyword").gte(requestMap.get("startDate")).lte(requestMap.get("endDate")))
						.must(QueryBuilders.matchQuery("data_dim", "increment_cnt")))
				.setSize(10000).get();
		
		for (SearchHit srHit : userCntSr.getHits().getHits()) {
			OrgRankVo rankVo = new OrgRankVo();
			rankVo.setOrgId(Integer.parseInt(srHit.getSourceAsMap().get("org_id").toString()));
			rankVo.setRankValue(srHit.getSourceAsMap().get("data_val").toString());
			userCntList.add(rankVo);
		}
		
		for (SearchHit srHit : sellAmountSr.getHits().getHits()) {
			OrgRankVo rankVo = new OrgRankVo();
			rankVo.setOrgId(Integer.parseInt(srHit.getSourceAsMap().get("org_id").toString()));
			rankVo.setRankValue(srHit.getSourceAsMap().get("data_val").toString());
			sellAmountList.add(rankVo);
		}
		
		for (SearchHit srHit : incrementCntSr.getHits().getHits()) {
			OrgRankVo rankVo = new OrgRankVo();
			rankVo.setOrgId(Integer.parseInt(srHit.getSourceAsMap().get("org_id").toString()));
			rankVo.setRankValue(srHit.getSourceAsMap().get("data_val").toString());
			incrementCntList.add(rankVo);
		}
		
		resultMap.put("userCnt", userCntList);
		resultMap.put("sellAmount", sellAmountList);
		resultMap.put("incrementCnt", incrementCntList);
		
		return resultMap;
	}

}

package com.hds.cn.bi.service;

import java.util.List;
import java.util.Map;

import com.hds.cn.bi.vo.BuyRateVo;
import com.hds.cn.bi.vo.ClientVo;
import com.hds.cn.bi.vo.PayVo;
import com.hds.cn.bi.vo.RegionVo;
import com.hds.cn.bi.vo.SourceVo;
import com.hds.cn.bi.vo.TotalVo;

public interface GetDataByEsService {
	
	/**
	 * 数据中心-数据概览-汇总数据
	 */
	public Map<String, Object> getTotalbyDay(Map<String, Object> requestMap);
	
	/**
	 * 数据中心-数据概览-今日趋势
	 */
	public List<TotalVo> getTotalbyHour(Map<String, Object> requestMap);
	
	/**
	 * 数据中心-数据概览-访客来源
	 */
	public List<SourceVo> getVisitorFrom(Map<String, Object> requestMap);
	
	/**
	 * 数据中心-数据概览-终端来源
	 */
	public List<ClientVo> getClientFrom(Map<String, Object> requestMap);
	
	/**
	 * 数据中心-数据概览-支付习惯
	 */
	public List<PayVo> getPayFrom(Map<String, Object> requestMap);
	
	/**
	 * 数据中心-数据概览-地区分布
	 */
	public List<RegionVo> getRegionDistribution(Map<String, Object> requestMap);
	
	/**
	 * 数据中心-数据概览-转化漏斗
	 */
	public List<BuyRateVo> getBuyRate(Map<String, Object> requestMap);
	
}

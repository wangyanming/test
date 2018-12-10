package com.hds.cn.bi.service;

import java.util.List;
import java.util.Map;

import com.hds.cn.bi.vo.UserRegistVo;

public interface GetUserDataService {
	/**
	 * 数据中心-用户分析-汇总数据
	 * @param requestMap
	 * @return
	 */
	public Map<String, Object> getTotalData(Map<String, Object> requestMap);
	
	/**
	 * 数据中心-用户分析-注册来源-按天/或者小时汇总
	 * @param requestMap
	 * @return
	 */
	public List<UserRegistVo> getRegistFrom(Map<String, Object> requestMap);
	
	/**
	 * 数据中心-用户分析-注册来源-渠道
	 * @param requestMap
	 * @return
	 */
	public Map<String, Object> getRegistChannel(Map<String, Object> requestMap);
	
	/**
	 * 数据中心-用户分析-注册地域
	 * @param requestMap
	 * @return
	 */
	public Map<String, Object> getRegistRegion(Map<String, Object> requestMap);
	
	/**
	 * 数据中心-用户分析-支付习惯
	 * @param requestMap
	 * @return
	 */
	public Map<String, Object> getPayFrom(Map<String, Object> requestMap);
	
	/**
	 * 数据中心-用户分析-消费终端
	 * @param requestMap
	 * @return
	 */
	public Map<String, Object> getClientFrom(Map<String, Object> requestMap);
	
	/**
	 * 数据中心-用户分析-消费次数
	 * @param requestMap
	 * @return
	 */
	public Map<String, Object> getConsumeCnt(Map<String, Object> requestMap);
	
	/**
	 * 数据中心-用户分析-消费金额
	 * @param requestMap
	 * @return
	 */
	public Map<String, Object> getConsumeAmount(Map<String, Object> requestMap);
	
	/**
	 * 数据中心-用户分析-访问活跃度
	 * @param request
	 * @return
	 */
	public Map<String, Object> getLogonCnt(Map<String, Object> requestMap);
	
	/**
	 * 数据中心-用户分析-性别
	 * @param request
	 * @return
	 */
	public Map<String, Object> getGenderCnt(Map<String, Object> requestMap);
	
	/**
	 * 数据中心-用户分析-年龄
	 * @param request
	 * @return
	 */
	public Map<String, Object> getAgeCnt(Map<String, Object> requestMap);
}

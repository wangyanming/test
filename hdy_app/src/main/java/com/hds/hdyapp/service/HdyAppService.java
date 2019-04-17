package com.hds.hdyapp.service;

import java.util.List;
import java.util.Map;

import com.hds.hdyapp.vo.BestSellingVo;
import com.hds.hdyapp.vo.ClientCountVo;
import com.hds.hdyapp.vo.DataSourceVo;
import com.hds.hdyapp.vo.ProductDataVo;
import com.hds.hdyapp.vo.RegionDistributionVo;
import com.hds.hdyapp.vo.TotalDataVo;
import com.hds.hdyapp.vo.UserDistributionVo;
import com.hds.hdyapp.vo.UserPayWayVo;

public interface HdyAppService {
	
	/**
	 * 活动易APP-获取汇总数据
	 * @param requestMap
	 * @return
	 */
	public Map<String, Object> getTotalData(Map<String, Object> requestMap);
	
	/**
	 * 活动易APP-获取按日分组汇总数据
	 * @param requestMap
	 * @return
	 */
	public List<TotalDataVo> getTotalDataByDay(Map<String, Object> requestMap);
	
	/**
	 * 活动易APP-获取按周分组汇总数据
	 * @param requestMap
	 * @return
	 */
	public List<TotalDataVo> getTotalDataByWeek(Map<String, Object> requestMap);
	
	/**
	 * 活动易APP-获取按月分组汇总数据
	 * @param requestMap
	 * @return
	 */
	public List<TotalDataVo> getTotalDataByMonth(Map<String, Object> requestMap);
	
	/**
	 * 活动易APP-获取按年分组汇总数据
	 * @param requestMap
	 * @return
	 */
	public List<TotalDataVo> getTotalDataByYear(Map<String, Object> requestMap);
	
	/**
	 * 活动易APP-来源统计
	 * @param requestMap
	 * @return
	 */
	public List<DataSourceVo> getDataSourceByDate(Map<String, Object> requestMap);
	
	/**
	 * 活动易APP-来源统计-指定渠道统计
	 * @param requestMap
	 * @return
	 */
	public List<ProductDataVo> getSourceDataByChannelId(Map<String, Object> requestMap);
	
	/**
	 * 活动易APP-地域分布
	 * @param requestMap
	 * @return
	 */
	public List<RegionDistributionVo> getRegionDistributionByDate(Map<String, Object> requestMap);
	
	/**
	 * 活动易APP-地域分布-指定省市数据
	 * @param requestMap
	 * @return
	 */
	public List<ProductDataVo> getRegionDateByRegionId(Map<String, Object> requestMap);
	
	/**
	 * 活动易APP-使用终端统计
	 * @param requestMap
	 * @return
	 */
	public List<ClientCountVo> getClientDataByDate(Map<String, Object> requestMap);
	
	/**
	 * 活动易APP-使用终端统计-指定终端统计
	 * @param requestMap
	 * @return
	 */
	public List<ProductDataVo> getClientDataByClientId(Map<String, Object> requestMap);
	
	/**
	 * 活动易APP-用户数据统计
	 * @param requestMap
	 * @return
	 */
	public Map<String, Object> getUserDataByDate(Map<String, Object> requestMap);
	
	/**
	 * 活动易APP-用户省份分布
	 * @param requestMap
	 * @return
	 */
	public List<UserDistributionVo> getUserDistributionByDate(Map<String, Object> requestMap);
	
	/**
	 * 活动易APP-用户支付方式分布
	 * @param requestMap
	 * @return
	 */
	public List<UserPayWayVo> getUserPayWayByDate(Map<String, Object> requestMap);
	
	/**
	 * 活动易APP-畅销排行
	 * @param requestMap
	 * @return
	 */
	public List<BestSellingVo> getBestSellingByDate(Map<String, Object> requestMap);
	
	/**
	 * 活动易APP-活动统计-概览
	 * @param requestMap
	 * @return
	 */
	public Map<String, Object> getProductData(Map<String, Object> requestMap);
	
	/**
	 * 互动易APP-活动统计-来源链接
	 * @param requestMap
	 * @return
	 */
	public List<DataSourceVo> getDataSourceByProductId(Map<String, Object> requestMap);
	
	/**
	 * 活动易APP-活动统计-地域分布
	 * @param requestMap
	 * @return
	 */
	public List<RegionDistributionVo> getRegionDistributionByProductId(Map<String, Object> requestMap);
	
	/**
	 * 活动易APP-活动统计-使用终端统计
	 * @param requestMap
	 * @return
	 */
	public List<ClientCountVo> getClientDataByProductId(Map<String, Object> requestMap);
}

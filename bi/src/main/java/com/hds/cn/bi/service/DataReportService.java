package com.hds.cn.bi.service;

import java.util.List;
import java.util.Map;

import com.hds.cn.bi.vo.LogonOrgVo;
import com.hds.cn.bi.vo.NewOrgVo;
import com.hds.cn.bi.vo.TotalOrgVo;

public interface DataReportService {
	
	/**
	 * 整体数据-月度运营数据
	 * @param requestMap
	 * @return
	 */
	public List<TotalOrgVo> getTotalData(Map<String, Object> requestMap);
	
	/**
	 * 主办数据统计-主办新增数据
	 * @param requestMap
	 * @return
	 */
	public List<NewOrgVo> getNewOrgData(Map<String, Object> requestMap);
	
	/**
	 * 主办数据统计-主办活跃数据
	 * @param requestMap
	 * @return
	 */
	public List<LogonOrgVo> getLogonOrgData(Map<String, Object> requestMap);
	
	/**
	 * VIP客户统计
	 * @param requestMap
	 * @return
	 */
	public Map<String, Object> getOrgRankData(Map<String, Object> requestMap);
}

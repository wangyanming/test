package com.hds.cn.bi.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.hds.cn.bi.service.DataReportService;
import com.hds.cn.bi.util.BaseController;
import com.hds.cn.bi.util.RequestUtil;
import com.hds.cn.bi.util.ResponseUtil;
import com.hds.cn.bi.vo.LogonOrgVo;
import com.hds.cn.bi.vo.NewOrgVo;
import com.hds.cn.bi.vo.TotalOrgVo;

import net.sf.json.JSONObject;

@Controller
@RequestMapping("report")
public class DataReport extends BaseController{
	Logger logger = LoggerFactory.getLogger(this.getClass());
	private String mseg = "";
	@Autowired
	private DataReportService dataReprotService;
	
	@ResponseBody
	@RequestMapping(value = "getTotalData", method = RequestMethod.POST)
	public ResponseUtil<List<TotalOrgVo>> getTotalData(@RequestBody String info) {
		List<TotalOrgVo> totalList = new ArrayList<TotalOrgVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			totalList = dataReprotService.getTotalData(requestMap);
			logger.info("requestMap:" + info.toString() + ";" + "\r\n" + "getTotalData" + ":" + totalList);
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("totalList:" + info + ";" + "totalList" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, totalList);
	}
	
	@ResponseBody
	@RequestMapping(value = "getNewOrgData", method = RequestMethod.POST)
	public ResponseUtil<List<NewOrgVo>> getNewOrgData(@RequestBody String info) {
		List<NewOrgVo> totalList = new ArrayList<NewOrgVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requesHourtUtil(jsonObject);
			totalList = dataReprotService.getNewOrgData(requestMap);
			logger.info("requestMap:" + info.toString() + ";" + "\r\n" + "getNewOrgData" + ":" + totalList.toString());
			logger.info("getNewOrgData" + ":" + totalList.toString());
			mseg = "success";
		} catch (Exception e) {
			logger.error("getNewOrgData" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, totalList);
	}
	
	@ResponseBody
	@RequestMapping(value = "getLogonOrgData", method = RequestMethod.POST)
	public ResponseUtil<List<LogonOrgVo>> getLogonOrgData(@RequestBody String info) {
		List<LogonOrgVo> totalList = new ArrayList<LogonOrgVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requesHourtUtil(jsonObject);
			totalList = dataReprotService.getLogonOrgData(requestMap);
			logger.info("requestMap:" + info.toString() + ";" + "\r\n" + "getLogonOrgData" + ":" + totalList.toString());
			logger.info("getLogonOrgData" + ":" + totalList.toString());
			mseg = "success";
		} catch (Exception e) {
			logger.error("getLogonOrgData" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, totalList);
	}
	
	@ResponseBody
	@RequestMapping(value = "getOrgRankData", method = RequestMethod.POST)
	public ResponseUtil<Map<String, Object>> getOrgRankData(@RequestBody String info) {
		Map<String, Object> resultMap = new HashMap<String, Object>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requesHourtUtil(jsonObject);
			resultMap = dataReprotService.getOrgRankData(requestMap);
			logger.info("requestMap:" + info.toString() + ";" + "\r\n" + "getOrgRankData" + ":" + resultMap.toString());
			logger.info("getOrgRankData" + ":" + resultMap.toString());
			mseg = "success";
		} catch (Exception e) {
			logger.error("getOrgRankData" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, resultMap);
	}
	
}

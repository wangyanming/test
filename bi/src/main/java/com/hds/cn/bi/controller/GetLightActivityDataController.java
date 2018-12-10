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

import com.alibaba.fastjson.JSON;
import com.hds.cn.bi.service.GetLightActivityDataService;
import com.hds.cn.bi.util.BaseController;
import com.hds.cn.bi.util.RequestUtil;
import com.hds.cn.bi.util.ResponseUtil;
import com.hds.cn.bi.vo.AgentDataVo;
import com.hds.cn.bi.vo.AgentVo;
import com.hds.cn.bi.vo.MultiActivityVo;
import com.hds.cn.bi.vo.ProductDataVo;

import net.sf.json.JSONObject;

@Controller
@RequestMapping("lightActivity")
public class GetLightActivityDataController extends BaseController{
	Logger logger = LoggerFactory.getLogger(this.getClass());
	private String mseg = "";
	
	@Autowired
	private GetLightActivityDataService lightActivityService;
	
	@ResponseBody
	@RequestMapping(value = "getMultiActivity", method = RequestMethod.POST)
	public ResponseUtil<List<MultiActivityVo>> getMultiActivity(@RequestBody String info) {
		List<MultiActivityVo> activityList = new ArrayList<MultiActivityVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			activityList = lightActivityService.getMultiActivity(requestMap);
			logger.info("入参：{}", info );
			logger.info("响应数据：{}", JSON.toJSONString(activityList));
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("requestMap:" + info + ";" + "getMultiActivity" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, activityList);
	}
	
	@ResponseBody
	@RequestMapping(value = "getTotalData", method = RequestMethod.POST)
	public ResponseUtil<Map<String, Object>> getTotalData(@RequestBody String info) {
		Map<String, Object> resultMap = new HashMap<String, Object>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			resultMap = lightActivityService.getTotalData(requestMap);
			logger.info("入参：{}", info );
			logger.info("响应数据：{}", JSON.toJSONString(resultMap));
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("requestMap:" + info + ";" + "getTotalData" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, resultMap);
	}
	
	@ResponseBody
	@RequestMapping(value = "getAgentData", method = RequestMethod.POST)
	public ResponseUtil<List<AgentVo>> getAgentData(@RequestBody String info) {
		List<AgentVo> agentList = new ArrayList<AgentVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			agentList = lightActivityService.getAgentData(requestMap);
			logger.info("入参：{}", info );
			logger.info("响应数据：{}", JSON.toJSONString(agentList));
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("requestMap:" + info + ";" + "getAgentData" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, agentList);
	}
	
	@ResponseBody
	@RequestMapping(value = "getAgentRegionData", method = RequestMethod.POST)
	public ResponseUtil<List<AgentDataVo>> getAgentRegionData(@RequestBody String info) {
		List<AgentDataVo> agentList = new ArrayList<AgentDataVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			agentList = lightActivityService.getAgentRegionData(requestMap);
			logger.info("入参：{}", info );
			logger.info("响应数据：{}", JSON.toJSONString(agentList));
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("requestMap:" + info + ";" + "getAgentRegionData" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, agentList);
	}
	
	@ResponseBody
	@RequestMapping(value = "getAgentSaleData", method = RequestMethod.POST)
	public ResponseUtil<List<AgentDataVo>> getAgentSaleData(@RequestBody String info) {
		List<AgentDataVo> agentList = new ArrayList<AgentDataVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			agentList = lightActivityService.getAgentSaleData(requestMap);
			logger.info("入参：{}", info );
			logger.info("响应数据：{}", JSON.toJSONString(agentList));
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("requestMap:" + info + ";" + "getAgentSaleData" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, agentList);
	}
	
	@ResponseBody
	@RequestMapping(value = "getAgentFromData", method = RequestMethod.POST)
	public ResponseUtil<List<AgentDataVo>> getAgentFromData(@RequestBody String info) {
		List<AgentDataVo> agentList = new ArrayList<AgentDataVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			agentList = lightActivityService.getAgentFromData(requestMap);
			logger.info("入参：{}", info );
			logger.info("响应数据：{}", JSON.toJSONString(agentList));
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("requestMap:" + info + ";" + "getAgentFromData" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, agentList);
	}
	
	@ResponseBody
	@RequestMapping(value = "getAgentClientData", method = RequestMethod.POST)
	public ResponseUtil<List<AgentDataVo>> getAgentClientData(@RequestBody String info) {
		List<AgentDataVo> agentList = new ArrayList<AgentDataVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			agentList = lightActivityService.getAgentClientData(requestMap);
			logger.info("入参：{}", info );
			logger.info("响应数据：{}", JSON.toJSONString(agentList));
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("requestMap:" + info + ";" + "getAgentClientData" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, agentList);
	}
	
	@ResponseBody
	@RequestMapping(value = "getProductData", method = RequestMethod.POST)
	public ResponseUtil<List<ProductDataVo>> getProductData(@RequestBody String info) {
		List<ProductDataVo> agentList = new ArrayList<ProductDataVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			agentList = lightActivityService.getProductData(requestMap);
			logger.info("入参：{}", info );
			logger.info("响应数据：{}", JSON.toJSONString(agentList));
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("requestMap:" + info + ";" + "getProductData" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, agentList);
	}
	
	@ResponseBody
	@RequestMapping(value = "getProductDateByProductId", method = RequestMethod.POST)
	public ResponseUtil<List<MultiActivityVo>> getProductDateByProductId(@RequestBody String info) {
		List<MultiActivityVo> activityList = new ArrayList<MultiActivityVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			activityList = lightActivityService.getProductDateByProductId(requestMap);
			logger.info("入参：{}", info );
			logger.info("响应数据：{}", JSON.toJSONString(activityList));
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("requestMap:" + info + ";" + "getMultiActivity" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, activityList);
	}
}

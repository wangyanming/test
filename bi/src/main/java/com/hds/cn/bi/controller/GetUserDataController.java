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

import com.hds.cn.bi.service.GetUserDataService;
import com.hds.cn.bi.util.BaseController;
import com.hds.cn.bi.util.RequestUtil;
import com.hds.cn.bi.util.ResponseUtil;
import com.hds.cn.bi.vo.UserRegistVo;

import net.sf.json.JSONObject;

@Controller
@RequestMapping("userData")
public class GetUserDataController extends BaseController{
	Logger logger = LoggerFactory.getLogger(this.getClass());
	private String mseg = "";
	@Autowired
	private GetUserDataService getUserDataService;
	
	@ResponseBody
	@RequestMapping(value = "getTotalData", method = RequestMethod.POST)
	public ResponseUtil<Map<String, Object>> getTotalData(@RequestBody String info) {
		Map<String, Object> resultMap = new HashMap<String, Object>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			resultMap = getUserDataService.getTotalData(requestMap);
			logger.info("getTotalData：" + resultMap.toString());
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("getTotalData：" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, resultMap);
	}
	
	@ResponseBody
	@RequestMapping(value = "getRegistFrom", method = RequestMethod.POST)
	public ResponseUtil<List<UserRegistVo>> getRegistFrom(@RequestBody String info) {
		List<UserRegistVo> userRegistList = new ArrayList<UserRegistVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requesHourtUtil(jsonObject);
			userRegistList = getUserDataService.getRegistFrom(requestMap);
			logger.info("getRegistFrom：" + userRegistList.toString());
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("getRegistFrom：" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, userRegistList);
	}
	
	@ResponseBody
	@RequestMapping(value = "getRegistChannel", method = RequestMethod.POST)
	public ResponseUtil<Map<String, Object>> getRegistChannel(@RequestBody String info) {
		Map<String, Object> resultMap = new HashMap<String, Object>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			resultMap = getUserDataService.getRegistChannel(requestMap);
			logger.info("getRegistChannel：" + resultMap.toString());
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("getRegistChannel：" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, resultMap);
	}
	
	@ResponseBody
	@RequestMapping(value = "getRegistRegion", method = RequestMethod.POST)
	public ResponseUtil<Map<String, Object>> getRegistRegion(@RequestBody String info) {
		Map<String, Object> resultMap = new HashMap<String, Object>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestRegionUtil(jsonObject);
			resultMap = getUserDataService.getRegistRegion(requestMap);
			logger.info("getRegistRegion：" + resultMap.toString());
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("getRegistRegion：" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, resultMap);
	}
	
	@ResponseBody
	@RequestMapping(value = "getPayFrom", method = RequestMethod.POST)
	public ResponseUtil<Map<String, Object>> getPayFrom(@RequestBody String info) {
		Map<String, Object> resultMap = new HashMap<String, Object>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			resultMap = getUserDataService.getPayFrom(requestMap);
			logger.info("getPayFrom：" + resultMap.toString());
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("getPayFrom：" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, resultMap);
	}
	
	@ResponseBody
	@RequestMapping(value = "getClientFrom", method = RequestMethod.POST)
	public ResponseUtil<Map<String, Object>> getClientFrom(@RequestBody String info) {
		Map<String, Object> resultMap = new HashMap<String, Object>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			resultMap = getUserDataService.getClientFrom(requestMap);
			logger.info("getClientFrom：" + resultMap.toString());
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("getClientFrom：" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, resultMap);
	}
	
	@ResponseBody
	@RequestMapping(value = "getConsumeCnt", method = RequestMethod.POST)
	public ResponseUtil<Map<String, Object>> getConsumeCnt(@RequestBody String info) {
		Map<String, Object> resultMap = new HashMap<String, Object>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			resultMap = getUserDataService.getConsumeCnt(requestMap);
			logger.info("getConsumeCnt：" + resultMap.toString());
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("getConsumeCnt：" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, resultMap);
	}
	
	@ResponseBody
	@RequestMapping(value = "getConsumeAmount", method = RequestMethod.POST)
	public ResponseUtil<Map<String, Object>> getConsumeAmount(@RequestBody String info) {
		Map<String, Object> resultMap = new HashMap<String, Object>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			resultMap = getUserDataService.getConsumeAmount(requestMap);
			logger.info("getConsumeAmount：" + resultMap.toString());
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("getConsumeAmount：" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, resultMap);
	}
	
	@ResponseBody
	@RequestMapping(value = "getLogonCnt", method = RequestMethod.POST)
	public ResponseUtil<Map<String, Object>> getLogonCnt(@RequestBody String info) {
		Map<String, Object> resultMap = new HashMap<String, Object>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			resultMap = getUserDataService.getLogonCnt(requestMap);
			logger.info("getLogonCnt：" + resultMap.toString());
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("getLogonCnt：" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, resultMap);
	}
	
	@ResponseBody
	@RequestMapping(value = "getGenderCnt", method = RequestMethod.POST)
	public ResponseUtil<Map<String, Object>> getGenderCnt(@RequestBody String info) {
		Map<String, Object> resultMap = new HashMap<String, Object>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUserUtil(jsonObject);
			resultMap = getUserDataService.getGenderCnt(requestMap);
			logger.info("getGenderCnt：" + resultMap.toString());
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("getGenderCnt：" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, resultMap);
	}
	
	@ResponseBody
	@RequestMapping(value = "getAgeCnt", method = RequestMethod.POST)
	public ResponseUtil<Map<String, Object>> getAgeCnt(@RequestBody String info) {
		Map<String, Object> resultMap = new HashMap<String, Object>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUserUtil(jsonObject);
			resultMap = getUserDataService.getAgeCnt(requestMap);
			logger.info("getAgeCnt：" + resultMap.toString());
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("getAgeCnt：" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, resultMap);
	}
}

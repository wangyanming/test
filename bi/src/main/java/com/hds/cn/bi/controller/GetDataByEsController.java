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

import com.hds.cn.bi.service.GetDataByEsService;
import com.hds.cn.bi.util.BaseController;
import com.hds.cn.bi.util.RequestUtil;
import com.hds.cn.bi.util.ResponseUtil;
import com.hds.cn.bi.vo.BuyRateVo;
import com.hds.cn.bi.vo.ClientVo;
import com.hds.cn.bi.vo.PayVo;
import com.hds.cn.bi.vo.RegionVo;
import com.hds.cn.bi.vo.SourceVo;
import com.hds.cn.bi.vo.TotalVo;

import net.sf.json.JSONObject;

/**
 * 
 * @author wangyanming
 *
 */
@Controller
@RequestMapping("GetDataByEsController")
public class GetDataByEsController extends BaseController{
	Logger logger = LoggerFactory.getLogger(this.getClass());
	private String mseg = "";
	@Autowired
	private GetDataByEsService getDataByEsService;
	
	@ResponseBody
	@RequestMapping(value = "getTotalbyDay", method = {RequestMethod.POST, RequestMethod.GET})
	public ResponseUtil<Map<String, Object>> getTotalbyDay(@RequestBody String info) {
		Map<String, Object> resultMap = new HashMap<String, Object>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			resultMap = getDataByEsService.getTotalbyDay(requestMap);
			logger.info("requestMap:" + info.toString() + ";" + "\r\n" + "getTotalbyDay" + ":" + resultMap);
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("requestMap:" + info + ";" + "getTotalbyDay" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, resultMap);
	}
	
	@ResponseBody
	@RequestMapping(value = "getTotalbyHour", method = RequestMethod.POST)
	public ResponseUtil<List<TotalVo>> getTotalbyHour(@RequestBody String info) {
		List<TotalVo> totalList = new ArrayList<TotalVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requesHourtUtil(jsonObject);
			totalList = getDataByEsService.getTotalbyHour(requestMap);
			logger.info("requestMap:" + info.toString() + ";" + "\r\n" + "getTotalbyHour" + ":" + totalList.toString());
			logger.info("getTotalbyHour" + ":" + totalList.toString());
			mseg = "success";
		} catch (Exception e) {
			logger.error("getTotalbyHour" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, totalList);
	}
	
	@ResponseBody
	@RequestMapping(value = "getVisitorFrom", method = RequestMethod.POST)
	public ResponseUtil<List<SourceVo>> getVisitorFrom(@RequestBody String info) {
		List<SourceVo> sourceList = new ArrayList<SourceVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			sourceList = getDataByEsService.getVisitorFrom(requestMap);
			logger.info("getVisitorFrom" + ":" + sourceList.toString());
			mseg = "success";
		} catch (Exception e) {
			logger.error("getVisitorFrom" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, sourceList);
	}
	
	@ResponseBody
	@RequestMapping(value = "getClientFrom", method = RequestMethod.POST)
	public ResponseUtil<List<ClientVo>> getClientFrom(@RequestBody String info) {
		List<ClientVo> clientList = new ArrayList<ClientVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			clientList = getDataByEsService.getClientFrom(requestMap);
			logger.info("getClientFrom" + ":" + clientList.toString());
			mseg = "success";
		} catch (Exception e) {
			logger.error("getClientFrom" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, clientList);
	}
	
	@ResponseBody
	@RequestMapping(value = "getPayFrom", method = RequestMethod.POST)
	public ResponseUtil<List<PayVo>> getPayFrom(@RequestBody String info) {
		List<PayVo> payList = new ArrayList<PayVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			payList = getDataByEsService.getPayFrom(requestMap);
			logger.info("getPayFrom" + ":" + payList.toString());
			mseg = "success";
		} catch (Exception e) {
			logger.error("getPayFrom" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, payList);
	}
	
	@ResponseBody
	@RequestMapping(value = "getRegionDistribution", method = RequestMethod.POST)
	public ResponseUtil<List<RegionVo>> getRegionDistribution(@RequestBody String info) {
		List<RegionVo> regionList = new ArrayList<RegionVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestRegionUtil(jsonObject);
			regionList = getDataByEsService.getRegionDistribution(requestMap);
			logger.info("getRegionDistribution" + ":" + regionList.toString());
			mseg = "success";
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("getRegionDistribution" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, regionList);
	}
	
	@ResponseBody
	@RequestMapping(value = "getBuyRate", method = RequestMethod.POST)
	public ResponseUtil<List<BuyRateVo>> getBuyRate(@RequestBody String info) {
		List<BuyRateVo> buyList = new ArrayList<BuyRateVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requesBuytUtil(jsonObject);
			buyList = getDataByEsService.getBuyRate(requestMap);
			logger.info("getBuyRate" + ":" + buyList.toString());
			mseg = "success";
		} catch (Exception e) {
			logger.error("getBuyRate" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, buyList);
	}
}

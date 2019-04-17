package com.hds.hdyapp.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.hds.hdyapp.service.HdyAppService;
import com.hds.hdyapp.util.BaseController;
import com.hds.hdyapp.util.CommonConstant;
import com.hds.hdyapp.util.RequestUtil;
import com.hds.hdyapp.util.ResponseUtil;
import com.hds.hdyapp.vo.BestSellingVo;
import com.hds.hdyapp.vo.ClientCountVo;
import com.hds.hdyapp.vo.DataSourceVo;
import com.hds.hdyapp.vo.ProductDataVo;
import com.hds.hdyapp.vo.RegionDistributionVo;
import com.hds.hdyapp.vo.TotalDataVo;
import com.hds.hdyapp.vo.UserDistributionVo;
import com.hds.hdyapp.vo.UserPayWayVo;

import net.sf.json.JSONObject;

/**
 * 
 * <p>Description: </p>
 * @author wangyanming
 * @date 2018年3月5日
 */
@Controller
@RequestMapping("hdyapp")
public class HdyAppController extends BaseController{
	Logger logger = LoggerFactory.getLogger(this.getClass());
	private String mseg = "";
	@Autowired
	private HdyAppService hdyAppService;
	
	@ResponseBody
	@RequestMapping(value = "getTotalData", method = RequestMethod.POST)
	public ResponseUtil<Map<String, Object>> getTotalData(@RequestBody String info) {
		Map<String, Object> resultMap = new HashMap<String, Object>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			resultMap = hdyAppService.getTotalData(requestMap);
			logger.info("requestMap:" + info + ";" + "getTotalData" + ":" + resultMap.toString());
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
	@RequestMapping(value = "getTotalDataByDate", method = RequestMethod.POST)
	public ResponseUtil<List<TotalDataVo>> getTotalDataByDate(@RequestBody String info) {
		List<TotalDataVo> totalList = new ArrayList<TotalDataVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			switch (requestMap.get("searchType").toString()) {
			case CommonConstant.DAY:
				totalList = hdyAppService.getTotalDataByDay(requestMap);
				break;
			case CommonConstant.WEEK:
				totalList = hdyAppService.getTotalDataByWeek(requestMap);
				break;
			case CommonConstant.MONTH:
				totalList = hdyAppService.getTotalDataByMonth(requestMap);
				break;
			case CommonConstant.YEAR:
				totalList = hdyAppService.getTotalDataByYear(requestMap);
				break;
			}
			logger.info("requestMap:" + info + ";" + "getTotalDataByDate" + ":" + totalList.toString());
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("requestMap:" + info + ";" + "getTotalDataByDate" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, totalList);
	}
	
	@ResponseBody
	@RequestMapping(value = "getDataSourceByDate", method = RequestMethod.POST)
	public ResponseUtil<List<DataSourceVo>> getDataSourceByDate(@RequestBody String info) {
		List<DataSourceVo> sourceList = new ArrayList<DataSourceVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			sourceList = hdyAppService.getDataSourceByDate(requestMap);
			logger.info("requestMap:" + info + ";" + "getDataSourceByDate" + ":" + sourceList.toString());
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("requestMap:" + info + ";" + "getDataSourceByDate" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, sourceList);
	}
	
	@ResponseBody
	@RequestMapping(value = "getSourceDataByChannelId", method = RequestMethod.POST)
	public ResponseUtil<List<ProductDataVo>> getSourceDataByChannelId(@RequestBody String info) {
		List<ProductDataVo> productList = new ArrayList<ProductDataVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			productList = hdyAppService.getSourceDataByChannelId(requestMap);
			logger.info("requestMap:" + info + ";" + "getSourceDataByChannelId" + ":" + productList.toString());
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("requestMap:" + info + ";" + "getSourceDataByChannelId" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, productList);
	}
	
	@ResponseBody
	@RequestMapping(value = "getRegionDistributionByDate", method = RequestMethod.POST)
	public ResponseUtil<List<RegionDistributionVo>> getRegionDistributionByDate(@RequestBody String info, HttpServletRequest request) {
		List<RegionDistributionVo> regionList = new ArrayList<RegionDistributionVo>();
		//System.out.println(request.getRemoteAddr());
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			regionList = hdyAppService.getRegionDistributionByDate(requestMap);
			logger.info("requestMap:" + info + ";" + "getRegionDistributionByDate" + ":" + regionList.toString());
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("requestMap:" + info + ";" + "getRegionDistributionByDate" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, regionList);
	}
	
	@ResponseBody
	@RequestMapping(value = "getRegionDateByRegionId", method = RequestMethod.POST)
	public ResponseUtil<List<ProductDataVo>> getRegionDateByRegionId(@RequestBody String info) {
		Map<String, Object> resultMap = new HashMap<String, Object>();
		List<ProductDataVo> productList = new ArrayList<ProductDataVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			productList = hdyAppService.getRegionDateByRegionId(requestMap);
			logger.info("requestMap:" + info + ";" + "getRegionDistributionByDate" + ":" + resultMap.toString());
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("requestMap:" + info + ";" + "getRegionDistributionByDate" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, productList);
	}
	
	@ResponseBody
	@RequestMapping(value = "getClientDataByDate", method = RequestMethod.POST)
	public ResponseUtil<List<ClientCountVo>> getClientDataByDate(@RequestBody String info) {
		List<ClientCountVo> clientList = new ArrayList<ClientCountVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			clientList = hdyAppService.getClientDataByDate(requestMap);
			logger.info("requestMap:" + info + ";" + "getClientDataByDate" + ":" + clientList.toString());
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("requestMap:" + info + ";" + "getClientDataByDate" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, clientList);
	}
	
	@ResponseBody
	@RequestMapping(value = "getClientDataByClientId", method = RequestMethod.POST)
	public ResponseUtil<List<ProductDataVo>> getClientDataByClientId(@RequestBody String info) {
		List<ProductDataVo> productList = new ArrayList<ProductDataVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			productList = hdyAppService.getClientDataByClientId(requestMap);
			logger.info("requestMap:" + info + ";" + "getClientDataByClientId" + ":" + productList.toString());
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("requestMap:" + info + ";" + "getClientDataByClientId" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, productList);
	}
	
	@ResponseBody
	@RequestMapping(value = "getUserDataByDate", method = RequestMethod.POST)
	public ResponseUtil<Map<String, Object>> getUserDataByDate(@RequestBody String info) {
		Map<String, Object> resultMap = new HashMap<String, Object>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			resultMap = hdyAppService.getUserDataByDate(requestMap);
			logger.info("requestMap:" + info + ";" + "getUserDataByDate" + ":" + resultMap.toString());
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("requestMap:" + info + ";" + "getUserDataByDate" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, resultMap);
	}
	
	@ResponseBody
	@RequestMapping(value = "getUserDistributionByDate", method = RequestMethod.POST)
	public ResponseUtil<List<UserDistributionVo>> getUserDistributionByDate(@RequestBody String info) {
		List<UserDistributionVo> userList = new ArrayList<UserDistributionVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			userList = hdyAppService.getUserDistributionByDate(requestMap);
			logger.info("requestMap:" + info + ";" + "getUserDistributionByDate" + ":" + userList.toString());
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("requestMap:" + info + ";" + "getUserDistributionByDate" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, userList);
	}
	
	@ResponseBody
	@RequestMapping(value = "getUserPayWayByDate", method = RequestMethod.POST)
	public ResponseUtil<List<UserPayWayVo>> getUserPayWayByDate(@RequestBody String info) {
		List<UserPayWayVo> userList = new ArrayList<UserPayWayVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			userList = hdyAppService.getUserPayWayByDate(requestMap);
			logger.info("requestMap:" + info + ";" + "getUserPayWayByDate" + ":" + userList.toString());
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("requestMap:" + info + ";" + "getUserPayWayByDate" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, userList);
	}
	
	@ResponseBody
	@RequestMapping(value = "getBestSellingByDate", method = RequestMethod.POST)
	public ResponseUtil<List<BestSellingVo>> getBestSellingByDate(@RequestBody String info) {
		List<BestSellingVo> sellingList = new ArrayList<BestSellingVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			sellingList = hdyAppService.getBestSellingByDate(requestMap);
			logger.info("requestMap:" + info + ";" + "getUserPayWayByDate" + ":" + sellingList.toString());
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("requestMap:" + info + ";" + "getBestSellingByDate" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, sellingList);
	}
	
	@ResponseBody
	@RequestMapping(value = "getProductData", method = RequestMethod.POST)
	public ResponseUtil<Map<String, Object>> getProductData(@RequestBody String info) {
		Map<String, Object> resultMap = new HashMap<String, Object>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			resultMap = hdyAppService.getProductData(requestMap);
			logger.info("requestMap:" + info + ";" + "getProductData" + ":" + resultMap.toString());
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("requestMap:" + info + ";" + "getProductData" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, resultMap);
	}
	
	@ResponseBody
	@RequestMapping(value = "getDataSourceByProductId", method = RequestMethod.POST)
	public ResponseUtil<List<DataSourceVo>> getDataSourceByProductId(@RequestBody String info) {
		List<DataSourceVo> sourceList = new ArrayList<DataSourceVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			sourceList = hdyAppService.getDataSourceByProductId(requestMap);
			logger.info("requestMap:" + info + ";" + "getDataSourceByProductId" + ":" + sourceList.toString());
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("requestMap:" + info + ";" + "getDataSourceByProductId" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, sourceList);
	}
	
	@ResponseBody
	@RequestMapping(value = "getRegionDistributionByProductId", method = RequestMethod.POST)
	public ResponseUtil<List<RegionDistributionVo>> getRegionDistributionByProductId(@RequestBody String info) {
		List<RegionDistributionVo> regionList = new ArrayList<RegionDistributionVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			regionList = hdyAppService.getRegionDistributionByProductId(requestMap);
			logger.info("requestMap:" + info + ";" + "getRegionDistributionByProductId" + ":" + regionList.toString());
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("requestMap:" + info + ";" + "getRegionDistributionByProductId" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, regionList);
	}
	
	@ResponseBody
	@RequestMapping(value = "getClientDataByProductId", method = RequestMethod.POST)
	public ResponseUtil<List<ClientCountVo>> getClientDataByProductId(@RequestBody String info) {
		List<ClientCountVo> clientList = new ArrayList<ClientCountVo>();
		try {
			JSONObject jsonObject = JSONObject.fromObject(info);
			Map<String, Object> requestMap = RequestUtil.requestUtil(jsonObject);
			clientList = hdyAppService.getClientDataByProductId(requestMap);
			logger.info("requestMap:" + info + ";" + "getClientDataByProductId" + ":" + clientList.toString());
			mseg = "success";
			requestMap = null;
		} catch (Exception e) {
			logger.error("requestMap:" + info + ";" + "getClientDataByProductId" + ":" + e.toString());
			mseg = e.toString();
			return error(mseg);
		}
		return success(mseg, clientList);
	}
}

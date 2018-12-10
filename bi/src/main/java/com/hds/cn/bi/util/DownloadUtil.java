package com.hds.cn.bi.util;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import com.hds.cn.bi.vo.TotalVo;

public class DownloadUtil {
	private static final String path = "d:text222.xls";
	public static HSSFWorkbook downloadExcel(List<?> list, String[] title) {
		HSSFWorkbook hwb = new HSSFWorkbook();
		OutputStream outStream = null;
		try {
			HSSFSheet sheet = hwb.createSheet("概览数据");
			sheet = hwb.getSheetAt(0);
			createTitle(hwb, sheet, title);
			HSSFCellStyle hcs = hwb.createCellStyle();
			hcs.setLocked(false);
			HSSFCell cell = null;
			if (list != null && list.size() > 0) {
				for (int i = 0; i < list.size(); i++) {
					HSSFRow rows = null;
					rows = sheet.createRow(i + 1);
					String[] str = list.get(i).toString().split(",");
					for (int j = 0; j < str.length; j++) {
						cell = rows.createCell(j);
						cell.setCellValue(str[j].toString());
					}
				}
			}
			outStream = new FileOutputStream(path);
			hwb.write(outStream);
			outStream.flush();
			outStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return hwb;
	}
	
	public static void createTitle(HSSFWorkbook workbook, HSSFSheet sheet, String[] title) {
		HSSFRow row = sheet.createRow(0);
		for (int i = 0; i < title.length; i++) {
			HSSFCell cell = row.createCell(i);
			cell.setCellValue(title[i]);
		}
	}

	public static void main(String[] args) throws SecurityException, Exception {
		List<TotalVo> list = new ArrayList<TotalVo>();
		String[] title = {"访客（UV）", "访问量（PV）", "销售额（元）", "成单数", "成功用户数", "购买转化率"};
		TotalVo totalVo = new TotalVo();
		totalVo.setPv(10);
		totalVo.setUv(10);
		totalVo.setOrderAmount("100.00");
		totalVo.setOrderCnt(5);
		totalVo.setUserCnt(10);
		totalVo.setRegistCnt(10);
		totalVo.setConversionRate("1.00");
		list.add(totalVo);
		HSSFWorkbook workbook = downloadExcel(list, title);
		OutputStream outStream = new FileOutputStream(path);
		workbook.write(outStream);
		outStream.flush();
		outStream.close();
	}
}

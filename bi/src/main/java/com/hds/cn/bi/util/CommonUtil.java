package com.hds.cn.bi.util;

import java.text.DecimalFormat;

public class CommonUtil {
	/**
	 * 保留两位小数
	 * @param num
	 * @return
	 */
	public static <T> String getDoubleNum(T num) {
		DecimalFormat df = new DecimalFormat("0.00");
		return df.format((Double.valueOf((String) num)));
	}
}

package com.hds.cn.bi;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.hds.cn.bi.vo.PayHabitVo;

public class Demo2 {
	public static void main(String[] args) {
		int size = 1;
		Set<PayHabitVo> payList = new HashSet<PayHabitVo>();
		
		for (int i = 0; i < 1; i++) {
			PayHabitVo pay = new PayHabitVo();
			pay.setPayWay("alipay");
			pay.setPayWayId(i + "alipay");
			payList.add(pay);
		}
		
		for (int i = 0; i < 2; i++) {
			PayHabitVo pay = new PayHabitVo();
			pay.setPayWay("weixin");
			pay.setPayWayId(i + "weixin");
			payList.add(pay);
		}
		
		for (int i = 0; i < 3; i++) {
			PayHabitVo pay = new PayHabitVo();
			pay.setPayWay("yinlian");
			pay.setPayWayId(i + "yinlian");
			payList.add(pay);
		}
		
		for (int i = 0; i < 4; i++) {
			PayHabitVo pay = new PayHabitVo();
			pay.setPayWay("meituan");
			pay.setPayWayId(i + "meituan");
			payList.add(pay);
		}
		
		for (int i = 0; i < 5; i++) {
			PayHabitVo pay = new PayHabitVo();
			pay.setPayWay("jd");
			pay.setPayWayId(i + "jd");
			payList.add(pay);
		}
		
		for (int i = 0; i < 6; i++) {
			PayHabitVo pay = new PayHabitVo();
			pay.setPayWay("bd");
			pay.setPayWayId(i + "bd");
			payList.add(pay);
		}
		//必须为linkedhashmap
		Map<String, Long> sortedMap = new LinkedHashMap<String, Long>();
		Map<String, Long> resultMap = new LinkedHashMap<String, Long>();
		
		//分组,计数
		Map<String, Long> payMap = 
				payList.stream().collect(
						Collectors.groupingBy(
								PayHabitVo::getPayWay, Collectors.counting()
								)
						);
		
		//排序
		if (size == -1) {
			payMap.entrySet().stream()
			.sorted(Map.Entry.<String, Long>comparingByValue()
				.reversed()).limit(5).forEachOrdered(e -> sortedMap.put(e.getKey(), e.getValue()));
		} else {
			payMap.entrySet().stream()
			.sorted(Map.Entry.<String, Long>comparingByValue()
				.reversed()).forEachOrdered(e -> sortedMap.put(e.getKey(), e.getValue()));
		}
		
		sortedMap.forEach((k, v) -> resultMap.put(k, v));
		
		System.out.println(resultMap);
	}
}

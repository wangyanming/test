package com.hds.cn.hds_app;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Unit test for simple App.
 */
public class AppTest {
	public static void main(String[] args) {
		String str = "2018-03-08";
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd"); //设置时间格式
		SimpleDateFormat df=new SimpleDateFormat("yyyy/MM/dd");
        Calendar cal = Calendar.getInstance(); 
        String endDate = str;
        try {
			cal.setTime(sdf.parse(str));
		} catch (ParseException e) {
			e.printStackTrace();
		}
        //判断要计算的日期是否是周日，如果是则减一天计算周六的，否则会出问题，计算到下一周去了  
        int dayWeek = cal.get(Calendar.DAY_OF_WEEK);//获得当前日期是一个星期的第几天  
        if (1 == dayWeek) {  
          cal.add(Calendar.DAY_OF_MONTH, -1);  
        }
        cal.setFirstDayOfWeek(Calendar.MONDAY);//设置一个星期的第一天，按中国的习惯一个星期的第一天是星期一  
        cal.setMinimalDaysInFirstWeek(7);
        int day = cal.get(Calendar.DAY_OF_WEEK);//获得当前日期是一个星期的第几天  
        System.out.println(cal.getFirstDayOfWeek());
        cal.add(Calendar.DATE, cal.getFirstDayOfWeek()-day);//根据日历的规则，给当前日期减去星期几与一个星期第一天的差值   
        int week = cal.get(Calendar.WEEK_OF_YEAR);
        String startDate = df.format(cal.getTime());
        if (1 != dayWeek) {
        	cal.add(Calendar.DATE, 6);
        	endDate = df.format(cal.getTime());
        }
        System.out.println(startDate + " ~ " + endDate + " 第" + week + "周");
	}
}

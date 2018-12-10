package com.hds.cn.bi.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 时间转换工具类
 * @author wym
 *
 */
public class DateUtil {
	
	public static void main(String[] args) throws Exception {
		Map<String, String> map = new HashMap<String, String>();
		map.put("1", "1");
		map.put("2", "2");
		map.put("3", "3");
		System.out.println(map.toString());
		map.remove("1");
		System.out.println(map.toString());
	}
	
	public static String toDate(String str) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date date = new Date();
		try {
			date = sdf.parse(str);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return  sdf.format(date);
	}
	
	public static String toTime(String str) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = new Date();
		try {
			date = sdf.parse(str);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return  sdf.format(date);
	}
	
	/**
	 * 时间戳转时间
	 * @param timeStamp
	 * @return
	 */
	public static String dateStampToDate(long timeStamp) {
		String str = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		if(String.valueOf(timeStamp).length() == 13) {
			str = sdf.format(timeStamp).toString();
		} else {
			str = sdf.format(timeStamp * 1000L).toString();
		}
		return str + ":00:00";
	}
	
	/**
	 * 時間戳轉小時
	 * @param timeStamp
	 * @return
	 */
	public static String dateStampToHour(long timeStamp) {
		String str = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH");
		if(String.valueOf(timeStamp).length() == 13) {
			str = sdf.format(timeStamp).toString();
		} else {
			str = sdf.format(timeStamp * 1000L).toString();
		}
		return str + ":00:00";
	}
	
	public static String dateStampToDay(long timeStamp) {
		String str = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		if(String.valueOf(timeStamp).length() == 13) {
			str = sdf.format(timeStamp).toString();
		} else {
			str = sdf.format(timeStamp * 1000L).toString();
		}
		return str;
	}
	
	public static Long dateToDateStamp(String date) {
		long timeStamp = 0;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
		try {
			Date time = sdf.parse(date);
			timeStamp = time.getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return timeStamp;
	}
	
	public static Long dateToDateStampS(String date) {
		long timeStamp = 0;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			Date time = sdf.parse(date);
			timeStamp = time.getTime()/1000;
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return timeStamp;
	}
	
	public static String dateAdd(String str) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");   
		Date date = new Date();
		try {
			date = df.parse(str);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	    return df.format(new Date(date.getTime() + (long)1 * 24 * 60 * 60 * 1000));
	}
	
	public static String dateDiff(String str) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");   
		Date date = new Date();
		try {
			date = df.parse(str);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	    return df.format(new Date(date.getTime() - (long)1 * 24 * 60 * 60 * 1000));
	}
	
	public static int getAge(String birth) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Date birthDay = new Date();
		try {
			birthDay = sdf.parse(birth);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		Calendar cal = Calendar.getInstance();  
		  
        if (cal.before(birthDay)) {  
            throw new IllegalArgumentException(  
                    "The birthDay is before Now.It's unbelievable!");  
        }  
        int yearNow = cal.get(Calendar.YEAR);  
        int monthNow = cal.get(Calendar.MONTH);  
        int dayOfMonthNow = cal.get(Calendar.DAY_OF_MONTH);  
        cal.setTime(birthDay);  
  
        int yearBirth = cal.get(Calendar.YEAR);  
        int monthBirth = cal.get(Calendar.MONTH);  
        int dayOfMonthBirth = cal.get(Calendar.DAY_OF_MONTH);  
  
        int age = yearNow - yearBirth;  
  
        if (monthNow <= monthBirth) {  
            if (monthNow == monthBirth) {  
                if (dayOfMonthNow < dayOfMonthBirth) age--;  
            }else {  
                age--;  
            }  
        }  
        return age;
	}
	
	/**
	 * 查询区间内所有日期
	 * @param beginDate
	 * @param endDate
	 * @return
	 */
	public static List<Date> getDatesBetweenTwoDate(Date beginDate, Date endDate) {
		List<Date> lDate = new ArrayList<Date>();
		lDate.add(beginDate);// 把开始时间加入集合
		Calendar cal = Calendar.getInstance();
		// 使用给定的 Date 设置此 Calendar 的时间
		cal.setTime(beginDate);
		boolean bContinue = true;
		while (bContinue) {
			// 根据日历的规则，为给定的日历字段添加或减去指定的时间量
			cal.add(Calendar.DAY_OF_MONTH, 1);
			// 测试此日期是否在指定日期之后
			if (endDate.after(cal.getTime())) {
				lDate.add(cal.getTime());
			} else {
				break;
			}
		}
		lDate.add(endDate);// 把结束时间加入集合
		return lDate;
	}
}

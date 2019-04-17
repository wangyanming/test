package com.hds.hdyapp.util;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 时间转换工具类
 * 
 * @author wym
 *
 */
public class DateUtil {

	public static void main(String[] args) throws Exception {
		Calendar calendar = Calendar.getInstance();
		String endDate;
		endDate = calendar.get(Calendar.YEAR) + "/" + (calendar.get(Calendar.MONTH) + 1) + "/" + calendar.get(Calendar.DATE);
		System.out.println(endDate);
	}

	public static String toDate(String str) {
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd");
		Date date = new Date();
		int week = 0;
		String weekDay = "";
		try {
			cal.setTime(sdf.parse(str));
			date = sdf.parse(str);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		week = cal.get(Calendar.DAY_OF_WEEK);
		switch (week) {
		case 1:
			weekDay = "周日";
			break;
		case 2:
			weekDay = "周一";
			break;
		case 3:
			weekDay = "周二";
			break;
		case 4:
			weekDay = "周三";
			break;
		case 5:
			weekDay = "周四";
			break;
		case 6:
			weekDay = "周五";
			break;
		case 7:
			weekDay = "周六";
			break;
		}
		return df.format(date) + " " + weekDay;
	}

	public static String toTime(String str) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = new Date();
		try {
			date = sdf.parse(str);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return sdf.format(date);
	}

	/**
	 * 时间戳转时间
	 * 
	 * @param timeStamp
	 * @return
	 */
	public static String dateStampToDate(long timeStamp) {
		String str = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		if (String.valueOf(timeStamp).length() == 13) {
			str = sdf.format(timeStamp).toString();
		} else {
			str = sdf.format(timeStamp * 1000L).toString();
		}
		return str + ":00:00";
	}

	/**
	 * 時間戳轉小時
	 * 
	 * @param timeStamp
	 * @return
	 */
	public static String dateStampToHour(long timeStamp) {
		String str = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH");
		if (String.valueOf(timeStamp).length() == 13) {
			str = sdf.format(timeStamp).toString();
		} else {
			str = sdf.format(timeStamp * 1000L).toString();
		}
		return str + ":00:00";
	}

	public static String dateStampToDay(long timeStamp) {
		String str = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		if (String.valueOf(timeStamp).length() == 13) {
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
			timeStamp = time.getTime() / 1000;
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
		return df.format(new Date(date.getTime() + (long) 1 * 24 * 60 * 60 * 1000));
	}

	public static String dateDiff(String str) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		Date date = new Date();
		try {
			date = df.parse(str);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return df.format(new Date(date.getTime() - (long) 1 * 24 * 60 * 60 * 1000));
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
			throw new IllegalArgumentException("The birthDay is before Now.It's unbelievable!");
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
				if (dayOfMonthNow < dayOfMonthBirth)
					age--;
			} else {
				age--;
			}
		}
		return age;
	}

	public static String numFormat(Double num) {
		DecimalFormat df = new DecimalFormat("0.0000");
		return df.format(num);
	}

	public static String getWeek(String str) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd"); // 设置时间格式
		SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd");
		Calendar cal = Calendar.getInstance();
		String endDate = "";
		try {
			cal.setTime(sdf.parse(str));
			endDate = df.format(sdf.parse(str));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		// 判断要计算的日期是否是周日，如果是则减一天计算周六的，否则会出问题，计算到下一周去了
		int dayWeek = cal.get(Calendar.DAY_OF_WEEK);// 获得当前日期是一个星期的第几天
		if (1 == dayWeek) {
			cal.add(Calendar.DAY_OF_MONTH, -1);
		}
		cal.setFirstDayOfWeek(Calendar.MONDAY);// 设置一个星期的第一天，按中国的习惯一个星期的第一天是星期一
		cal.setMinimalDaysInFirstWeek(7);
		int day = cal.get(Calendar.DAY_OF_WEEK);// 获得当前日期是一个星期的第几天
		cal.add(Calendar.DATE, cal.getFirstDayOfWeek() - day);// 根据日历的规则，给当前日期减去星期几与一个星期第一天的差值
		int week = cal.get(Calendar.WEEK_OF_YEAR);
		String startDate = df.format(cal.getTime());
		if (1 != dayWeek) {
			cal.add(Calendar.DATE, 6);
			endDate = df.format(cal.getTime());
		}
		return startDate + " ~ " + endDate + " 第" + week + "周";
	}

	public static String startWeek(String str) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd"); // 设置时间格式
		Calendar cal = Calendar.getInstance();
		try {
			cal.setTime(sdf.parse(str));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		// 判断要计算的日期是否是周日，如果是则减一天计算周六的，否则会出问题，计算到下一周去了
		int dayWeek = cal.get(Calendar.DAY_OF_WEEK);// 获得当前日期是一个星期的第几天
		if (1 == dayWeek) {
			cal.add(Calendar.DAY_OF_MONTH, -1);
		}
		cal.setFirstDayOfWeek(Calendar.MONDAY);// 设置一个星期的第一天，按中国的习惯一个星期的第一天是星期一
		cal.setMinimalDaysInFirstWeek(7);
		int day = cal.get(Calendar.DAY_OF_WEEK);// 获得当前日期是一个星期的第几天
		cal.add(Calendar.DATE, cal.getFirstDayOfWeek() - day);// 根据日历的规则，给当前日期减去星期几与一个星期第一天的差值
		return sdf.format(cal.getTime());
	}

	public static String getMonth(String str) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
		SimpleDateFormat df = new SimpleDateFormat("yyyy" + "年" + "MM" + "月");
		Calendar cal = Calendar.getInstance();
		try {
			cal.setTime(sdf.parse(str));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return df.format(cal.getTime());
	}

	public static String startMonth(String str) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM" + "-01");
		Calendar cal = Calendar.getInstance();
		try {
			cal.setTime(sdf.parse(str));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return df.format(cal.getTime());
	}

	public static String startYear(String str) {
		Date now = new Date();
		Calendar c = Calendar.getInstance();
		c.setTime(now);
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		c.set(Calendar.DAY_OF_YEAR, 1);
		String startDate = df.format(c.getTime());
		c.setTime(c.getTime());
		c.add(Calendar.DAY_OF_YEAR, -1);
		return startDate;
	}

	public static String endYear(String str) {
		Date now = new Date();
		Calendar c = Calendar.getInstance();
		c.setTime(now);
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		c.set(Calendar.DAY_OF_YEAR, 1);
		c.setTime(c.getTime());
		c.add(Calendar.DAY_OF_YEAR, -1);
		String endDate = df.format(c.getTime());
		return endDate;
	}

	public static String getYearByStr(String str) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy");
		String year = "";
		try {
			year = sdf.format(df.parse(str));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return year;
	}
}

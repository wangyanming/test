package com.hds.cn.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class DateUtil {
	public static String toDate(String str) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date date = new Date();
		try {
			date = sdf.parse(str);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return sdf.format(date);
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

	public static String dateStampToDate(long timeStamp) {
		String str = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		if (String.valueOf(timeStamp).length() == 13) {
			str = sdf.format(Long.valueOf(timeStamp)).toString();
		} else {
			str = sdf.format(Long.valueOf(timeStamp * 1000L)).toString();
		}
		return str + ":00:00";
	}

	public static String dateStampToHour(long timeStamp) {
		String str = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH");
		if (String.valueOf(timeStamp).length() == 13) {
			str = sdf.format(Long.valueOf(timeStamp)).toString();
		} else {
			str = sdf.format(Long.valueOf(timeStamp * 1000L)).toString();
		}
		return str + ":00:00";
	}

	public static String dateStampToDay(long timeStamp) {
		String str = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		if (String.valueOf(timeStamp).length() == 13) {
			str = sdf.format(Long.valueOf(timeStamp)).toString();
		} else {
			str = sdf.format(Long.valueOf(timeStamp * 1000L)).toString();
		}
		return str;
	}

	public static Long dateToDateStamp(String date) {
		long timeStamp = 0L;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
		try {
			Date time = sdf.parse(date);
			timeStamp = time.getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return Long.valueOf(timeStamp);
	}

	public static Long dateToDateStampS(String date) {
		long timeStamp = 0L;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			Date time = sdf.parse(date);
			timeStamp = time.getTime() / 1000L;
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return Long.valueOf(timeStamp);
	}

	public static String dateAdd(String str) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		Date date = new Date();
		try {
			date = df.parse(str);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return df.format(new Date(date.getTime() + 86400000L));
	}

	public static String dateDiff(String str) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		Date date = new Date();
		try {
			date = df.parse(str);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return df.format(new Date(date.getTime() - 86400000L));
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
		int yearNow = cal.get(1);
		int monthNow = cal.get(2);
		int dayOfMonthNow = cal.get(5);
		cal.setTime(birthDay);

		int yearBirth = cal.get(1);
		int monthBirth = cal.get(2);
		int dayOfMonthBirth = cal.get(5);

		int age = yearNow - yearBirth;
		if (monthNow <= monthBirth) {
			if (monthNow == monthBirth) {
				if (dayOfMonthNow < dayOfMonthBirth) {
					age--;
				}
			} else {
				age--;
			}
		}
		return age;
	}

	public static List<Date> getDatesBetweenTwoDate(Date beginDate, Date endDate) {
		List<Date> lDate = new ArrayList<>();
		lDate.add(beginDate);
		Calendar cal = Calendar.getInstance();

		cal.setTime(beginDate);
		boolean bContinue = true;
		while (bContinue) {
			cal.add(5, 1);
			if (!endDate.after(cal.getTime())) {
				break;
			}
			lDate.add(cal.getTime());
		}
		lDate.add(endDate);
		return lDate;
	}
}

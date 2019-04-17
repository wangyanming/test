package com.hds.hdyapp.util;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import com.hds.hdyapp.vo.TotalDataVo;

public class ListSortUtil {
	public static List<TotalDataVo> listSort(List<TotalDataVo> list) {
		Collections.sort(list, new Comparator<TotalDataVo>() {
            @Override
            public int compare(TotalDataVo o1, TotalDataVo o2) {
                SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");
                try {
                    Date dt1 = format.parse(o1.getDate().substring(0, 10));
                    Date dt2 = format.parse(o2.getDate());
                    if (dt1.getTime() < dt2.getTime()) {
                        return 1;
                    } else if (dt1.getTime() > dt2.getTime()) {
                        return -1;
                    } else {
                        return 0;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return 0;
            }
        });
		return list;
	}
	
	public static List<TotalDataVo> listMonthSort(List<TotalDataVo> list) {
		Collections.sort(list, new Comparator<TotalDataVo>() {
            @Override
            public int compare(TotalDataVo o1, TotalDataVo o2) {
                SimpleDateFormat format = new SimpleDateFormat("yyyy/MM");
                try {
                    Date dt1 = format.parse(o1.getDate().replaceAll("年", "/").replaceAll("月", ""));
                    Date dt2 = format.parse(o2.getDate().replaceAll("年", "/").replaceAll("月", ""));
                    if (dt1.getTime() < dt2.getTime()) {
                        return 1;
                    } else if (dt1.getTime() > dt2.getTime()) {
                        return -1;
                    } else {
                        return 0;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return 0;
            }
        });
		return list;
	}
	
	public static List<TotalDataVo> listYearSort(List<TotalDataVo> list) {
		Collections.sort(list, new Comparator<TotalDataVo>() {
            @Override
            public int compare(TotalDataVo o1, TotalDataVo o2) {
                SimpleDateFormat format = new SimpleDateFormat("yyyy");
                try {
                    Date dt1 = format.parse(o1.getDate().replaceAll("年", ""));
                    Date dt2 = format.parse(o2.getDate().replaceAll("年", ""));
                    if (dt1.getTime() < dt2.getTime()) {
                        return 1;
                    } else if (dt1.getTime() > dt2.getTime()) {
                        return -1;
                    } else {
                        return 0;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return 0;
            }
        });
		return list;
	}
}

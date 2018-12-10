package com.hds.cn.bi.vo;

public class UserRegistVo {
	//时间
	private String date;
	//
	private int UserCnt;
	//
	private int registCnt;
	
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public int getUserCnt() {
		return UserCnt;
	}
	public void setUserCnt(int userCnt) {
		UserCnt = userCnt;
	}
	public int getRegistCnt() {
		return registCnt;
	}
	public void setRegistCnt(int registCnt) {
		this.registCnt = registCnt;
	}
	
	@Override
	public String toString() {
		return date + "," + UserCnt + "," + registCnt;
	}
}

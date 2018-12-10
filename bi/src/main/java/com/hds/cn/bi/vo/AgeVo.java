package com.hds.cn.bi.vo;

public class AgeVo {
	private int age;
	private int userCnt;
	
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}
	public int getUserCnt() {
		return userCnt;
	}
	public void setUserCnt(int userCnt) {
		this.userCnt = userCnt;
	}
	
	@Override
	public String toString() {
		return age + "," + userCnt;
	}
}

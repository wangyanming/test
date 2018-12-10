package com.hds.cn.bi.util;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ResponseUtil<T> implements Serializable{
	private static final long serialVersionUID = 1L;
	//成功
	public static final String SUCCESS = "000000";
	//失败
	public static final String ERROR = "100000";
	
	@JsonProperty(value = "code")
	private String code;
	
	@JsonProperty(value = "message")
	private String message;
	
	@JsonProperty(value = "data")
	private T data;
	
	protected ResponseUtil(String code, String message, T data) {
		this.code = code;
		this.message = message;
		this.data = data;
	}
	
	/**
	 * 请求成功是返回结果对象
	 * @param data
	 * @return
	 */
	public static <T> ResponseUtil<T> success(T data) {
		return new ResponseUtil<T>(SUCCESS, "", data);
	}
	
	/**
	 * data结果为空，返回msg提示信息
	 * @param msg
	 * @param data
	 * @return
	 */
	public static <T> ResponseUtil<T> success(String msg, T data) {
		return new ResponseUtil<T>(SUCCESS, msg, data);
	}
	
	/**
	 * 处理指定异常的返回结果
	 * @param e
	 * @return
	 */
	public static <T> ResponseUtil<T> error(Throwable e) {
		//处理异常业务
		if (e instanceof BusinessException) {
			String message = ((BusinessException) e).getMessage();
			return error(message);
		}
		return error(e.toString());
	}
	
	/**
	 * 错误消息及具体数据
	 * @param code
	 * @param message
	 * @param data
	 * @return
	 */
	public static <T> ResponseUtil<T> error(String code, String message, T data) {
		return new ResponseUtil<T>(code, message, data);
	}
	
	/**
	 * 处理错误消息的返回结果
	 * @param message
	 * @return
	 */
	public static <T> ResponseUtil<T> error(String message) {
		return error(ERROR, message, null);
	}
	
	/**
	 * 返回消息及具体的数据
	 * 
	 * @param code
	 * @param message
	 * @return ApiResponse<T>
	 */
	public static <T> ResponseUtil<T> response(String code, String message) {
		return response(code, message, null);
	}

	/**
	 * 返回消息及具体的数据
	 * 
	 * @param code
	 * @param message
	 * @param data
	 * @return ApiResponse<T>
	 */
	public static <T> ResponseUtil<T> response(String code, String message, T data) {
		return new ResponseUtil<T>(code, message, data);
	}
	
	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public T getData() {
		return data;
	}

	public void setData(T data) {
		this.data = data;
	}
}

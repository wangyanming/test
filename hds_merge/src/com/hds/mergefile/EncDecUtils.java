package com.hds.mergefile;

import org.jasypt.util.text.BasicTextEncryptor;

public class EncDecUtils {
	private static String pwd = "jesong";
	private BasicTextEncryptor textEncryptor;

	public EncDecUtils() {
		this.textEncryptor = new BasicTextEncryptor();
		this.textEncryptor.setPassword(pwd);
	}

	public String enc(String s) {
		return this.textEncryptor.encrypt(s);
	}

	public String dec(String s) {
		return this.textEncryptor.decrypt(s);
	}

	public static void main(String[] args) {
		BasicTextEncryptor textEncryptor = new BasicTextEncryptor();
		textEncryptor.setPassword(pwd);

		String oldPassword = textEncryptor.decrypt("xN1sll9oBc0XaZo2");
		System.out.println(oldPassword);
		System.out.println("--------------------------");
	}
}

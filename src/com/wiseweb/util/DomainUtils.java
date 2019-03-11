package com.wiseweb.util;

import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DomainUtils {
	// 一级域名提取
	final static String RE_TOP1 = "(\\w*\\.?){1}\\.(com.cn|net.cn|org.cn|gov.cn|com|net|cn|org|gov|cc|co|me|tel|mobi|asia|biz|info|name|tv|hk|公司|中国|网络)$";
	// 二级域名提取
	final static String RE_TOP2 = "(\\w*\\.?){2}\\.(com.cn|net.cn|org.cn|gov.cn|com|net|cn|org|gov|cc|co|me|tel|mobi|asia|biz|info|name|tv|hk|公司|中国|网络)$";
	
	public static String getTopDomainWithoutSubdomain(String url, String regular) {
		try {
			String host = new URL(url).getHost().toLowerCase();
			Pattern pattern = Pattern.compile(regular);
			Matcher matcher = pattern.matcher(host);
			if (matcher.find())
				return matcher.group();
			else
				return "根据规则未发现一二级域名";
		} catch (Exception e) {
			e.printStackTrace();
			return "一二级域名解析有误";
		}
	}
	/**
	 * 一级域名
	 * @param url
	 * @return
	 */
	public static String getRE_TOP1(String url) {
		return getTopDomainWithoutSubdomain(url, RE_TOP1);
	}

	/**
	 * 二级域名
	 * @param url
	 * @return
	 */
	public static String getRE_TOP2(String url) {
		return getTopDomainWithoutSubdomain(url, RE_TOP2);
	}
	
	public static void main(String[] args) {
		//String url = "htdsgfstp://www.runoob.com/scala/scala-regular-expressions.html";
		//String url = "https://www.yangguihu.tv";
		String url = "http://blog.csdn.net/tounaobun/article/details/8485832";
		String topDoamin1 = getRE_TOP1(url);
		String topDoamin2 = getRE_TOP2(url);
		System.out.println(topDoamin1);
		System.out.println(topDoamin2);
	}
	
}

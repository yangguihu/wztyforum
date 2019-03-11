package com.wiseweb.util;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mysql.jdbc.StringUtils;

public class DateUtil {
	static HashMap<String, String> dateRegFormat = new HashMap<String, String>();
	static {
		dateRegFormat.put(
                "^\\d{4}\\D+\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}\\D*$",
                "yyyy-MM-dd-HH-mm-ss");//2014年3月12日 13时5分34秒，2014-03-12 12:05:34，2014/3/12 12:5:34
        dateRegFormat.put("^\\d{4}\\D+\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}$",
                "yyyy-MM-dd-HH-mm");//2014-03-12 12:05
        dateRegFormat.put("^\\d{4}\\D+\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}$",
                "yyyy-MM-dd-HH");//2014-03-12 12
        dateRegFormat.put("^\\d{4}\\D+\\d{1,2}\\D+\\d{1,2}$", "yyyy-MM-dd");//2014-03-12
        dateRegFormat.put("^\\d{4}\\D+\\d{1,2}$", "yyyy-MM");//2014-03
        dateRegFormat.put("^\\d{4}$", "yyyy");//2014
        dateRegFormat.put("^\\d{14}$", "yyyyMMddHHmmss");//20140312120534
        dateRegFormat.put("^\\d{12}$", "yyyyMMddHHmm");//201403121205
        dateRegFormat.put("^\\d{10}$", "yyyyMMddHH");//2014031212
        dateRegFormat.put("^\\d{8}$", "yyyyMMdd");//20140312
        dateRegFormat.put("^\\d{6}$", "yyyyMM");//201403
        dateRegFormat.put("^\\d{1,2}\\s*:\\s*\\d{1,2}\\s*:\\s*\\d{1,2}$",
                "yyyy-MM-dd-HH-mm-ss");//13:05:34  拼接当前日期
        dateRegFormat.put("^\\d{1,2}\\s*:\\s*\\d{1,2}$", "yyyy-MM-dd-HH-mm");//13:05  拼接当前日期
        dateRegFormat.put("^\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}$", "yy-MM-dd");//14.10.18(年.月.日)
        dateRegFormat.put("^\\d{1,2}\\D+\\d{1,2}$", "yyyy-dd-MM");//30.12(日.月) 拼接当前年份
        dateRegFormat.put("^\\d{1,2}\\D+\\d{1,2}\\D+\\d{4}$", "dd-MM-yyyy");//12.21.2013(日.月.年)
	}

	static HashMap<String, Pattern> patternMap = new HashMap<String, Pattern>();
	static {
		for (String key : dateRegFormat.keySet()) {
			patternMap.put(key, Pattern.compile(key));
		}
	}

	static final SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	@SuppressWarnings("finally")
	public static String FormatDate(String dateStr) {
		String curDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
		if("0000-00-00 00:00:00".equals(dateStr)){
			return formatter1.format(new Date());
		}

		SimpleDateFormat formatter2;
		String dateReplace;
		String strSuccess = "";
		try {
			if(StringUtils.isNullOrEmpty(dateStr)){
				strSuccess = formatter1.format(new Date());
			}else{
				for (String key : dateRegFormat.keySet()) {
					if(Pattern.compile(key).matcher(dateStr).matches()) {
	                    formatter2 = new SimpleDateFormat(dateRegFormat.get(key));
	                    if(key.equals("^\\d{1,2}\\s*:\\s*\\d{1,2}\\s*:\\s*\\d{1,2}$")
	                            || key.equals("^\\d{1,2}\\s*:\\s*\\d{1,2}$")) {//13:05:34 或 13:05 拼接当前日期
	                        dateStr = curDate + "-"+ dateStr;
	                    } else if(key.equals("^\\d{1,2}\\D+\\d{1,2}$")) {//21.1 (日.月) 拼接当前年份
	                        dateStr = curDate.substring(0, 4) + "-"+ dateStr;
	                    }
	                    dateReplace = dateStr.replaceAll("\\D+", "-");
	                    // System.out.println(dateRegExpArr[i]);
	                    strSuccess = formatter1.format(formatter2.parse(dateReplace));
	                    break;
	                }
				}
				if("".equals(strSuccess)){ //不是时间格式 如：sdfsf
					strSuccess = formatter1.format(new Date());
				}
			}
		} catch (Exception e) {
			System.err.println("-----------------日期格式无效:" + dateStr);
			// throw new Exception( "日期格式无效");
			strSuccess = formatter1.format(new Date());
		} finally {
			return strSuccess;
		}
	}

	/**
	 * 获取当前时间
	 * 
	 * @return
	 */
	public static String getCurrentDate() {
		return formatter1.format(new Date());
	}
	
	/**
	 * 格式化时间戳
	 * @return
	 */
	public static String formatTst(Date tst) {
		if(tst==null){
			tst=new Date();
		}
		return formatter1.format(tst);
	}
	/**
	 * 格式化时间戳并校验
	 * @return
	 */
	public static String formatCheckTst(Date tst) {
		if(tst==null){
			tst=new Date();
		}
		return FormatDate(formatter1.format(tst));
	}
	
	


	/**
	 * (1)能匹配的年月日类型有： 2014年4月19日 2014年4月19号 2014-4-19 2014/4/19 2014.4.19
	 * (2)能匹配的时分秒类型有： 15:28:21 15:28 5:28 pm 15点28分21秒 15点28分 15点
	 * (3)能匹配的年月日时分秒类型有： (1)和(2)的任意组合，二者中间可有任意多个空格
	 * 如果dateStr中有多个时间串存在，只会匹配第一个串，其他的串忽略
	 * 
	 * @param text
	 * @return
	 */
	@SuppressWarnings("unused")
	private static String matchDateString(String dateStr) {
		try {
			List matches = null;
			Pattern p = Pattern
					.compile(
							"(\\d{1,4}[-|\\/|年|\\.]\\d{1,2}[-|\\/|月|\\.]\\d{1,2}([日|号])?(\\s)*(\\d{1,2}([点|时])?((:)?\\d{1,2}(分)?((:)?\\d{1,2}(秒)?)?)?)?(\\s)*(PM|AM)?)",
							Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
			Matcher matcher = p.matcher(dateStr);
			if (matcher.find() && matcher.groupCount() >= 1) {
				matches = new ArrayList();
				for (int i = 1; i <= matcher.groupCount(); i++) {
					String temp = matcher.group(i);
					matches.add(temp);
				}
			} else {
				matches = Collections.EMPTY_LIST;
			}
			if (matches.size() > 0) {
				return ((String) matches.get(0)).trim();
			} else {
			}
		} catch (Exception e) {
			return "";
		}

		return dateStr;
	}

	public static void main(String[] args) {
		
//		String[] dateStrArray = new String[] { "2014-3-2 12:05:34",
//				"2014-03-12 12:05", "2014-03-12 12", "2014-03-12", "2014-03",
//				"2014", "20140312120534", "2014/03/12 12:05:34",
//				"2014/3/12 12:5:34", "2014年3月12日 13时5分34秒", "201403121205",
//				"1234567890", "20140312", "201403", "2000 13 33 13 13 13",
//				"30.12.2013", "12.21.2013", "21.1", "13:05:34", "12:05",
//				"14.1.8", "14.10.18" };
//		for (int i = 0; i < dateStrArray.length; i++) {
//			System.out.println(dateStrArray[i]
//					+ "------------------------------".substring(1,
//							30 - dateStrArray[i].length())
//					+ FormatDate(dateStrArray[i]));
//		}
		//String date="2016-1-21 17:13";
		//System.out.println(FormatDate(date));
		System.out.println(getCurrentDate());
		
	}
	// public static void main2(String[] args) {
	// String iSaid =
	// "亲爱的，2014年4月25 15时36分21秒， 我会在世贸天阶向你求婚！等到2015年6月25日，我们就结婚。";
	//
	// // 匹配时间串
	// String answer = matchDateString(iSaid);
	//
	// // 输出：
	// // 问：请问我们什么时候结婚？
	// // 答：2014年4月25 15时36分21秒
	// System.out.println("问：请问我们什么时候结婚？");
	// System.out.println("答：" + answer);
	// }

}

package com.wiseweb.kafka;

/**
 *kafkaConfConstant kafka配置常量
 *KCConstant
 * @author yangguihu
 *
 */
public class KCConstant {

	//ali云  broker 外网测试 （本地开发使用）
//	public static final String BK1="182.92.68.59:9092";
//	public static final String BK2="182.92.242.215:9092";
//	public static final String BK3="123.56.102.134:9092";
//	public static final String BK6="123.56.118.189:9092";
//	public static final String BK7="123.56.118.192:9092";
	//ali云  broker 内网测试 （正式部署上线使用）
	public static final String BK1="10.165.65.14:9092";
	public static final String BK2="10.171.100.247:9092";
	public static final String BK3="10.251.5.35:9092";
	public static final String BK6="10.170.193.188:9092";
	public static final String BK7="10.170.183.32:9092";
	
	//虚拟机测试
//	public static final String BK1="192.168.33.51:9092";
//	public static final String BK2="192.168.33.52:9092";
//	public static final String BK3="192.168.33.53:9092";
	
	//ali云地址
	public static final String BROKERS=BK1+","+BK2+","+BK3+","+BK6+","+BK7;
	//本地虚拟机测试
//	public static final String BROKERS=BK1+","+BK2+","+BK3;
	
	//topic 的定义 ：  网智天元 - 爬虫数据 - 微博 的方式命名。  
	public static final String LUNTAN="wiseweb_crawler_forum";   //论坛主贴
	public static final String REPLIES="wiseweb_crawler_replies"; //论坛回复
	
	public static final String HCT_HOMEPAGE="wiseweb_hct_homepage"; //homepage
	public static final String FOREIGN="wiseweb_crawler_webpage";
//	public static final String FOREIGN="wiseweb_crawler_outside";

}

package com.wiseweb.kafka;

import java.util.UUID;

/**
 * 生产者调用api
 * @author yangguihu
 *
 */
public class ProducerCallApi {
	public static void main(String[] args) {
	       
	      String artics="{title:\"标题\",context:\"内容1\",papername:\"报纸名称\",source:\"http://app.why.com.cn/epaper/qnb/html/2013-08/05/node_1.htm\",publishdata:\"2016-08-04 15:39:01\"}";
	      //添加一个uuid作为key,改善数据倾斜
	      String key="";
	      for (int messageNo = 21; messageNo <= 40; messageNo++) {
	    	  //消息kafka分区使用key ,暂使用 uuid
	    	  key=UUID.randomUUID().toString();
	    	  
	    	  //wiseweb_crawler_newspapers
	    	  KafkaNewApiUtils.crawToKafa(KCConstant.LUNTAN,KCConstant.BROKERS , key, artics);
	      }
	      //关闭producer
	      KafkaNewApiUtils.closeProducer();
//		System.out.println(UUID.randomUUID().toString());
	  }
}

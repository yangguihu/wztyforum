package com.wiseweb.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.alibaba.fastjson.JSON;

/**
 * kafka api调用工具类
 * @author yangguihu
 *
 */
public class KafkaNewApiUtils {
	
	/**
	 * 存放产生的producer
	 */
	 static Producer<String,String> producer=null;

	 /**
	  * 
	  * @param topic		指定的主题
	  * @param brokers		kafka broker服务节点，使用常量
	  * @param message		发送json
	  * @param key			唯一key，和分区也有关系，使用uuid生成
	  * @param partition	自定义分区，没有填null（后期使用）
	  * 
	  * 调用说明：上述参数对应KCConstant常量
	  * 调用形式： KafkaProducerUtils.crawToKafa(KCConstant.TPDY,KCConstant.BROKERS , 
        		sourceId, artics.toString());
	  */
	public static void crawToKafa(String topic,String brokers,final String key,final String message){
		if(producer==null){
			 Properties props = new Properties();
			 //broker列表
			 props.put("bootstrap.servers", brokers);
			 //要求 all->leader和副本都接收,1->leader收到后确认,0->不用确认
			 props.put("acks", "all");
			 //失败后重试次数
			 props.put("retries", 1);
			 //10m 批量发送的为10m  默认为1m
			 props.put("batch.size", 10485760);
			 //一个请求的最大大小  	默认为1048576 设置成10m 防止请求过大不能发送
			 props.put("max.request.size", 10485760);
			 //指示生产者发送请求之前等待一段时间，希望更多的消息填补到同一个批次
			 props.put("linger.ms", 1);
			 //props.put("buffer.memory", 33554432); //默认值就为
			 //key的序列化程序类
			 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			 //具体发送消息的序列化程序类
			 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			 //创建一个发送类
			 producer = new KafkaProducer<>(props);
		}
		//如果要模拟一个简单的阻塞调用，你可以立刻调用get()方法。
		//producer.send(new ProducerRecord<String, String>(topic, key, message).get()
		
		//完全无阻塞的话,可以利用回调参数提供的请求完成时将调用的回调通知。
		producer.send(new ProducerRecord<String, String>(topic, key, message),
				new Callback() {
					public void onCompletion(RecordMetadata metadata,Exception e) {
						 if(e != null){
							 //可以在此执行自己的回调逻辑
							 e.printStackTrace();
							 String table = key.substring(2);
							 String id = JSON.parseObject(message).get("Id").toString();
							 System.out.println(table+" Id ==> "+id);
						 }
					}
				});
	}

	
	/**
	 * 关闭生产者
	 */
	public static void closeProducer(){
		if(producer!=null){
			producer.close();
			producer=null;
		}
	}
}

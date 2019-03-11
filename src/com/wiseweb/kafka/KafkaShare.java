package com.wiseweb.kafka;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * kafka api调用工具类
 * @author yangguihu
 *
 */
public class KafkaShare {
	private static Producer<String,String> producer=null;
	
	public static boolean getProducer(){
		 Properties props = new Properties();
		 //broker列表
		 props.put("bootstrap.servers", KCConstant.BROKERS);
		 //要求 all->leader和副本都接收,1->leader收到后确认,0->不用确认
		 props.put("acks", "1");
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
		 producer = new KafkaProducer<String,String>(props);
		 
		 return producer!=null;
	}
	
	//一开始就创建一个
	static {
		getProducer();
	}
	
	//调之前先调一下getProducer
	public static void crawToKafa(String topic,final String key,final String message,final long id,final LinkedBlockingQueue<Long> queue,final String table){
		//完全无阻塞的话,可以利用回调参数提供的请求完成时将调用的回调通知。
		if(producer==null){
			getProducer();
		}
		try {
			producer.send(new ProducerRecord<String, String>(topic, key, message),
					new Callback() {
						public void onCompletion(RecordMetadata metadata,Exception e) {
							 if(e != null){
								 //可以在此执行自己的回调逻辑
								 queue.add(id);
								 System.out.println(new Date()+" "+table +"->"+id+" 消息发送失败 "+e.getMessage());
							 }
						}
					});
		} catch (IllegalStateException e) {
			//java.lang.IllegalStateException: Memory records is not writable
			queue.add(id);
			System.out.println(new Date()+" "+table +"->"+id+" 消息发送失败 "+e.getMessage());
		}
	}
}

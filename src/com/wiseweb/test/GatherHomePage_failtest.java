package com.wiseweb.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.zookeeper.data.Stat;

import com.alibaba.fastjson.JSON;
import com.wiseweb.kafka.KCConstant;
import com.wiseweb.kafka.KafkaUtils;
import com.wiseweb.util.DateUtil;
import com.wiseweb.util.DomainUtils;
import com.wiseweb.util.HashUtil;
import com.wiseweb.util.JdbcUtil2;
import com.wiseweb.util.RecordMaping;
import com.wiseweb.util.ZkUtil;

public class GatherHomePage_failtest{

	@SuppressWarnings("resource")
	public static void main(String[] args) throws Exception {
		//十分钟连一次，重试次数无数
		CuratorFramework client = CuratorFrameworkFactory.newClient(
				"node1:2181,node2:2181,node3:2181", new ExponentialBackoffRetry(10*60*1000, Integer.MAX_VALUE));
		client.start();
		
		//获取一个生产者
		Producer<String,String> producer = KafkaUtils.getProducer(KCConstant.BROKERS);
		
		String ROOT = "/wzty/for/";
		String table="sina_com";
		String dbname="homepage_gather";
		int pagenum=20;  //每次查询的数量
		
		System.out.println("启动时间 --> " +DateUtil.getCurrentDate());
	
		//发送失败id集合
		LinkedBlockingQueue<Long> queue=new LinkedBlockingQueue<Long>();
		
		//false 没跑完， true 跑完了
		boolean flag = false;
		long start = 0L;
		
		Connection conn = JdbcUtil2.getConn(dbname);
		PreparedStatement pst = null;
		ResultSet rs =null;
		
		Map<String, Object> rowData= new HashMap<String, Object>(); //主贴内容
		
		long finishtime = 0L; //完成时间
		long pretime = 0L;    //上次时间
		long finishid = 0L;    //本轮result的最后一行id
		long preid = 0L;    	 //上轮结果及最后的id
		long failpre=0L;	  //记录上一次处理失败队列的时间
		try {
			Stat stat = client.checkExists().forPath(ROOT+table);
			if(stat==null){//创建节点
				client.create().forPath(ROOT+table, "0".getBytes()); 
			}
			pretime=ZkUtil.getDataLong(client, ROOT+table)-1;
			finishtime=ZkUtil.getDataLong(client, ROOT+table)-1;
			
		} catch (Exception e3) {
			e3.printStackTrace();
		}
		//一直不停的执行
		while(true){
			try {
				//获取上次Id
				if(finishtime==0){
					finishtime=ZkUtil.getDataLong(client, ROOT+table);
				}
				
				if(conn==null || conn.isClosed()){
					conn = JdbcUtil2.getConn(dbname);
				}
				//2000条
				String sql="SELECT * FROM sina_com WHERE inserttime > ? LIMIT ?";
				pst = conn.prepareStatement(sql);
				pst.setLong(1, finishtime);
				pst.setInt(2, pagenum);
				rs = pst.executeQuery();
				
				if(rs.next()){  //第一行
					pretime=rs.getLong("inserttime")-1;
					
					rs.last();
					finishtime = rs.getLong("inserttime");
					finishid= rs.getInt("id");
					rs.beforeFirst();//恢复
					
					if((finishtime+180) >= System.currentTimeMillis()/1000){//在3分钟之内
						flag=true;
						start=System.currentTimeMillis();
					}
				}else{ //第一次取数据就为空
					try {
						JdbcUtil2.close(conn, pst, rs);
						Thread.sleep(3*60*1000);
						continue;
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				
				while(rs.next()){
					long id = rs.getLong("id");
					if(id>preid){ //排除已经发送的
						//映射处理并发送
						handleRs(rs, producer, queue, client,ROOT+table,rowData);
					}
				}
				//重新赋值为真正发送过了的
				finishtime=finishtime-1;
				preid=finishid;//本轮最后一行的id赋给preid,留给下一次排重
				//设置采集到的id
				client.setData().forPath(ROOT+table, String.valueOf(finishtime).getBytes()); 
				
				long failstart=System.currentTimeMillis()/1000;
				//System.out.println("队列中的获取的数据==> "+queue.peek());
				if((queue.peek()!=null)&&(failstart-failpre>=10)){
					//处理发送失败的消息
					StringBuffer sb=new StringBuffer("SELECT * FROM "+table+" WHERE id in (");
		            while (queue.peek() != null) {
	                   sb.append(queue.poll()+",");
	                } 
					sb.deleteCharAt(sb.length()-1);
					sb.append(")");
					
					System.out.println(sb.toString());
					PreparedStatement failpst = conn.prepareStatement(sb.toString());
					ResultSet failrs = failpst.executeQuery();
					while(failrs.next()){
						handleRs(failrs, producer, queue, client,ROOT+table,rowData);
					}
					//关闭结果集
					failrs.close();
					failpre=System.currentTimeMillis()/1000;
				}
				
			} catch (SQLException e) {
				e.printStackTrace();
				System.out.println("报错表==> "+table);
				try {
					client.setData().forPath(ROOT+table, String.valueOf(pretime).getBytes()); 
					
				} catch (SQLException e1) {
					e1.printStackTrace();
				}catch (Exception e2) {
					e2.printStackTrace();
				}
			}catch (Exception e) {
				e.printStackTrace();
			}
			
			if(flag){ //设置间隔
				try {
					JdbcUtil2.close(conn, pst, rs);
					Thread.sleep(3*60*1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				long interval=System.currentTimeMillis()-start;
				if(interval > (30 * 60 * 1000)){
					flag=false;  //跑完之后半小时检查一下
				}
			}else{
				//数据没有跑完不关 conn
				JdbcUtil2.close(null, pst, rs);
			}
		}
	}
	
	/**
	 * 处理结果集并发送至kafka
	 * @param rs			jdbc结果集
	 * @param producer		kafka生产者
	 * @param queue			存放发送失败的消息队列
	 * @param client		zk客户端对象
	 * @param node			表对应的zk节点	
	 * @param table			表
	 * @param rowData		用来存放映射后的信息map
	 * @throws Exception
	 */
	public static void handleRs(ResultSet rs,Producer<String,String> producer,
			LinkedBlockingQueue<Long> queue,CuratorFramework client,String node,
			Map<String, Object> rowData) throws Exception{
		
		long id = rs.getLong("id");
        rowData.put("id", id);
        
		for (String key : RecordMaping.homeSet) {
			 rowData.put(key, rs.getObject(key));
		}
        long inserttime = rs.getLong("inserttime");

        rowData.put("inserttime", inserttime);
        //使用getObject读取出来是时间错
        Timestamp gathertime=rs.getTimestamp("gathertime");
        rowData.put("gathertime", DateUtil.formatTst(gathertime));

		//处理publishtime
        Timestamp publishtime=rs.getTimestamp("publishtime");
		rowData.put("publishtime",DateUtil.formatCheckTst(publishtime));
		//处理url
		String url = rs.getString("url");
		rowData.put("url", url);
		String urlhash = String.valueOf(HashUtil.xxHash(url));
		rowData.put("domain_1",DomainUtils.getRE_TOP1(url)); //一级域名
		rowData.put("domain_2",DomainUtils.getRE_TOP2(url)); //二级域名
		
		rowData.put("urlhash", urlhash);//主贴 urlhash
		
		//添加采集到kafka的时间
		rowData.put("kafkatime", DateUtil.getCurrentDate());
		String jsonStr = JSON.toJSONString(rowData); //发送主贴
        
		String keyMsg=UUID.randomUUID().toString().substring(0,8);
        //发送kafka
        crawToKafa(producer,KCConstant.HCT_HOMEPAGE, keyMsg, jsonStr, id, queue);
        //记录采集信息
        if(id % 200==0){ //200条也记一次
        	client.setData().forPath(node, String.valueOf(inserttime).getBytes()); 
        }
	}
	
	/**
	 * 发送方法，发送失败会将传过来的id放入队列中
	 * @param producer  kafka生产者
	 * @param topic     发送的topic
	 * @param key		发送消息的key
	 * @param message	发送的json消息
	 * @param id		获取到的消息的id
	 * @param queue		存放失败消息的队列
	 */
	public static void crawToKafa(Producer<String,String> producer,String topic,
			final String key,final String message,final long id,final LinkedBlockingQueue<Long> queue){
		//完全无阻塞的话,可以利用回调参数提供的请求完成时将调用的回调通知。
		producer.send(new ProducerRecord<String, String>(topic, key, message),
				new Callback() {
					public void onCompletion(RecordMetadata metadata,Exception e) {
						 if(e != null){
							 //可以在此执行自己的回调逻辑
							 queue.add(id);
							 System.out.println(id+" -->消息发送失败");
							 System.out.println(e.getMessage());
						 }
						 if(System.currentTimeMillis()%20==0){
							//可以在此执行自己的回调逻辑
							 queue.add(id);
							 System.out.println(id+" -->消息发送失败");
						 }
					}
				});
	}
}

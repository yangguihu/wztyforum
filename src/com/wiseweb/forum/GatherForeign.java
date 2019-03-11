package com.wiseweb.forum;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;

import com.alibaba.fastjson.JSON;
import com.wiseweb.kafka.KCConstant;
import com.wiseweb.kafka.KafkaShare;
import com.wiseweb.util.DateUtil;
import com.wiseweb.util.HashUtil;
import com.wiseweb.util.JdbcUtil2;
import com.wiseweb.util.RecordMaping;
import com.wiseweb.util.ZkUtil;
/**
 * 采集外媒的数据多表
 * @author yangguihu
 *
 */
public class GatherForeign implements Runnable{
	private String table;
	private String dbname;
	private String zkpath; //集群上的zk路径
	private int pagenum;
	
	CuratorFramework client;
	
	public GatherForeign() {}

	public GatherForeign(String table) {
		this.table = table;
	}
	public GatherForeign(String table,String dbname,String zkpath,int pagenum,CuratorFramework client) {
		this.table = table;
		this.dbname = dbname;
		this.zkpath=zkpath;
		this.pagenum=pagenum;
		this.client=client;
	}


	@SuppressWarnings("resource")
	public void run() {	
		String tnode="/wzty/"+zkpath+table;
		//发送失败id集合
		LinkedBlockingQueue<Long> queue=new LinkedBlockingQueue<Long>();
		
		//false 没跑完， true 跑完了
		boolean flag = false;
		long start = 0L;
		
		Connection conn = JdbcUtil2.getConn(dbname);
		PreparedStatement pst = null;
		ResultSet rs =null;
		
		Map<String, Object> rowData= new HashMap<String, Object>(); //数据内容
		
		long finishtime = 0L; //完成时间
		long pretime = 0L;    //上次时间
		long finishid = 0L;    //本轮result的最后一行id
		long preid = 0L;    	 //上轮结果及最后的id
		long failpre=0L;	  //记录上一次处理失败队列的时间
		try {
			Stat stat = client.checkExists().forPath(tnode);
			if(stat==null){//创建节点
				client.create().forPath(tnode, "0".getBytes()); 
			}
			pretime=ZkUtil.getDataLong(client, tnode)-1;
			finishtime=ZkUtil.getDataLong(client, tnode)-1;
			
		} catch (Exception e3) {
			e3.printStackTrace();
		}
		//一直不停的执行
		while(true){
			try {
				//获取上次Id
				if(finishtime==0){
					finishtime=ZkUtil.getDataLong(client, tnode);
				}
				if(conn.isClosed() || conn==null){
					conn = JdbcUtil2.getConn(dbname);
				}
				//2000条
				String sql="SELECT * FROM "+table+" WHERE inserttime > ? LIMIT ?";
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
						handleRs(rs,queue, client,tnode,table,rowData);
					}
				}
				//重新赋值为真正发送过了的
				finishtime=finishtime-1;
				preid=finishid;//本轮最后一行的id赋给preid,留给下一次排重
				//设置采集到的id
				client.setData().forPath(tnode, String.valueOf(finishtime).getBytes()); 
				
				long failstart=System.currentTimeMillis()/1000;
				if((queue.peek()!=null)&&(failstart-failpre>=1200)){
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
						handleRs(failrs,queue, client,tnode,table,rowData);
					}
					//关闭结果集
					failrs.close();
					failpre=System.currentTimeMillis()/1000;
				}
				
			} catch (SQLException e) {
				e.printStackTrace();
				System.out.println("报错表==> "+table);
				try {
					client.setData().forPath(tnode, String.valueOf(pretime).getBytes()); 
					
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
	 * @param queue			存放发送失败的消息队列
	 * @param client		zk客户端对象
	 * @param node			表对应的zk节点	
	 * @param table			表
	 * @param rowData		用来存放映射后的信息map
	 * @throws Exception
	 */
	public static void handleRs(ResultSet rs,LinkedBlockingQueue<Long> queue,
			CuratorFramework client,String node,String table,Map<String, Object> rowData) throws Exception{
		
		long id = rs.getLong("id");
        rowData.put("id", id);
        
		for (String key : RecordMaping.foreignSet) {
			 rowData.put(key, rs.getObject(key));
		}
        long inserttime = rs.getLong("inserttime");

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
		
		rowData.put("urlhash", urlhash);//主贴 urlhash
		
		//添加采集到kafka的时间,此处使用inserttime是为了兼容
		rowData.put("inserttime", DateUtil.getCurrentDate());
		String jsonStr = JSON.toJSONString(rowData); //发送主贴
        
		String keyMsg=UUID.randomUUID().toString().substring(0,8);
        //发送kafka
		KafkaShare.crawToKafa(KCConstant.FOREIGN, keyMsg, jsonStr,id,queue,table);
        //记录采集信息
        if(id % 500==0){ //500条也记一次
        	client.setData().forPath(node, String.valueOf(inserttime).getBytes()); 
        }
	}
	
	public static void main(String[] args) throws SQLException {
		List<String> tableNames=new ArrayList<String>();
		//forum1
		String comn=args[0];
		
		String dbname=comn+"_gather";
		Connection conn = JdbcUtil2.getConn(dbname);
		//获取表名
		PreparedStatement pst = conn.prepareStatement("select table_name from information_schema.tables WHERE TABLE_SCHEMA='"+dbname+"'");
		ResultSet rs = pst.executeQuery();
		while(rs.next()){
			tableNames.add(rs.getString("table_name"));
		}
		//关闭资源
		JdbcUtil2.close(conn, pst, rs);
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
		System.out.println(df.format(new Date()));
		System.out.println(dbname+" 采集表 ===> "+tableNames);
		//线程池
        ExecutorService executor = Executors.newFixedThreadPool(tableNames.size());
		//十分钟连一次，重试次数无数
		CuratorFramework client = CuratorFrameworkFactory.newClient(
				"node1:2181,node2:2181,node3:2181", new ExponentialBackoffRetry(10*60*1000, Integer.MAX_VALUE));
		//客户端开启
		client.start();
		
        int pagenum=2000;
        if(args.length>1){
        	 pagenum=Integer.valueOf(args[1]);
        }
        for (int i = 0; i <tableNames.size() ; i++) {
            executor.execute(new GatherForeign(tableNames.get(i),dbname,comn+"/",pagenum,client));
        }
	}
}

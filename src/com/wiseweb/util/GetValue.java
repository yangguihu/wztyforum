package com.wiseweb.util;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * 查询采集程序在zookeeper中的记录，判断采集是否及时和采集数量情况
 * @author yangguihu
 *
 */
public class GetValue {
	private static Map<String,String> insrtMap=new HashMap<String,String>();
	
	public static void main(String[] args) throws Exception {
		String brokers="node1:2181,node2:2181,node3:2181";
		CuratorFramework client = null;

		String[] roots={"/wzty/forum"};
//		String[] roots={"/wzty/forum1"};
		ZooKeeper zk;
		try {
			zk = new ZooKeeper(brokers, 30000, new Watcher() {
				public void process(org.apache.zookeeper.WatchedEvent event) {
				}
			});

			client= CuratorFrameworkFactory.newClient(
				brokers, new ExponentialBackoffRetry(10*1000, 3));
			client.start();
			
			for (String root : roots) {
				getValuePath(zk,client, brokers, root);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			//System.out.println(insrtMap);
			getNumByTime(insrtMap);
			CloseableUtils.closeQuietly(client);
		}
	}
	
	public static void getValuePath(ZooKeeper zk,CuratorFramework client, String brokers,String root) {
		try {
			List<String> children = ZKPaths.getSortedChildren(zk, root);

			System.out.println(root + "/");
			for (String path : children) {
				byte[] value = client.getData().forPath(root + "/" + path);
				insrtMap.put(path, new String(value));

				System.out.println(path + " --> " + new String(value));
				// System.out.println(path);
			}
			System.out.println();
			System.out.println("当前时间--->"+System.currentTimeMillis()/1000);
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
	
	/**
	 * 获取根据时间查询的语句
	 * @param insrtMap
	 * @throws SQLException
	 */
	public static void getNumByTime(Map<String,String> insrtMap) throws SQLException{
		String[] tables={"bbs_baidu_cn","bbs_baidu_cn_2","bbs_cn","bbs_kdnet_cn","bbs_ltaaa_cn","bbs_mop_cn","bbs_news_cn","bbs_people_cn","bbs_rednet_cn","bbs_sina_cn","bbs_tianya_cn","bbs_voc_cn","bbs_xici_cn"};
//		String[] tables={"chinese_com","chinese_com_2","english_com","korean_com","mfa_com","thai_com","twitter_com"};
		Map<String,String> hhMap=new HashMap<String,String>();
		for (Entry<String, String> entry : insrtMap.entrySet()) {
			StringBuffer sb=new StringBuffer();
			sb.append("SELECT COUNT(id) as count FROM ");
			sb.append(entry.getKey());
			sb.append(" WHERE inserttime <=");
			sb.append(Integer.valueOf(entry.getValue())+1);
			if(!tables[tables.length-1].equals(entry.getKey())){
				sb.append(" UNION ALL ");
			}
			hhMap.put(entry.getKey(), sb.toString());
		}
		
		StringBuffer sbt=new StringBuffer();
		for (int i = 0; i < tables.length; i++) {
			sbt.append(hhMap.get(tables[i]));
		}
		System.out.println(sbt.toString());
		
//		Connection conn = JdbcUtil.INSTANCE.getConn();
//		PreparedStatement pst = conn.prepareStatement(sql);
//		ResultSet rs = pst.executeQuery();
//		while(rs.next()){
//			rs.getString("count");
//		}
//		JdbcUtil.INSTANCE.close(conn, pst, rs);
	}
}

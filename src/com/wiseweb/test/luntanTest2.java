package com.wiseweb.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;

import com.alibaba.fastjson.JSON;
import com.wiseweb.kafka.KCConstant;
import com.wiseweb.kafka.KafkaNewApiUtils;
import com.wiseweb.util.DateUtil;
import com.wiseweb.util.DomainUtils;
import com.wiseweb.util.HashUtil;
import com.wiseweb.util.JdbcUtil2;
import com.wiseweb.util.RecordMaping;
import com.wiseweb.util.ZkUtil;

public class luntanTest2 {

	public static void main(String[] args) {
		String dbname="forum_gather";
		// 十分钟连一次，重试次数无数
//		CuratorFramework client = CuratorFrameworkFactory.newClient(
//				"node1:2181,node2:2181,node3:2181",
//				new ExponentialBackoffRetry(10 * 60 * 1000, Integer.MAX_VALUE));
//		client.start();
//		String ROOT = "/wzty/forum1/";

		// false 没跑完， true 跑完了
		boolean flag = false;
		long start = 0L;

		Connection conn = JdbcUtil2.getConn(dbname);
		PreparedStatement selpst = null;
		ResultSet rs = null;

		Map<String, Object> rowData = new HashMap<String, Object>();
		// data_content_31 --> 31
		String keyid = "31";

//		int finishId = 0; // 完成Id
//		int preId = 0; // 上次Id
//		try {
//			Stat stat = client.checkExists().forPath(ROOT + table);
//			if (stat == null) {// 创建节点
//				client.create().forPath(ROOT + table, "0".getBytes());
//			}
//			preId = ZkUtil.getData(client, ROOT + table);
//			finishId = ZkUtil.getData(client, ROOT + table);
//
//		} catch (Exception e3) {
//			e3.printStackTrace();
//		}

		// 一直不停的执行
//		NEW: while (true) {
		int count=0;
			try {
				// 获取上次Id
//				if (finishId == 0) {
//					finishId = ZkUtil.getData(client, ROOT + table);
//				}
//
//				if (conn == null || conn.isClosed()) {
//					conn = JdbcUtil2.getConn(dbname);
//				}
				conn.setAutoCommit(false);

				String sql = "SELECT * FROM data_content_31 WHERE Id > ? LIMIT ?";
				selpst = conn.prepareStatement(sql);
				selpst.setInt(1, 0);
				selpst.setInt(2, 1000);
				rs = selpst.executeQuery();

//				if (rs.next()) { // 第一行
//					preId = rs.getInt("Id") - 1;
//
//					rs.last();
//					int row = rs.getRow();
//					finishId = rs.getInt("Id");
//					rs.beforeFirst();// 恢复
//
//					if (row < pagenum) {// 少于查询数，历史数据跑完了
//						flag = true;
//						start = System.currentTimeMillis();
//					}
//				} else { // 第一次取数据就为空
//					try {
//						JdbcUtil.INSTANCE.close(conn, selpst, rs);
//						Thread.sleep(3 * 60 * 1000);
//					} catch (Exception e) {
//						e.printStackTrace();
//					}
//					break NEW;
//				}

				// 获取列数
				String jsonStr = "";
				int id;
				String key;
				Object value;
				
				while (rs.next()) {
					for (Entry<String, String> entry : RecordMaping.resMap
							.entrySet()) {
						key = entry.getKey();
						value = rs.getObject(entry.getValue());

						if ("site_name".equals(key)) {
							value = value.toString().replaceAll(">", "_");
						} else if ("url".equals(key)) {
							rowData.put("urlhash",
									String.valueOf(HashUtil.xxHash(value)));// urlhash
							rowData.put("domain_1",
									DomainUtils.getRE_TOP1(value.toString())); // 一级域名
							rowData.put("domain_2",
									DomainUtils.getRE_TOP2(value.toString())); // 二级域名
						} else if ("publishtime".equals(key)) {
							value = value == null ? "" : value;
							value = DateUtil
									.FormatDate(value.toString().trim());
						}
						rowData.put(key, value);
					}
					rowData.put("group_id", "2"); // 论坛
					rowData.put("site_id", "0"); // 论坛
					rowData.put("gathertime", DateUtil.getCurrentDate()); // 采集时间
					rowData.put("inserttime", DateUtil.getCurrentDate()); // 入kafka时间
					// rowData.put("table",table); //所属表

					// 转换成json字符串
					id = rs.getInt("Id");
					jsonStr = JSON.toJSONString(rowData);
					// 发送kafka
					KafkaNewApiUtils.crawToKafa(KCConstant.LUNTAN,
							KCConstant.BROKERS, id + keyid, jsonStr);
					count++;
//					if (id % 200 == 0) { // 200条也记一次
//						client.setData().forPath(ROOT + table,
//								String.valueOf(id).getBytes());
//					}
					// System.out.println(jsonStr);
				}
				// 提交事务 跟新
				conn.commit();
				// 设置采集到的id
//				client.setData().forPath(ROOT + table,
//						String.valueOf(finishId).getBytes());

			} catch (SQLException e) {
				e.printStackTrace();
//				System.out.println("报错表==> " + table);
//				try {
//					conn.rollback();// 回滚
//					client.setData().forPath(ROOT + table,
//							String.valueOf(preId).getBytes());
//
//				} catch (SQLException e1) {
//					e1.printStackTrace();
//				} catch (Exception e2) {
//					e2.printStackTrace();
//				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			System.out.println("发送了===>"+ count);
		}
		// 关闭kafka 生产者
		// 关闭zookeeper
		// CloseableUtils.closeQuietly(client);

//	}

}

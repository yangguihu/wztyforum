package com.wiseweb.util;

import org.apache.curator.framework.CuratorFramework;

public class ZkUtil {
	
	public static int getData(CuratorFramework client,String path){
		byte[] bs = null;
		try {
			bs = client.getData().forPath(path);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return Integer.valueOf(new String(bs));
	}
	
	public static long getDataLong(CuratorFramework client,String path){
		byte[] bs = null;
		try {
			bs = client.getData().forPath(path);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return Long.valueOf(new String(bs));
	}
	
	
	
	public static void main(String[] args) {
//		CuratorFramework client = CuratorFrameworkFactory.newClient(
//				"spark1:2181,spark2:2181,spark3:2181", new ExponentialBackoffRetry(1000, Integer.MAX_VALUE));
//		client.start();
//		int data = getData(client,"/wzty/forum/test");
//		System.out.println(data);
//		client.close();
		System.out.println(System.currentTimeMillis()/1000);
	}

}

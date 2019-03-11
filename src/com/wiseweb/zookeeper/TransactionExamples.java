package com.wiseweb.zookeeper;

import java.util.Collection;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;

public class TransactionExamples {
	private static CuratorFramework client = CuratorFrameworkFactory.newClient(
			"spark1:2181,spark2:2181,spark3:2181", new ExponentialBackoffRetry(1000, 3)); 
	  
    public static void main(String[] args) {  
        try {  
            client.start();  
            // 开启事务  
            CuratorTransaction transaction = client.inTransaction();  
  
            Collection<CuratorTransactionResult> results = transaction.create()  
                    .forPath("/a/path", "some data".getBytes()).and().setData()  
                    .forPath("/another/path", "other data".getBytes()).and().delete().forPath("/yet/another/path")  
                    .and().commit();  
  
            for (CuratorTransactionResult result : results) {  
                System.out.println(result.getForPath() + " - " + result.getType());  
            }  
        } catch (Exception e) {  
            e.printStackTrace();  
        } finally {  
            // 释放客户端连接  
            CloseableUtils.closeQuietly(client);  
        }  
  
    } 
}

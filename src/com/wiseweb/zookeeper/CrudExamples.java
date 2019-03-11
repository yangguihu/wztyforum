package com.wiseweb.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.data.Stat;

public class CrudExamples {
	
//    CuratorFramework client = builder.connectString("192.168.11.56:2180")  
//            .sessionTimeoutMs(30000)  
//            .connectionTimeoutMs(30000)  
//            .canBeReadOnly(false)  
//            .retryPolicy(new ExponentialBackoffRetry(1000, Integer.MAX_VALUE))  
//            .namespace(namespace)  
//            .defaultData(null)  
//            .build();  
//    client.start();  
	
	
	private static CuratorFramework client = CuratorFrameworkFactory.newClient(
			"spark1:2181,spark2:2181,spark3:2181", new ExponentialBackoffRetry(1000, 3));  
    private static final String PATH = "/wzty/forum/test";  
  
    public static void main(String[] args) {  
        try {  
            client.start();  
  
            client.create().forPath(PATH, "123".getBytes());  
  
            byte[] bs = client.getData().forPath(PATH);  
            System.out.println("新建的节点，data为:" + new String(bs));  
  
            client.setData().forPath(PATH, "I love football".getBytes());  
  
            // 由于是在background模式下获取的data，此时的bs可能为null  
            byte[] bs2 = client.getData().watched().inBackground().forPath(PATH);  
            System.out.println("修改后的data为" + new String(bs2 != null ? bs2 : new byte[0]));  
  
            client.delete().forPath(PATH);  
            Stat stat = client.checkExists().forPath(PATH);  
  
            // Stat就是对zonde所有属性的一个映射， stat=null表示节点不存在！  
            System.out.println(stat);  
        } catch (Exception e) {  
            e.printStackTrace();  
        } finally {  
            CloseableUtils.closeQuietly(client);


        }  
    }  

}

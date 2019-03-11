package com.wiseweb.test;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.LinkedBlockingQueue;

public class Test {
	public static void main(String[] args) throws InterruptedException {
		StringBuffer sb=new StringBuffer("");
		System.out.println("   ".toString());
		
		
//		System.out.println(Calendar.getInstance(TimeZone.getTimeZone("GMT+08:00")).getTime());
//		System.out.println(Calendar.getInstance(TimeZone.getTimeZone("GMT+08:00")).getTime());
//		System.out.println(new Date());
//		System.out.println(new Date()+"消息发送失败");
//		LinkedBlockingQueue<Long> queue=new LinkedBlockingQueue<Long>();
//		for (long i = 0; i < 5; i++) {
//			queue.add(i);
//		}
//		while(true){
//			for (long i = 0; i < 10000; i++) {
//				queue.add(i);
//				Thread.sleep(1000);
//				
//				System.out.println(queue.poll());
//			}
//		}
	}
}
class MyThread extends Thread {  
	private LinkedBlockingQueue<Long> queue;
	public MyThread(LinkedBlockingQueue<Long> queue) {
		this.queue = queue;
	}
	public void run() {
		for (long i = 0; i < 10000; i++) {
			queue.add(i);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}

class MyThread2 extends Thread {
	private LinkedBlockingQueue<Long> queue;
	public MyThread2(LinkedBlockingQueue<Long> queue) {
		this.queue = queue;
	}
	public void run() {
		while(queue.peek()!=null){
			System.out.println(queue.poll());
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
} 

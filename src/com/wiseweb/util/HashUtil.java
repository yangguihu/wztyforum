package com.wiseweb.util;

import java.io.ByteArrayInputStream;

import net.jpountz.xxhash.StreamingXXHash64;
import net.jpountz.xxhash.XXHashFactory;

public class HashUtil {
	  public static long xxHash(Object str) {
		  try {
			  XXHashFactory factory = XXHashFactory.fastestInstance();

			  byte[] data = str.toString().getBytes("UTF-8");
			  ByteArrayInputStream in = new ByteArrayInputStream(data);

			  int seed = 0; // used to initialize the hash value, use whatever
			  // value you want, but always the same
			  StreamingXXHash64 hash64 = factory.newStreamingHash64(seed);
			  byte[] buf = new byte[64]; // for real-world usage, use a larger buffer, like 8192 bytes
			  for (; ; ) {
				  int read = in.read(buf);
				  if (read == -1) {
					  break;
				  }
				  hash64.update(buf, 0, read);
			  }
			  long hash = hash64.getValue();
			  return hash;
		  } catch (Exception e) {
			  return 0L;
		  }
	  }
	
      
	public static void main(String[] args) {
		System.out.println(xxHash("http://tieba.baidu.com/p/3197144448"));
	}


}

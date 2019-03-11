package com.wiseweb.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RecordMaping {
	/**
	 * 论坛字段映射
	 */
	public static Map<String,String> resMap= new HashMap<String,String>();
	static{
		 //resMap.put("Id","Id");
		 //resMap.put("url","PageUrl");
		 resMap.put("site_url","entry_url");
		 //resMap.put("content","主贴");
		 resMap.put("author","作者");
		 //resMap.put("replycontent","回贴");
		 //resMap.put("publishtime","时间");
		 resMap.put("title","标题");
		 //resMap.put("site_name","栏目");
		 resMap.put("click","点击量");
		 resMap.put("reply","评论数");
		 resMap.put("reposts_count","转载量");
	}
	
	/**
	 * 论坛采集字段集合
	 */
	public static Set<String> forumSet= new HashSet<String>();
	static{
		//forumSet.add("id");
		//forumSet.add("site_name");
		forumSet.add("group_id");
		forumSet.add("title");
		forumSet.add("source");
		forumSet.add("author");
		forumSet.add("click");
		forumSet.add("reply");
		//forumSet.add("publishtime");
		//forumSet.add("content");
		//forumSet.add("replycontent");
		//forumSet.add("url");
		//forumSet.add("gathertime");
		//forumSet.add("inserttime");
		forumSet.add("task_id");
	}
	
	/**
	 * 首页字段集合
	 */
	public static Set<String> homeSet= new HashSet<String>();
	static{
		//homeSet.add("id");
		homeSet.add("site_name");
		homeSet.add("group_id");
		homeSet.add("title");
		homeSet.add("source");
		//homeSet.add("publishtime");
		homeSet.add("content");
		//homeSet.add("url");
		//homeSet.add("gathertime");
		//homeSet.add("inserttime");
		homeSet.add("task_id");
	}
	/**
	 * 外媒字段集合
	 */
	public static Set<String> foreignSet= new HashSet<String>();
	static{
		//foreignSet.add("id");
		foreignSet.add("site_name");
		foreignSet.add("group_id");
		foreignSet.add("title");
		foreignSet.add("source");
		//foreignSet.add("publishtime");
		foreignSet.add("content");
		foreignSet.add("language");
		//foreignSet.add("url");
		//foreignSet.add("gathertime");
		//foreignSet.add("inserttime");
	}
}

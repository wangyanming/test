package com.hds.cn.bi;

import java.util.HashSet;
import java.util.Set;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.hds.cn.bi.util.EsClient;

public class Demo1 {
	static Set<Object> set = new HashSet<Object>();
	
	public static void main(String[] args) {
		System.out.println(System.currentTimeMillis());
		Thread t1 = new Thread() {
			public void run() {
				SearchResponse sr = EsClient.getConnect().prepareSearch("order")
						.setQuery(QueryBuilders.boolQuery()
								.must(QueryBuilders.rangeQuery("end_time.keyword").gte("2018-06-16").lte("2018-06-18"))
								.must(QueryBuilders.matchQuery("org_id", 10170))
								.must(QueryBuilders.matchQuery("event_type", "paidOrder"))
								)
						.setExplain(false)
						.setFetchSource("user_id", "")
						.setSize(100000).get();
				
				System.out.println(System.currentTimeMillis());
				
				for (SearchHit srHit : sr.getHits().getHits()) {
					set.add(srHit.getSourceAsMap().get("user_id"));
				}
				
				System.out.println(System.currentTimeMillis());
				
				System.out.println(set.size());
			}
		};
		
		t1.start();
	}
}

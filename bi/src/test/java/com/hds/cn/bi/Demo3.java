package com.hds.cn.bi;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;

import com.hds.cn.bi.util.EsClient;

public class Demo3 {
	public static void main(String[] args) {
		SearchResponse sr = EsClient.getConnect().prepareSearch("json")
				.setQuery(QueryBuilders.nestedQuery("log_extra", QueryBuilders.boolQuery()
						.must(QueryBuilders.matchQuery("log_extra.file", "nohupLog")), ScoreMode.None))
				.get();
		
		System.out.println(sr);
	}
}

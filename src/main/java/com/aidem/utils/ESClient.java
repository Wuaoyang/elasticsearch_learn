package com.aidem.utils;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * xxxxxxxxxxxx
 *
 * @author aosun_wu
 * @date 2020-11-21 14:40
 */
public class ESClient {

	public static RestHighLevelClient getClient() {

		// 创建HttpHost对象
		HttpHost httpHost = new HttpHost("你的ip，若本地即为127.0.0.1", 9200);

		// 创建RestClientBuilder
		RestClientBuilder clientBuilder = RestClient.builder(httpHost);

		// 创建RestHighLevelClient
		RestHighLevelClient restHighLevelClient = new RestHighLevelClient(clientBuilder);

		return restHighLevelClient;

	}

}

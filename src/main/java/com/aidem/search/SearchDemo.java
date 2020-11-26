package com.aidem.search;

import com.aidem.search.entity.SmsLogs;
import com.aidem.utils.ESClient;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * 查询专用Demo
 *
 * @author aosun_wu
 * @date 2020-11-24 22:45
 */
public class SearchDemo {

    //  按照阿里规范 静态常量最好是大写开头 因借鉴了部分博客的代码 懒得改哈 友情提示
    private RestHighLevelClient client = ESClient.getClient();
    private Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
    private String index = "sms-logs-index";
    private String type = "sms-logs-type";

    /**
     * 创建索引
     */
    @Test
    public void createIndex() throws Exception {
        // 1.准备关于索引的setting
        Settings.Builder settings = Settings.builder().put("number_of_shards", 3).put("number_of_replicas", 1);
        // 2.准备关于索引的mapping
        XContentBuilder mappings = JsonXContent.contentBuilder().startObject().startObject("properties").startObject("corpName").field("type", "keyword").endObject().startObject("createDate").field("type", "date").field("format", "yyyy-MM-dd").endObject().startObject("fee").field("type", "long").endObject().startObject("ipAddr").field("type", "ip").endObject().startObject("longCode").field("type", "keyword").endObject().startObject("mobile").field("type", "keyword").endObject().startObject("operatorId").field("type", "integer").endObject().startObject("province").field("type", "keyword").endObject().startObject("replyTotal").field("type", "integer").endObject().startObject("sendDate").field("type", "date").field("format", "yyyy-MM-dd").endObject().startObject("smsContent").field("type", "text").field("analyzer", "ik_max_word").endObject().startObject("state").field("type", "integer").endObject().endObject().endObject();
        // 3.将settings和mappings 封装到到一个Request对象中
        CreateIndexRequest request = new CreateIndexRequest(index).settings(settings).mapping(type, mappings);
        // 4.使用client 去连接ES
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println("response:" + response.toString());

    }

    /**
     * 批量插入测试数据
     */
    @Test
    public void bulkCreateDoc() throws Exception {
        // 1.准备多个json 对象
        String longcode = "1008687";
        String mobile = "138340658";
        List<String> companies = new ArrayList<>();
        companies.add("腾讯课堂");
        companies.add("阿里旺旺");
        companies.add("海尔电器");
        companies.add("海尔智家公司");
        companies.add("格力汽车");
        companies.add("苏宁易购");
        List<String> provinces = new ArrayList<>();
        provinces.add("北京");
        provinces.add("重庆");
        provinces.add("上海");
        provinces.add("晋城");
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 1; i < 16; i++) {
            Thread.sleep(200);
            SmsLogs s1 = new SmsLogs();
            s1.setId(i);
            s1.setCreateDate(new Date());
            s1.setSendDate(new Date());
            s1.setLongCode(longcode + i);
            s1.setMobile(mobile + 2 * i);
            s1.setCorpName(companies.get(i % 5));
            s1.setSmsContent(SmsLogs.doc.substring((i - 1) * 100, i * 100));
            s1.setState(i % 2);
            s1.setOperatorId(i % 3);
            s1.setProvince(provinces.get(i % 4));
            s1.setIpAddr("127.0.0." + i);
            s1.setReplyTotal(i * 3);
            s1.setFee(i * 6 + "");
            String json1 = gson.toJson(s1);
            bulkRequest.add(new IndexRequest(index, type, s1.getId().toString()).source(json1, XContentType.JSON));
            System.out.println("数据" + i + s1.toString());
        }
        // 3.client 执行
        BulkResponse responses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        // 4.输出结果
        System.out.println(responses.getItems());
    }

    /**
     * 使用 term 查询
     */
    @Test
    public void termSearchTest() throws IOException {
        // 1.创建request对象
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        // 2.创建查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.from(0);
        builder.size(5);
        builder.query(QueryBuilders.termQuery("province", "北京"));
        request.source(builder);
        // 3.执行查询
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.输出查询结果
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
    }

    /**
     * 使用 terms 查询
     */
    @Test
    public void termsSearchTest() throws IOException {
        // 1.创建request对象
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        // 2.创建查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.from(0);
        builder.size(5);
        // 和term的例子仅仅是改成了termsQuery 同时第二个参数传入了String数组
        // 跟进去很容易看到，其实termQuery 和 termsQuery逻辑都一样，实际都是转成数组去处理。
        builder.query(QueryBuilders.termsQuery("province", "北京", "晋城"));
        request.source(builder);
        // 3.执行查询
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.输出查询结果
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
    }

    /**
     * 使用 matchAll 查询
     */
    @Test
    public void matchAllSearch() throws IOException {
        // 1.创建request对象
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        //  2.创建查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        //  ES 默认只查询10条数据
        builder.size(20);
        request.source(builder);
        //  3.执行查询
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.输出查询结果
        for (SearchHit hit : response.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
        System.out.println(response.getHits().getHits().length);
    }

    /**
     * 使用 match  查询 单个字段（会发现，他已经会基于分词器进行对“伟大战士”进行分词搜索了）
     */
    @Test
    public void matchSearch() throws IOException {
        // 1.创建request对象
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        //  2.创建查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        //--------------------------------------------------------------
        builder.query(QueryBuilders.matchQuery("smsContent","伟大战士"));
        //--------------------------------------------------------------
        builder.size(20);
        request.source(builder);
        //  3.执行查询
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.输出查询结果
        for (SearchHit hit : response.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
        System.out.println(response.getHits().getHits().length);
    }

    /**
     * 布尔match
     * 实现类似 and / or 的操作
     */
    @Test
    public void booleanMatchSearch() throws IOException {
        // 1.创建request对象
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        //  2.创建查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        //--------------------------------------------------------------
        // and
        builder.query(QueryBuilders.matchQuery("smsContent","战士 团队").operator(Operator.AND));
        // or
//        builder.query(QueryBuilders.matchQuery("smsContent","战士 团队").operator(Operator.OR));
        //--------------------------------------------------------------
        builder.size(20);
        request.source(builder);
        //  3.执行查询
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.输出查询结果
        for (SearchHit hit : response.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
        System.out.println(response.getHits().getHits().length);
    }

}

package com.aidem.search;

import com.aidem.search.entity.SmsLogs;
import com.aidem.utils.ESClient;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * 各种查询专用Demo
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
        builder.query(QueryBuilders.matchQuery("smsContent", "伟大战士"));
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
        builder.query(QueryBuilders.matchQuery("smsContent", "战士 团队").operator(Operator.AND));
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

    /**
     * multi_match
     * 实现多字段match的操作
     */
    @Test
    public void multiMatchSearch() throws IOException {
        // 1.创建request对象
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        //  2.创建查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        //--------------------------------------------------------------
        builder.query(QueryBuilders.multiMatchQuery("团队 管理", "smsContent", "province").operator(Operator.AND));
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
     * 根据id查询
     */
    @Test
    public void idSearch() throws IOException {
        // 1. 构造request
        GetRequest getRequest = new GetRequest(index, type, "1");
        // 2. 执行查询
        GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
        // 3. 返回结果
        System.out.println(response.getSourceAsMap());
    }

    /**
     * 根据多个id查询
     */
    @Test
    public void idsSearch() throws IOException {
        // 1. 构造request
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        // 2. 构造查询体（相当于_search{}，花括号里的内容）
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.from(0);
        builder.size(20);
        builder.query(QueryBuilders.idsQuery().addIds("1", "2", "3"));
        request.source(builder);
        // 3. 执行查询
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4. 返回结果
        for (SearchHit hit : response.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
    }

    /**
     * 根据prefix查询
     */
    @Test
    public void prefixSearch() throws IOException {
        //  创建request对象
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        //  指定查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        //--------------------------------------------------
        builder.query(QueryBuilders.prefixQuery("corpName", "阿"));
        //------------------------------------------------------
        request.source(builder);
        // 执行
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 输出结果
        for (SearchHit hit : response.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
    }

    /**
     * 根据fuzzy查询
     */
    @Test
    public void fuzzySearch() throws IOException {
        //  创建request对象
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        //  指定查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        //--------------------------------------------------
        builder.query(QueryBuilders.fuzzyQuery("corpName", "腾讯客堂"));
        //------------------------------------------------------
        request.source(builder);
        // 执行
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 输出结果
        for (SearchHit hit : response.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
    }

    /**
     * 根据wildcard查询
     */
    @Test
    public void wildcardSearch() throws IOException {
        //  创建request对象
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        //  指定查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        //--------------------------------------------------
        builder.query(QueryBuilders.wildcardQuery("corpName", "海尔*"));
        //------------------------------------------------------
        request.source(builder);
        // 执行
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 输出结果
        for (SearchHit hit : response.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
    }

    /**
     * 根据range查询
     */
    @Test
    public void rangeSearch() throws IOException {
        // 1. 创建request
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        // 2. 指定查询内容
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.rangeQuery("fee").lte(20).gte(10));
        request.source(builder);
        // 3. 查询，获取结果
        SearchResponse search = client.search(request, RequestOptions.DEFAULT);
        // 4. 打印结果
        for (SearchHit hit : search.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
    }

    /**
     * 深分页 scroll
     */
    @Test
    public void scrollSearch() throws IOException {
        // 1.创建request
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(type);
        //  2.指定scroll信息,过期时间
        searchRequest.scroll(TimeValue.timeValueMinutes(1L));
        //  3.指定查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.size(4);
        builder.sort("fee", SortOrder.DESC);
        searchRequest.source(builder);
        // 4.获取返回结果scrollId,获取source
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        String scrollId = response.getScrollId();
        System.out.println("-------------首页数据---------------------");
        for (SearchHit hit : response.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
        while (true) {
            // 5.创建scroll request
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            // 6.指定scroll 有效时间
            scrollRequest.scroll(TimeValue.timeValueMinutes(1L));
            // 7.执行查询，返回查询结果
            SearchResponse scroll = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            // 8.判断是否查询到数据，查询到输出
            SearchHit[] searchHits = scroll.getHits().getHits();
            if (searchHits != null && searchHits.length > 0) {
                System.out.println("-------------下一页数据---------------------");
                for (SearchHit hit : searchHits) {
                    System.out.println(hit.getSourceAsMap());
                }
            } else {
                //  9.没有数据，结束
                System.out.println("-------------结束---------------------");
                break;
            }
        }
        // 10.创建 clearScrollRequest
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        // 11.指定scrollId
        clearScrollRequest.addScrollId(scrollId);
        //12.删除scroll
        ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        // 13.输出结果
        System.out.println("删除scroll:" + clearScrollResponse.isSucceeded());
    }

    /**
     * deleteByQuery， 根据条件删除
     * 若删除多的话，建议将数据查询出来，插入到新的index里，效率会高点
     */
    public void deleteByQuery() throws IOException {
        // 1.创建DeleteByQueryRequest
        DeleteByQueryRequest request = new DeleteByQueryRequest(index);
        request.types(type);
        // 2.指定条件
        request.setQuery(QueryBuilders.rangeQuery("fee").lt(20));
        // 3.执行
        BulkByScrollResponse response = client.deleteByQuery(request, RequestOptions.DEFAULT);
        // 4.输出返回结果
        System.out.println(response.toString());
    }




}

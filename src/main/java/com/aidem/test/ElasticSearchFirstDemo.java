package com.aidem.test;

import com.aidem.utils.ESClient;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;

/**
 * 测试例子，包含了各种api操作
 *
 * @author aosun_wu
 * @date 2020-11-21 14:48
 */
public class ElasticSearchFirstDemo {

    /**
     * es chient
     */
    private RestHighLevelClient client = ESClient.getClient();

    /**
     * gson
     */
    private Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();

    /**
     * 索引名称
     */
    private static final String INDEX = "aidem_java_index";
    /**
     * 文档类型
     */
    private static final String TYPE = "_doc";

    // ============================================ 索引篇 ============================================

    /**
     * 增加索引
     */
    @Test
    public void testConnect() throws IOException {
        // 1. 准备request 对象
        CreateIndexRequest request = new CreateIndexRequest();
        // 2. 构造创建settings 和 mappings
        Settings.Builder settings = Settings.builder().put("number_of_shards", 3).put("number_of_replicas", 1);
        XContentBuilder mappings = JsonXContent.contentBuilder()
                .startObject()
                .startObject("properties")
                .startObject("name")
                .field("type", "text")
                .endObject()
                .startObject("age")
                .field("type", "integer")
                .endObject()
                .startObject("birthday")
                .field("type", "date")
                .field("format", "yyyy-MM-dd")
                .endObject()
                .endObject()
                .endObject();
        // 3. 设置属性
        request.index(INDEX).settings(settings).mapping(TYPE, mappings);
        // 4. 使用client，操作es
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }

    /**
     * 判断索引是否存在
     */
    @Test
    public void testIndexExists() throws IOException {
        // 1.构造request
        GetIndexRequest request = new GetIndexRequest().indices(INDEX);
        // 2.使用client进行判断
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * 删除索引
     */
    @Test
    public void deleteIndex() throws IOException {
        // 1. 构造request
        DeleteIndexRequest request = new DeleteIndexRequest().indices(INDEX);
        // 2. 使用client删除（一般情况使用要先判断是否存在）
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }

    // ============================================ 文档篇 ============================================

    /**
     * 添加文档，可覆盖
     */
    @Test
    public void addDocument() throws IOException {
        // 1. 构造文档内容
        AidemJavaIndex entity = new AidemJavaIndex(22, 23, new Date(), "吴傲阳(aidem)");
        // 2.  构造request
        IndexRequest request = new IndexRequest(INDEX, TYPE, entity.getId().toString());
        request.source(gson.toJson(entity), XContentType.JSON);
        // 3. 使用client操作
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        System.out.println(response.getResult().toString());
    }

    /**
     * 修改文档
     */
    @Test
    public void updateDocument() throws IOException {
        // 1. 构造文档内容
        AidemJavaIndex entity = new AidemJavaIndex();
        entity.setId(1);
        entity.setName("修改！");
        // 2.  构造request
        UpdateRequest request = new UpdateRequest(INDEX, TYPE, entity.getId().toString());
        request.doc(gson.toJson(entity), XContentType.JSON);
        // 3. 使用client操作
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        System.out.println(response.getResult().toString());
    }

    /**
     * 删除文档
     */
    @Test
    public void deleteDocument() throws IOException {
        // 1.  构造request
        DeleteRequest request = new DeleteRequest(INDEX, TYPE, "1");
        // 2. 使用client操作
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        System.out.println(response.getResult().toString());
    }

    /**
     * 批量新增
     */
    @Test
    public void bulkInsertDoc() throws IOException {
        // 1.  准备多个json对象
        AidemJavaIndex a1 = new AidemJavaIndex(100, 10, new Date(), "100");
        AidemJavaIndex a2 = new AidemJavaIndex(101, 11, new Date(), "101");
        AidemJavaIndex a3 = new AidemJavaIndex(102, 12, new Date(), "102");
        String json1 = gson.toJson(a1);
        String json2 = gson.toJson(a2);
        String json3 = gson.toJson(a3);
        // 2. 构造bulk request
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest(INDEX, TYPE, a1.getId().toString()).source(json1, XContentType.JSON))
                .add(new IndexRequest(INDEX, TYPE, a2.getId().toString()).source(json2, XContentType.JSON))
                .add(new IndexRequest(INDEX, TYPE, a3.getId().toString()).source(json3, XContentType.JSON));
        // 3. 使用client操作
        BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
        System.out.println(response.getItems().toString());
    }

    /**
     * 批量删除
     */
    @Test
    public void bulkDeleteDoc() throws IOException {
        // 1. 构造bulk request
        BulkRequest request = new BulkRequest();
        request.add(new DeleteRequest(INDEX, TYPE, "101"))
                .add(new DeleteRequest(INDEX, TYPE, "102"))
                .add(new DeleteRequest(INDEX, TYPE, "103"));
        // 2. 使用client操作
        BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
        System.out.println(response.getItems().toString());
    }


}

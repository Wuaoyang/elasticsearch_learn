package com.aidem.utils.test;

import com.aidem.utils.ESClient;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Test;

import java.io.IOException;

/**
 * xxxxxxxxxxxx
 *
 * @author aosun_wu
 * @date 2020-11-21 14:48
 */
public class Demo {

    RestHighLevelClient client =  ESClient.getClient();

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
        String type = "_doc";
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
        request.index("aidem_java_index").settings(settings).mapping(type, mappings);
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
        GetIndexRequest request = new GetIndexRequest().indices("aidem_java_index");
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
        DeleteIndexRequest request = new DeleteIndexRequest().indices("aidem_java_index");
        // 2. 使用client删除（一般情况使用要先判断是否存在）
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.toString());
    }

    // ============================================ 文档篇 ============================================




}

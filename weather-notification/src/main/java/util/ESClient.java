package util;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class ESClient {
    public  static TransportClient getClient() throws UnknownHostException {
        // 设置集群名称biehl01
                 Settings settings = Settings.builder().put("cluster.name", "elasticsearch")
                         .build();
                 // 创建client
        TransportClient  client = new PreBuiltTransportClient(settings).addTransportAddresses(
                               // 建议指定2个及其以上的节点。
                                new TransportAddress(InetAddress.getByName("localhost"), 9200));

        return client;
    }

    public static RestHighLevelClient restClient(){
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));
        return client;

    }

    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = restClient();



        String doc = "{\"type\":\"WeatherAlert\",\"_status\":\"inactive\",\"lastUpdated\":\"2021-09-15T05:25:29.621Z\"}";

        IndexRequest request = new IndexRequest("weather-alert", "_doc", "6cp2ph8txtmm3wc5nge6mp5eq");
        request.source(doc, XContentType.JSON);

        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

        System.out.println(indexResponse);
    }
}

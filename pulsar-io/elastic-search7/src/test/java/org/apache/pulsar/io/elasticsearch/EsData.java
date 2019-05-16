package org.apache.pulsar.io.elasticsearch;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Random;

import static org.apache.pulsar.io.elasticsearch.ElasticSearchSink.DOCUMENT;

public class EsData {

    private static RestHighLevelClient client;
    private static URL url;
    private static CredentialsProvider credentialsProvider;

    public static void main(String[] args) throws IOException {
        Gson gson = new Gson();
        JsonObject object = new JsonObject();
        Random random = new Random();
        object.addProperty("id", "a300");
        object.addProperty("cs2001", random.nextInt(1));
        object.addProperty("tof", random.nextInt(100));

        IndexRequest indexRequest = Requests.indexRequest("yunhorn");
        indexRequest.type(DOCUMENT);
        indexRequest.source(gson.toJson(random), XContentType.JSON);
        try {
            IndexResponse indexResponse = getClient().index(indexRequest);
            System.out.println(indexResponse.getResult().equals(DocWriteResponse.Result.CREATED));
        } catch (final IOException ex) {
            ex.printStackTrace();
        }
        getClient().close();
    }
    private static RestHighLevelClient getClient() throws MalformedURLException {
        if (client == null) {
            CredentialsProvider cp = getCredentialsProvider();
            RestClientBuilder builder = RestClient.builder(new HttpHost(getUrl().getHost(),
                    getUrl().getPort(), getUrl().getProtocol()));
            client = new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.1.9",9200,"http")));
//            if (cp != null) {
//                builder.setHttpClientConfigCallback(httpClientBuilder ->
//                        httpClientBuilder.setDefaultCredentialsProvider(cp));
//            }
//            client = new RestHighLevelClient(builder);
        }
        return client;
    }

    private static CredentialsProvider getCredentialsProvider() {
        credentialsProvider = new BasicCredentialsProvider();
        return credentialsProvider;
    }

    private static URL getUrl() throws MalformedURLException {
        if (url == null) {
            url = new URL("http://192.168.1.9:9200");
        }
        return url;
    }

}

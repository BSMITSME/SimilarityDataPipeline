package elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.json.JSONArray;
import org.json.JSONObject;
import org.python.bouncycastle.util.encoders.UTF8;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class ElasticsearchData {
    private static RestClient restClient;
    private static Request request;
    ObjectMapper mapper = new ObjectMapper();;
    public void getClient(){
        String CREDENTIALS_STRING = "elastic:NBAH*onU+lD1dRtECfek";
        String encodedBytes = Base64.getEncoder().encodeToString(CREDENTIALS_STRING.getBytes());
        Header[] auto_headers_testCluster = {
                new BasicHeader("Authorization", "Basic " + encodedBytes)
        };

        restClient = RestClient.builder(
                new HttpHost("192.XXX.XX.XX", 9200, "http"),
                new HttpHost("192.XXX.XX.XX", 9200, "http"),
                new HttpHost("192.XXX.XX.XX", 9200, "http")
        ).setDefaultHeaders(auto_headers_testCluster).build();
    }
    public Response postDataSimilarity(StringBuffer js) throws IOException{
        request = new Request("POST", "sample_v4_lucy3_main_20240221_similarity/_bulk");
        request.setEntity(new NStringEntity(js.toString(), ContentType.APPLICATION_JSON));
        Response response = restClient.performRequest(request);
        return response;
    }
    public Response postDataSimCount(StringBuffer js) throws IOException{
        request = new Request("POST", "sample_v4_lucy3_main_20240221_sim_count/_bulk");
        request.setEntity(new NStringEntity(js.toString(), ContentType.APPLICATION_JSON));

        Response response = restClient.performRequest(request);
        return response;
    }

    public StringBuffer makeBulk(String i, String an_title, String content, String pid, String kw_docid, String p){
        StringBuffer js = new StringBuffer();
        String requestBody = "{\"index\":{\"_id\":\""+(i)+"\"}}";
        JSONObject bulkData = new JSONObject();
        bulkData.put("an_title", an_title);
        bulkData.put("an_content", content);
        bulkData.put("pid", pid);
        bulkData.put("kw_docid", kw_docid);
        bulkData.put("relation", p);
        js.append(requestBody).append('\n').append(bulkData.toString()).append("\n");
        return js;
    }

    public JSONArray getData(String index, int size, String search_after_value) throws IOException {
        Response rs = null;
        request = new Request("GET", index + "/_search");

        request.setEntity(new NStringEntity(query(size, search_after_value), ContentType.APPLICATION_JSON));

        rs = restClient.performRequest(request);
        String rs_str = EntityUtils.toString(rs.getEntity());
        JSONObject jo = new JSONObject(rs_str);

        JSONArray jsonArray = jo.getJSONObject("hits").getJSONArray("hits");
        return jsonArray;
    }
    public String query(int size, String search_after_value){
        String query = "\n" +
                "{\n" +
                "  \"size\" : "+size+",\n" +
                "  \"query\" : {\n" +
                "    \"query_string\" : {\n" +
                "      \"query\" : \"*\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"sort\": [\n" +
                "    {\n" +
                "      \"kw_docid\": {\n" +
                "        \"order\": \"asc\"\n" +
                "      }\n" +
                "    }\n" +
                "  ],\n" +
                "  \"search_after\": [\""+search_after_value+"\"]\n" +
                "}";
        return query;
    }
    public void closeConnection(){
        try{
            restClient.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}

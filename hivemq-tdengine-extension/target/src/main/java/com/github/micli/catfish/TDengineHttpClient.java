


package com.github.micli.catfish;

import java.net.URI;
import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
// import org.apache.http.impl.client.HttpClientBuilder;
// import org.apache.http.impl.client.HttpClients;

import com.alibaba.fastjson.JSON;


public class TDengineHttpClient {

    private String serverHost = "";
    private int serverPort = 6041;
    private String username = "";
    private String password = "";
    private String accessToken = "";
    private int connectTimeout = 3000;
    private String database = "";
    private String tablePrefix = "";

    public TDengineHttpClient(String host, int port, String username, String password, int connectTimeout, 
        final String database, final String tablePrefix) throws Exception {
            this.serverHost = host;
            this.serverPort = port;
            this.username = username;
            this.password = password;
            this.connectTimeout = connectTimeout;
            this.database = database;
            this.tablePrefix = tablePrefix;

            this.accessToken = this.getAccessToken();
    }

    public String getAuthToken(){
        return this.accessToken;
    }


    /*
        This function used for retrieve access token from server by username and password.
    */
    public String getAccessToken() throws Exception {
        if("" == this.username || "" == this.password)
        return "";
        URI uri = new URIBuilder()
        .setScheme("http")
        .setHost(serverHost + ":" + String.valueOf(serverPort))
        .setPath("/rest/login/" + this.username + "/" + this.password)
        .build();
        CloseableHttpClient client = HttpClients.createDefault();
        //请求url
        HttpGet httpGet = new HttpGet(uri);
        //添加请求头
        httpGet.addHeader("Content-Type", "application/json");
        httpGet.addHeader("Accept", "application/json");
        try {
            //发起请求
            CloseableHttpResponse response = client.execute(httpGet);
            //获得请求状态码
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
            //获得请求实体
            HttpEntity entity = response.getEntity();
            String result = EntityUtils.toString(entity, "utf-8");
            TDengineAuthResult authResult = JSON.parseObject(result, TDengineAuthResult.class);
            if(authResult.getStatus() == "succ") {
                return authResult.getDesc();
            }
        }
        } catch (Exception e) {
            return "";
        }
        return "";
    }

    public void WriteData() throws Exception {

    }

    private String executeSQL(String sqlStatement) throws Exception {
        return "";
    }
}

package com.github.micli.catfish;

import java.net.URI;
import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;
// import org.apache.http.HttpHost;
// import org.apache.http.HttpResponse;
// import org.apache.http.NameValuePair;
// import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.*;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
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

    public TDengineHttpClient(final String host, final int port, final String username, final String password,
            final int connectTimeout, final String database, final String tablePrefix) throws Exception {
        this.serverHost = host;
        this.serverPort = port;
        this.username = username;
        this.password = password;
        this.connectTimeout = connectTimeout;
        this.database = database;
        this.tablePrefix = tablePrefix;

        this.accessToken = this.getAccessToken();
    }

    public String getAuthToken() {
        return this.accessToken;
    }

    /*
     * This function used for retrieve access token from server by username and
     * password.
     */
    public String getAccessToken() throws Exception {
        if (this.username.equals("") || this.password.equals(""))
            return "";
        final URI uri = new URIBuilder().setScheme("http").setHost(serverHost + ":" + String.valueOf(serverPort))
                .setPath("/rest/login/" + this.username + "/" + this.password).build();
        final CloseableHttpClient client = HttpClients.createDefault();
        // 请求url
        final HttpGet httpGet = new HttpGet(uri);
        // 添加请求头
        httpGet.addHeader("Content-Type", "application/json");
        httpGet.addHeader("Accept", "application/json");
        try {
            // Send request.
            final CloseableHttpResponse response = client.execute(httpGet);
            // Retrieve status code from response line.
            final int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                // 获得请求实体
                final HttpEntity entity = response.getEntity();
                final String result = EntityUtils.toString(entity, "utf-8");
                final TDengineAuthResult authResult = JSON.parseObject(result, TDengineAuthResult.class);
                if (authResult.getStatus().equals("succ")) {
                    return "Taosd " + authResult.getDesc();
                }
            }
        } catch (final Exception e) {
            return "";
        }
        return "";
    }

    public void WriteData() throws Exception {

    }

    public String CreateSuperTable() throws Exception {
        final String createTable = "CREATE TABLE mqtt_msg (ts timestamp, msgid NCHAR(64), topic NCHAR(255), qos TINYINT, payload BINARY(1024), arrived timestamp);";
        return executeSQL(createTable);
    }

    public String executeSQL(final String sqlStatement) throws Exception {
        final String newSqlStatement = buildSqlStatements(sqlStatement);
        return executeSQLInternal(newSqlStatement);
    }

    private String executeSQLInternal(final String sqlStatement) throws Exception {

        if (this.getAuthToken().equals("") || sqlStatement.equals(""))
            return "";

        final URI uri = new URIBuilder().setScheme("http").setHost(serverHost + ":" + String.valueOf(serverPort))
                .setPath("/rest/sql").build();
        final CloseableHttpClient client = HttpClients.createDefault();
        // Post Url
        final HttpPost thePost = new HttpPost(uri);
        // Headers
        thePost.addHeader("Content-Type", "application/json");
        thePost.addHeader("Accept", "application/json");
        thePost.addHeader("Authorization", this.accessToken);
        thePost.setEntity(new StringEntity(sqlStatement, "utf-8"));
        try {
            // Send request.
            final CloseableHttpResponse response = client.execute(thePost);
            // Retrieve status code from response line.
            final int statusCode = response.getStatusLine().getStatusCode();
            if (200 == statusCode) {
                final HttpEntity entity = response.getEntity();
                final String result = EntityUtils.toString(entity, "utf-8");
                return result;
            }
        } catch (final Exception ex) {
            return "";
        }
        return "";
    }
    /*
        Added a use database; statement by default to prevent execute on wrong database.
    */
    private String buildSqlStatements(final String sql) {
        if(database.equals(""))
            return sql;
        return "use " + database + "; " + sql;
    }
}
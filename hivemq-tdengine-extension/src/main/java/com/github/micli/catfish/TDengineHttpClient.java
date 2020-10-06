/*
* MIT License
* 
* Copyright (c) 2020 Michael Li
* 
* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files (the "Software"), to deal
* in the Software without restriction, including without limitation the rights
* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:
* 
* The above copyright notice and this permission notice shall be included in all
* copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
* SOFTWARE.
*/

package com.github.micli.catfish;

import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;

import com.alibaba.fastjson.JSON;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This is a REST API communication client. 
 * It can be used for save data to TDengine by REST API.
 *
 * @author Michael Li
 * @since 0.1 
 */
public class TDengineHttpClient {

    private String serverHost = "";
    private int serverPort = 0;
    private String username = "";
    private String password = "";
    private String accessToken = "";
    private int connectTimeout = 0;
    private String database = "";
    private String tablePrefix = "";
    private final String defaultEncode = "utf-8";

    private RequestConfig defaultRequestConfig = null;
    private static final Logger log = LoggerFactory.getLogger(TDengineHttpClient.class);
    private ConcurrentHashMap<String, String> deviceMap = new ConcurrentHashMap<String, String>(); 

    public TDengineHttpClient(final String host, final int port, final String username, final String password,
            final int connectTimeout, final String database, final String tablePrefix) throws Exception {
        this.serverHost = host;
        this.serverPort = port;
        this.username = username;
        this.password = password;
        this.connectTimeout = connectTimeout;
        this.database = database;
        this.tablePrefix = tablePrefix;
        // Create a default timecout config object.
        defaultRequestConfig = RequestConfig.custom().setSocketTimeout(this.connectTimeout)
            .setConnectTimeout(this.connectTimeout)
            .setConnectionRequestTimeout(connectTimeout).build();
        
        try {
            this.accessToken = this.getAccessToken();
            initialize();
        } catch(Exception e) {
            log.error("Initialize TDengineHttpClient error: ", e);
        }
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
        final CloseableHttpClient client = HttpClients.custom().setDefaultRequestConfig(defaultRequestConfig).build();

        final HttpGet httpGet = new HttpGet(uri);
        httpGet.addHeader("Accept", "application/json");
        try {
            // Send request.
            final CloseableHttpResponse response = client.execute(httpGet);
            // Retrieve status code from response line.
            final int statusCode = response.getStatusLine().getStatusCode();
            final HttpEntity entity = response.getEntity();
            final String result = EntityUtils.toString(entity, defaultEncode);
            if (statusCode == 200) {
                final TDengineAuthResult authResult = JSON.parseObject(result, TDengineAuthResult.class);
                if (authResult.getStatus().equals("succ")) {
                    return "Taosd " + authResult.getDesc();
                }
            }
        } catch (final Exception e) {
            log.error("getAccessToken() error: {}", e);
            return "";
        }
        return "";
    }
    private void initialize() throws Exception {
        // Create database.
        createDatabase();

        if(tableExists(tablePrefix)) {
            loadDevices();
        } else {
            createSuperTable();
        }
    }
    public void WriteData(String msgid, String deviceId, String topic, int qos, String payLoad) throws Exception {
        
        try {
            String tableName = "";
            if(deviceMap.containsKey(deviceId))
                tableName = deviceMap.get(deviceId);
            else {
                tableName = getTableNameFromDeviceId(deviceId);
                createTable(tableName, deviceId);
                deviceMap.put(deviceId, tableName);
            }
    
            Date now = new Date();  
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");  
            String timeString = dateFormat.format(now);  
            String sqlInsert = String.format(TDengineSQLCmdList.getGetInsert(), 
            database, tableName, timeString, msgid, topic, qos, payLoad);
            // executeSQL(sqlInsert);
            executeSQLAsync(sqlInsert); // Call HTTP async.
        } catch (Exception ex) {
            log.error("writeData error: {}", ex);
        }

    }

    public String createDatabase() throws Exception {
        final String sqlCreateDb = String.format(TDengineSQLCmdList.getCreateDb(), database);
        return executeSQL(sqlCreateDb);
    }
    public void changeToCurrentDatabase() throws Exception {
        final String sqlUseDb = String.format(TDengineSQLCmdList.getUseDb(), database);
        executeSQL(sqlUseDb);       
    }
    private boolean tableExists(final String tableName) throws Exception {
        final String sqlTableExists = String.format(TDengineSQLCmdList.getGetSubTable(), database, tableName);
        String result = executeSQL(sqlTableExists);
        if(result.length() > 0) {
            TDengineQueryResult tdResult = TDengineQueryResult.GetResult(result);
            if(!tdResult.getStatus().equals("succ"))
                return false;
            if(null != tdResult) {
                return tdResult.getRows() > 0 ? true : false;
            }
        }
        return false;
    }
    /*
     *  Create super table as template.
     */
    public String createSuperTable() throws Exception {
        final String createTable = String.format(TDengineSQLCmdList.getCreateSuperTable(), database, tablePrefix);
        return executeSQL(createTable);
    }

    public String createTable(String deviceId) throws Exception {
        String tableName = getTableNameFromDeviceId(deviceId);
        return createTable(tableName, deviceId);
    }

    public String createTable(String tableName, String deviceId) throws Exception {
        changeToCurrentDatabase();
        final String createTable = String.format(TDengineSQLCmdList.getCreateTable(), 
        database, tableName, database, tablePrefix, deviceId);
        return executeSQL(createTable);
    }

    public int loadDevices() throws Exception {
        final String allDevices = String.format(TDengineSQLCmdList.getGetSubTable(), database, tablePrefix);
        String result = executeSQL(allDevices);
        deviceMap.clear();
        TDengineQueryResult tdResult = TDengineQueryResult.GetResult(result);
        if(null == tdResult)
            return 0;
        if(tdResult.getRows() > 0) {
            for(int i = 0; i < tdResult.getRows(); i++) {
                String deviceId = tdResult.getData()[i][0];
                String tableName = tdResult.getData()[i][1];
                deviceMap.put(deviceId, tableName);
            }
        }

        return deviceMap.size();
    }

    /*
     * Add Authorization in Header, put SQL statements into POST body.
     */
    public String executeSQL(final String sqlStatement) throws Exception {

        if (this.getAuthToken().equals("") || sqlStatement.equals("")) {
            log.warn("Access token is empty. Any actions will be failed!");
            return "";
        }

        final URI uri = new URIBuilder().setScheme("http").setHost(serverHost + ":" + String.valueOf(serverPort))
                .setPath("/rest/sql").build();
        final CloseableHttpClient client = HttpClients.custom().setDefaultRequestConfig(defaultRequestConfig).build();
        // Post Url
        final HttpPost thePost = new HttpPost(uri);
        // Headers
        thePost.addHeader("Accept", "application/json");
        thePost.addHeader("Authorization", this.accessToken);
        // Body
        thePost.setEntity(new StringEntity(sqlStatement, defaultEncode));
        try {
            // Send request.
            final CloseableHttpResponse response = client.execute(thePost);
            // Retrieve status code from response line.
            final int statusCode = response.getStatusLine().getStatusCode();
            final HttpEntity entity = response.getEntity();
            final String result = EntityUtils.toString(entity, defaultEncode);
            // log.info("Execute SQL statement: {} Result: {}", sqlStatement, result);
            if (200 == statusCode) {
                return result;
            }
        } catch (final Exception ex) {
            return "";
        } finally {
            client.close();
        }
        return "";
    }

    /*
     * Add Authorization in Header, put SQL statements into POST body.
     */
    public void executeSQLAsync(final String sqlStatement) throws Exception {

        if (this.getAuthToken().equals("") || sqlStatement.equals(""))
            return;

        final URI uri = new URIBuilder().setScheme("http").setHost(serverHost + ":" + String.valueOf(serverPort))
                .setPath("/rest/sql").build();
        final CloseableHttpAsyncClient client = HttpAsyncClients.custom().setDefaultRequestConfig(defaultRequestConfig)
                .build();
        client.start();
        // Post Url
        final HttpPost thePost = new HttpPost(uri);
        // Headers
        thePost.addHeader("Accept", "application/json");
        thePost.addHeader("Authorization", this.accessToken);
        // Body
        thePost.setEntity(new StringEntity(sqlStatement, defaultEncode));
        try {
            // Send request async.
            client.execute(thePost, new HttpAsyncResponseImpl(client, sqlStatement));
        } catch (final Exception ex) {
            log.error("Async HTTP request error: ", ex);
            return;
        }
    }


    class HttpAsyncResponseImpl implements FutureCallback<HttpResponse> {

        private final String sqlStatement;
        private CloseableHttpAsyncClient requestClient = null;

        public HttpAsyncResponseImpl(CloseableHttpAsyncClient client, final String sqlStatement) {
            this.sqlStatement = sqlStatement;
            requestClient = client;
        }

        @Override
        public void completed(HttpResponse response) {

            try {
                // Retrieve status code from response line.
                // final int statusCode = response.getStatusLine().getStatusCode();
                final HttpEntity entity = response.getEntity();
                final String result = EntityUtils.toString(entity, defaultEncode);
                log.info("Execute SQL statement: {} Result: {}", sqlStatement, result);
                // Close request object.
                requestClient.close();
                HttpClientUtils.closeQuietly(response);
            } catch (Exception e) {
                log.error("Async execute SQL statement: {} got error: {}", sqlStatement, e);
            }
        }

        @Override
        public void cancelled() {

            try {
                requestClient.close();
                log.info("SQL Statement: " + sqlStatement + " has been cancelled.");
            } catch(Exception e) {
                log.error("Async http client close error: {}", e.getMessage(), e);
            }
        }

        @Override
        public void failed(final Exception e) {

            try {
                requestClient.close();
                log.info("SQL Statement: " + sqlStatement + " has been failed.");
                log.error("Asyn http resquest failed: {}", e.getMessage(), e);
            } catch(Exception ex) {
                log.error("Async http client close error: {}", e.getMessage(), ex);
            }
        }

    }

    public String getTableNameFromDeviceId(String deviceId) {
        return deviceId.replace('+', '_')
        .replace('-', '_')
        .replace('/', '_')
        .replace(':', '_')
        .replace('|', '_');

        // It seems below code is slower than above.
        // byte[] array = deviceId.getBytes();
        // int len = array.length;
        // if(len > 64)
        //     len = 64; // the max length of table name is 64.
        // byte[] newArr = new byte[len];
        // for(int i = 0; i < len; i++) {
        //     if((32 <= array[i] && 47 >= array[i]) || 
        //     (58 <= array[i] && 64 >= array[i]) || 
        //     (91 <= array[i] && 96 >= array[i])) {
        //         newArr[i] = 95;
        //     }
        //     else {
        //         newArr[i] = array[i];
        //     }
        // }
        // return new String(newArr);
    }

}

package com.github.micli.catfish;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import ch.qos.logback.core.util.Duration;

import java.util.Base64;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.alibaba.fastjson.JSON;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public final class TDengineHttpClientIT {

    private String host = "40.73.33.53";
    private int port = 6041;
    private String user = "test";
    private String pwd = "123456";
    private int timeout = 3000;
    private String database = "testdb";
    private String prefix = "mqtt_msg";

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    void test_auth_server() throws Exception {

        TDengineHttpClient client = new TDengineHttpClient(host, port, user, pwd, timeout, database, prefix);
        assertTrue(!client.getAuthToken().equals(""));
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    void test_sql_execute_remotely() throws Exception {
        TDengineHttpClient client = new TDengineHttpClient(host, port, user, pwd, timeout, database, prefix);
        String result = client.executeSQL("SELECT * FROM abc;");
        assertTrue(!result.equals(""));
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    void test_sql_create_table() throws Exception {
        TDengineHttpClient client = new TDengineHttpClient(host, port, user, pwd, timeout, database, prefix);
        String result = client.createSuperTable();
        result = client.executeSQL("select * from mqtt_msg;");
        assertTrue(!result.equals(""));
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    void test_sql_execute_async() throws Exception {
        TDengineHttpClient client = new TDengineHttpClient(host, port, user, pwd, timeout, database, prefix);
        // Date date = new Date();  
        // DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");  
        // String timeString = dateFormat.format(date);  
        String sql = "SELECT TBNAME FROM testdb.mqtt_msg;";
        client.executeSQLAsync(sql);
        assertTrue(true); 
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    void test_create_database() throws Exception {
        TDengineHttpClient client = new TDengineHttpClient(host, port, user, pwd, timeout, database, prefix);
        String result = client.createDatabase();
        assertTrue(result.length() > 0); 
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    void test_create_supertable() throws Exception {
        TDengineHttpClient client = new TDengineHttpClient(host, port, user, pwd, timeout, database, prefix);
        String result = client.createSuperTable();
        assertTrue(result.length() > 0); 
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    void test_create_table() throws Exception {
        TDengineHttpClient client = new TDengineHttpClient(host, port, user, pwd, timeout, database, prefix);
        String result = client.createTable("device-1");
        result = client.createTable("device-2");
        result = client.createTable("device-3"); 
        assertTrue(result.length() > 0); 
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    void test_load_device() throws Exception {
        TDengineHttpClient client = new TDengineHttpClient(host, port, user, pwd, timeout, database, prefix);
        int count = client.loadDevices();
        assertTrue(count > 0); 
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    void test_write_data() throws Exception {
        TDengineHttpClient client = new TDengineHttpClient(host, port, user, pwd, timeout, database, prefix);

        String sqlCount = String.format(TDengineSQLCmdList.getGetRowCount(), database, "device_1");
        String resultString = client.executeSQL(sqlCount);
        TDengineQueryResult result1 = JSON.parseObject(resultString, TDengineQueryResult.class);
        // Row count before insert.
        int count1 = Integer.parseInt(result1.getData()[0][0]);
        client.changeToCurrentDatabase();
        client.WriteData("msgid", "device-1", "topic", 1, "testdata.");
        resultString = client.executeSQL(sqlCount);
        TDengineQueryResult result2 = JSON.parseObject(resultString, TDengineQueryResult.class);
        // Row count after insert.
        int count2 = Integer.parseInt(result2.getData()[0][0]);
        assertTrue(count2 > count1);
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    void test_base64_encode() throws Exception {
        byte[] bytes = new byte[]{3, 12, 24, 57, 22, 63, 45, 22, 77};
        ByteBuffer test = ByteBuffer.wrap(bytes);
        String result = Base64.getEncoder().encodeToString(test.array());
        assertTrue(result.length() > 0);
    }   
    
    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    void test_getTableNameFromDeviceId() throws Exception {
        TDengineHttpClient client = new TDengineHttpClient(host, port, user, pwd, timeout, database, prefix);

        String testString = "test-device/1-2/@\"building-1\"";
        Instant start = Instant.now();
        String tablename = client.getTableNameFromDeviceId(testString);
        Instant end = Instant.now();
        long value = ChronoUnit.MICROS.between(start, end);
        assertTrue(tablename.equals("test_device_1_2___building_1_"));
    }  
}

package com.github.micli.catfish;

// import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
// import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
// import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
// import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
// import com.hivemq.extension.sdk.api.annotations.NotNull;
// import com.hivemq.testcontainer.core.MavenHiveMQExtensionSupplier;
// import com.hivemq.testcontainer.junit5.HiveMQTestContainerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
// import org.junit.jupiter.api.extension.RegisterExtension;
// import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

// import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// import com.github.micli.catfish.TDengineHttpClient;

import java.io.*;

public final class TDengineHttpClientIT {

    private String host = "40.73.33.53";
    private int port = 6041;
    private String user = "test";
    private String pwd = "123456";
    private int timeout = 3000;
    private String database = "testdb";
    private String prefix = "testmqtt";

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
        String result = client.CreateSuperTable();
        result = client.executeSQL("select * from mqtt_msg;");
        assertTrue(!result.equals("")); 
    }
}
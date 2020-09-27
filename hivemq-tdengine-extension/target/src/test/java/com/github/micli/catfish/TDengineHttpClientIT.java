
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

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    void test_auth_server() throws Exception {
        
        TDengineHttpClient client = new TDengineHttpClient("40.73.33.53", 6041, "test", "123456", 3000, "testdb", "testmqtt");
        assertTrue(client.getAuthToken().equals("")); 
    }
    
    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    void test_sql_execute_remotely() throws Exception {
        TDengineHttpClient client = new TDengineHttpClient("40.73.33.53", 6041, "test", "123456", 3000, "testdb", "testmqtt");
        client.executeSQL("SELECT * FROM abc;");
        assertTrue(client.getAuthToken().equals("")); 
    }
}
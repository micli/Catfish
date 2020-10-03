
/*
 * Copyright 2018-present HiveMQ GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.micli.catfish;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.client.parameter.ClientInformation;
import com.hivemq.extension.sdk.api.interceptor.publish.PublishInboundInterceptor;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishInboundInput;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishInboundOutput;
import com.hivemq.extension.sdk.api.packets.publish.ModifiablePublishPacket;

import java.util.Base64;
import java.util.Optional;
import java.nio.ByteBuffer;

/**
 * This is a very simple {@link PublishInboundInterceptor}, it changes the
 * payload of every incoming PUBLISH with the topic 'hello/world' to 'Hello
 * World!'.
 *
 * @author Yannick Weber
 * @since 4.3.1
 */
public class TDengineInterceptor implements PublishInboundInterceptor {

    final private Base64.Encoder encoder = Base64.getEncoder();

    @Override
    public void onInboundPublish(final @NotNull PublishInboundInput publishInboundInput,
            final @NotNull PublishInboundOutput publishInboundOutput) {
        final ModifiablePublishPacket publishPacket = publishInboundOutput.getPublishPacket();
        final ClientInformation clientInformation = publishInboundInput.getClientInformation();

        try {
            String msgid = String.valueOf(publishPacket.getPacketId());
            String topic = publishPacket.getTopic();
            String deviceId = clientInformation.getClientId();
            Optional<ByteBuffer> buff = publishPacket.getPayload();
            String payLoad = "";
            byte[] bytes = buff.get().array().clone();
            payLoad = encoder.encodeToString(bytes);
            // String payLoad = encoder.encodeToString(buff.get().array().clone());
            int qos = publishPacket.getQos().getQosNumber();
            // Save data to TDengine.
            TDengineMain.client.WriteData(msgid, deviceId, topic, qos, payLoad);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }

}
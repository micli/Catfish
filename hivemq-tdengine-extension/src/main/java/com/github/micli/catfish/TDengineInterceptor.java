
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

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.client.parameter.ClientInformation;
import com.hivemq.extension.sdk.api.interceptor.publish.PublishInboundInterceptor;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishInboundInput;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishInboundOutput;
import com.hivemq.extension.sdk.api.packets.publish.ModifiablePublishPacket;

import java.util.Base64;
import java.nio.ByteBuffer;

/**
 * This is a very simple {@link PublishInboundInterceptor}, 
 * it retrieve every mqtt message 
 * and save data to TDengine by REST API.
 *
 * @author Michael Li
 * @since 0.1 
 */
public class TDengineInterceptor implements PublishInboundInterceptor {

    final private Base64.Encoder encoder = Base64.getEncoder();

    @Override
    public void onInboundPublish(final @NotNull PublishInboundInput publishInboundInput,
            final @NotNull PublishInboundOutput publishInboundOutput) {
        final ModifiablePublishPacket publishPacket = publishInboundOutput.getPublishPacket();
        final ClientInformation clientInformation = publishInboundInput.getClientInformation();
        if(!publishPacket.getPayload().isPresent())
                return;
        
        try {
            String msgid = String.valueOf(publishPacket.getPacketId());
            String topic = publishPacket.getTopic();
            String deviceId = clientInformation.getClientId();
            ByteBuffer buff = publishPacket.getPayload().get();
            String payLoad = "";
            byte[] bytes = new byte[buff.remaining()];
            buff.get(bytes);
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

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
import com.hivemq.extension.sdk.api.async.Async;
import com.hivemq.extension.sdk.api.async.TimeoutFallback;
import com.hivemq.extension.sdk.api.client.parameter.ClientInformation;
import com.hivemq.extension.sdk.api.interceptor.publish.PublishInboundInterceptor;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishInboundInput;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishInboundOutput;
import com.hivemq.extension.sdk.api.packets.publish.ModifiablePublishPacket;
import com.hivemq.extension.sdk.api.services.Services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.nio.ByteBuffer;
import java.time.Duration;

/**
 * This is a very simple {@link PublishInboundInterceptor}, 
 * it retrieve every mqtt message with async action
 * and save data to TDengine by REST API.
 * reference document: https://www.hivemq.com/docs/hivemq/4.4/extensions/interceptors.html#publish-inbound-modify-async
 * @author Michael Li
 * @since 0.1 
 */
public class TDengineInterceptor implements PublishInboundInterceptor {

    private static final Logger log = LoggerFactory.getLogger(TDengineHttpClient.class);
    final private Base64.Encoder encoder = Base64.getEncoder();

    @Override
    public void onInboundPublish(final @NotNull PublishInboundInput publishInboundInput,
            final @NotNull PublishInboundOutput publishInboundOutput) {
        final ModifiablePublishPacket publishPacket = publishInboundOutput.getPublishPacket();
        if(!publishPacket.getPayload().isPresent())
            return; // Ignore no data calls.
        
        final Async<PublishInboundOutput> outputAsyncResult =
            publishInboundOutput.async(Duration.ofSeconds(30), TimeoutFallback.FAILURE); // Default timeout is 30 secs.

            final CompletableFuture<?> task = Services.extensionExecutorService().submit(() -> {

                final ClientInformation clientInformation = publishInboundInput.getClientInformation();
                try {
                    String msgid = String.valueOf(publishPacket.getPacketId());
                    String topic = publishPacket.getTopic();
                    String deviceId = clientInformation.getClientId();
                    ByteBuffer buff = publishPacket.getPayload().get();
                    byte[] bytes = new byte[buff.remaining()];
                    buff.get(bytes);
                    String payLoad = encoder.encodeToString(bytes);
                    int qos = publishPacket.getQos().getQosNumber();
                    // Save data to TDengine.
                    TDengineMain.client.WriteData(msgid, deviceId, topic, qos, payLoad);
                } catch (Exception e) {
                    log.error("Error occured onInboundPublish: ", e);
                }
            });
        // Completion callback.
        task.whenComplete((ignored, throwable) -> {
            if (null != throwable) {
                log.error("Error occured during async action: ", throwable);
            }
            outputAsyncResult.resume();
        });        
    }
}
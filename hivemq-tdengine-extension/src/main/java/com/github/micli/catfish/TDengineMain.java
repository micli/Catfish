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

import java.io.File;

import com.github.micli.catfish.configuration.TDengineConfiguration;
import com.hivemq.extension.sdk.api.ExtensionMain;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.events.EventRegistry;
import com.hivemq.extension.sdk.api.parameter.*;
import com.hivemq.extension.sdk.api.services.Services;
import com.hivemq.extension.sdk.api.services.intializer.InitializerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;

/**
 * This is the main class of the extension,
 * which is instantiated either during the HiveMQ start up process (if extension is enabled)
 * or when HiveMQ is already started by enabling the extension.
 *
 * @author Florian Limpöck
 * @since 4.0.0
 */
public class TDengineMain implements ExtensionMain {

    private static final @NotNull Logger log = LoggerFactory.getLogger(TDengineMain.class);

    @Getter
    public static TDengineHttpClient client = null;

    @Override
    public void extensionStart(final @NotNull ExtensionStartInput extensionStartInput,
            final @NotNull ExtensionStartOutput extensionStartOutput) {

        try {

            final File extensionHomeFolder = extensionStartInput.getExtensionInformation().getExtensionHomeFolder();
            final TDengineConfiguration configuration = new TDengineConfiguration(extensionHomeFolder);

            if (!configuration.readPropertiesFromFile()) {
                extensionStartOutput.preventExtensionStartup("Could not read TDengine properties");
                return;
            }

            if (!configuration.validateConfiguration()) {
                extensionStartOutput.preventExtensionStartup("At least one mandatory property not set");
                return;
            }

            client = setupTDengineClient(configuration);
            if(null == client) {
                log.error("TDengineHttpClient setup failed.");
            }

            addClientLifecycleEventListener();
            addPublishModifier();

            final ExtensionInformation extensionInformation = extensionStartInput.getExtensionInformation();
            log.info("Started " + extensionInformation.getName() + ":" + extensionInformation.getVersion());

        } catch (Exception e) {
            log.error("Exception thrown at extension start: ", e);
        }

    }

    @Override
    public void extensionStop(final @NotNull ExtensionStopInput extensionStopInput, final @NotNull ExtensionStopOutput extensionStopOutput) {

        final ExtensionInformation extensionInformation = extensionStopInput.getExtensionInformation();
        log.info("Stopped " + extensionInformation.getName() + ":" + extensionInformation.getVersion());

    }

    private void addClientLifecycleEventListener() {

        final EventRegistry eventRegistry = Services.eventRegistry();

        final TDengineListener TDengineListener = new TDengineListener();

        eventRegistry.setClientLifecycleEventListener(input -> TDengineListener);

    }

    private void addPublishModifier() {
        final InitializerRegistry initializerRegistry = Services.initializerRegistry();

        final TDengineInterceptor TDengineInterceptor = new TDengineInterceptor();

        initializerRegistry.setClientInitializer((initializerInput, clientContext) -> clientContext.addPublishInboundInterceptor(TDengineInterceptor));
    }

    private TDengineHttpClient setupTDengineClient(TDengineConfiguration config) throws Exception {

        String host = config.getHost();
        int port = config.getPort();
        String username = config.getUsername();
        String password = config.getPassword();
        int connectTimeout = config.getConnectTimeout();
        String database = config.getDatabase();
        String tablePrefix = config.getPrefix();

        TDengineHttpClient client = null;
        try {
            client = new TDengineHttpClient(host, port, username, password, connectTimeout, database, tablePrefix);
        } catch (Exception e) {
            log.error("Initialize TDengineHttpClient failed.", e);
        }
        return client;
    }

}

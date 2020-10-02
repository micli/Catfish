
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

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

package com.github.micli.catfish.configuration;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Reads a property file containing TDengine properties
 * and provides some utility methods for working with {@link Properties}.
 *
 * @author Christoph Schäbel
 * @author Michael Walter
 */
public class TDengineConfiguration extends PropertiesReader {

    private static final Logger log = LoggerFactory.getLogger(TDengineConfiguration.class);

    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String REPORTING_INTERVAL = "reportingInterval";
    private static final int REPORTING_INTERVAL_DEFAULT = 1;
    private static final String PREFIX = "prefix";
    private static final String PREFIX_DEFAULT = "mqtt_msg";
    private static final String DATABASE = "database";
    private static final String DATABASE_DEFAULT = "testdb";
    private static final String DATABASE_USER = "user";
    private static final String DATABASE_USER_DEFAULT = "root";   
    private static final String DATABASE_PASSWORD = "password";
    private static final String DATABASE_PASSWORD_DEFAULT = "taosdata";  
    private static final String CONNECT_TIMEOUT = "connectTimeout";
    private static final int CONNECT_TIMEOUT_DEFAULT = 5000;

    public TDengineConfiguration(@NotNull final File configFilePath) {
        super(configFilePath);
    }

    /**
     * Check if mandatory properties exist and are valid. Mandatory properties are port and host.
     *
     * @return <b>true</b> if all mandatory properties exist, else <b>false</b>.
     */
    public boolean validateConfiguration() {
        int countError = 0;

        countError += checkMandatoryProperty(HOST);
        countError += checkMandatoryProperty(PORT);
        countError += checkMandatoryProperty(DATABASE);
        countError += checkMandatoryProperty(PREFIX);

        if (countError != 0){
            return false;
        }

        // check for valid port value
        final String port = getProperty(PORT);
        try {
            final int intPort = Integer.parseInt(port);

            if (intPort < 0 || intPort > 65535) {
                log.error("Value for mandatory TDengine property {} is not in valid port range.", PORT);
                countError++;
            }

        } catch (NumberFormatException e) {
            log.error("Value for mandatory TDengine property {} is not a number.", PORT);
            countError++;
        }

        // check if host is still --INFLUX-DB-IP--
        final String host = getProperty(HOST);

        if (host.equals("--INFLUX-DB-IP--")) {
            countError++;
        }

        return countError == 0;
    }

    /**
     * Check if mandatory property exists.
     *
     * @param property Property to check.
     * @return 0 if property exists, else 1.
     */
    private int checkMandatoryProperty(@NotNull final String property) {
        checkNotNull(property, "Mandatory property must not be null");

        final String value = getProperty(property);

        if (value == null) {
            log.error("Mandatory property {} is not set.", property);
            return 1;
        }
        return 0;
    }


    @Nullable
    public String getHost() {
        return getProperty(HOST);
    }

    @NotNull
    public String getDatabase() {
        return validateStringProperty(DATABASE, DATABASE_DEFAULT);
    }

    @Nullable
    public Integer getPort() {

        final Integer port;

        try {
            port = Integer.parseInt(getProperty(PORT));
        } catch (NumberFormatException e) {
            log.error("Value for {} is not a number", PORT);
            return null;
        }

        return port;
    }

    public int getReportingInterval() {
        return validateIntProperty(REPORTING_INTERVAL, REPORTING_INTERVAL_DEFAULT, false, false);
    }

    public int getConnectTimeout() {
        return validateIntProperty(CONNECT_TIMEOUT, CONNECT_TIMEOUT_DEFAULT, false, false);
    }

    @NotNull
    public String getPrefix() {
        return validateStringProperty(PREFIX, PREFIX_DEFAULT);
    }

    @Nullable
    public String getUsername() {
        return validateStringProperty(DATABASE_USER, DATABASE_USER_DEFAULT);
    }

    @Nullable
    public String getPassword() {
        return validateStringProperty(DATABASE_PASSWORD, DATABASE_PASSWORD_DEFAULT);
    }

    @Override
    public String getFilename() {
        return "TDengine.properties";
    }

    /**
     * Fetch property with given <b>key</b>. If the fetched {@link String} is <b>null</b> the <b>defaultValue</b> will be returned.
     *
     * @param key          Key of the property.
     * @param defaultValue Default value as fallback, if property has no value.
     * @return the actual value of the property if it is set, else the <b>defaultValue</b>.
     */
    private String validateStringProperty(@NotNull final String key, @NotNull final String defaultValue) {
        checkNotNull(key, "Key to fetch property must not be null");
        checkNotNull(defaultValue, "Default value for property must not be null");

        final String value = getProperty(key);

        if (value == null) {

            if (!defaultValue.isEmpty()) {
                log.warn("No '{}' configured for TDengine, using default: {}", key, defaultValue);
            }
            return defaultValue;
        }

        return value;
    }

    /**
     * Fetch property with given <b>key</b>.
     * If the fetched {@link String} value is not <b>null</b> convert the value to an int and check validation constraints if given flags are <b>false</b> before returning the value.
     *
     * @param key             Key of the property
     * @param defaultValue    Default value as fallback, if property has no value
     * @param zeroAllowed     use <b>true</b> if property can be zero
     * @param negativeAllowed use <b>true</b> is property can be negative int
     * @return the actual value of the property if it is set and valid, else the <b>defaultValue</b>
     */
    private int validateIntProperty(@NotNull final String key, final int defaultValue, final boolean zeroAllowed, final boolean negativeAllowed) {
        checkNotNull(key, "Key to fetch property must not be null");

        final String value = properties.getProperty(key);

        if (value == null) {
            log.warn("No '{}' configured for TDengine, using default: {}", key, defaultValue);
            return defaultValue;
        }

        int valueAsInt;

        try {
            valueAsInt = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            log.warn("Value for TDengine property '{}' is not a number, original value {}. Using default: {}", key, value, defaultValue);
            return defaultValue;
        }

        if (!zeroAllowed && valueAsInt == 0) {
            log.warn("Value for TDengine property '{}' can't be zero. Using default: {}", key, defaultValue);
            return defaultValue;
        }

        if (!negativeAllowed && valueAsInt < 0) {
            log.warn("Value for TDengine property '{}' can't be negative. Using default: {}", key, defaultValue);
            return defaultValue;
        }

        return valueAsInt;
    }
}
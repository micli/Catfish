![catfish logo](./images/catfish-logo.png)

# TDengine Extension of HiveMQ (Codename: Catfish)

This is a HiveMQ extension that supports data save to TDEngine.

For Chinese user, please read [中文版本](index_cn.md).

Content:

+ [Design document](design.md)
+ [Testing Report](testing.md)

## Deployment

1. Please download hivemq-tdengine-extension-1.0-distribution.zip from releases.
2. Unzip the file: hivemq-tdengine-extension-1.0-distribution.zip on local folder.
3. Put the whole folder into {HIVEMQHOME}/extensions/ as below:
![extensions folder layout](./images/extension-folder.png)
4. Create a configuration file named "TDengine.properties" under hivemq-tdengine-extension. The template as below:

TDengine.properties
```shell
host: {TDengine Server IP/hostname}
port:6041
user: {TDengine account}
password:{password}

prefix:mqtt_msg
database:testdb

reportingInterval:1
connectTimeout:5000
```
5. start HiveMQ service by {HIVEMQHOME}/bin/run.sh

Notes:
****
It doesen't need to create any database or tables in TDengine service. Just specify database name and table name by database and prefix properties in TDengine.properties. These database objects will created automatically.
****

## Enable Firewall Rules

The TDengine extension needs to access TDengine REST APIs which is locate on TDengine services with port 6041. Please configure firewall to enable outbound rule to communicate to 6041.

By default, the HiveMQ service exposes 1883 to recevied MQTT traffic, please enable it either.

## Tuning I/O Performance

There is one way to extend I/O performance:

+ Extends REST APIs serve threads number.

### Extends REST APIs serve threads number

The TDengine extension uses REST APIs to communicate to TDengine service. By default, there are only 2 threads serve HTTP traffic. If you have a heavy MQTT traffic, please add the number of httMaxThreads.

![taos.cfg](./images/tsos.cfg.png)

For details, please review:
[TDengine Documentation](https://www.taosdata.com/en/documentation/administrator/#Configuration-on-Server)

## Build from source

Build this project is very simple. It only needs OpenJDK 11 and Maven.

For install OpenJDK 11, please check https://openjdk.java.net/install/.

For install Maven, please check https://maven.apache.org/install.html.

Then you can build code directly.

```shell

# Get code from Github.
> git clone https://www.github.com/micli/Catfish.git
# change current folder.
> cd Catfish/hivemq-tdengine-extension/
# Build extension
> mvn package

```

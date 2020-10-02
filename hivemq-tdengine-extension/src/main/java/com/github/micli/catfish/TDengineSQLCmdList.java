package com.github.micli.catfish;

import lombok.*;

public class TDengineSQLCmdList {
    
    @Getter 
    private static final String createDb = "CREATE DATABASE IF NOT EXISTS %s ;";

    @Getter
    private static final String useDb = "USE %s ;";

    @Getter
    private static final String createSuperTable = "CREATE TABLE IF NOT EXISTS %s.%s (ts timestamp, msgid NCHAR(64), topic NCHAR(255), qos TINYINT, payload BINARY(4096)) TAGS (deviceId NCHAR(128));";

    @Getter
    private static final String createTable = "CREATE TABLE IF NOT EXISTS %s.%s USING %s.%s TAGS (\"%s\");";

    @Getter
    private static final String descTable = "DESCRIBE %s;";

    @Getter
    private static final String getSubTable = "SELECT deviceId, TBNAME FROM %s.%s;";

    @Getter
    private static final String getInsert = "INSERT INTO %s.%s VALUES ('%s', '%s', '%s', %d, '%s');";

    @Getter
    private static final String getRowCount = "SELECT COUNT(*) FROM %s.%s;";
}

package com.github.micli.catfish;

import lombok.*;
import com.alibaba.fastjson.JSON;


public class TDengineQueryResult {

    @Getter @Setter private String status;

    @Getter @Setter private String[] head;

    @Getter @Setter private String[][] data;

    @Getter @Setter private int rows;

    public static TDengineQueryResult GetResult(String resultString) throws Exception {
        if(resultString.isEmpty())
            return null;
        final TDengineQueryResult theResult = JSON.parseObject(resultString, TDengineQueryResult.class);
        return theResult;
    }

}

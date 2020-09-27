package com.github.micli.catfish;

import lombok.*;


public class TDengineQueryResult {

    @Getter @Setter private String status;

    @Getter @Setter String[] head;

    @Getter @Setter Object[] data;

    @Getter @Setter private int rows;

}

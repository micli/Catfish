

package com.github.micli.catfish;

import lombok.*;

public class TDengineAuthResult {

    @Getter @Setter private String status = "";
    @Getter @Setter private String code = "";   
    @Getter @Setter private String desc = "";

    // public String getStatus(){
    //     return this.status;
    // }
    // public void setStatus(String statusValue){
    //     this.status = statusValue;
    // }
    // public String getCode(){
    //     return this.code;
    // }
    // public void setCode(String codeValue){
    //     this.code = codeValue;
    // }
    // public String getDesc(){
    //     return this.desc;
    // }
    // public void setDesc(String descValue){
    //     this.desc = descValue;
    // }

    public TDengineAuthResult(String status, String code, String desc) {

        this.status = status;
        this.code = code;
        this.desc = desc;
    }

}
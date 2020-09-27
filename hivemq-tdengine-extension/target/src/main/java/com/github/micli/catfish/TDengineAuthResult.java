

package com.github.micli.catfish;


public class TDengineAuthResult {

    private String status = "";
    private String code = "";   
    private String desc = "";

    public String getStatus(){
        return this.status;
    }
    public void setStatus(String statusValue){
        this.status = statusValue;
    }
    public String getCode(){
        return this.code;
    }
    public void setCode(String codeValue){
        this.code = codeValue;
    }
    public String getDesc(){
        return this.desc;
    }
    public void setDesc(String descValue){
        this.desc = descValue;
    }

    public TDengineAuthResult(String status, String code, String desc) {

        this.status = status;
        this.code = code;
        this.desc = desc;
    }

}
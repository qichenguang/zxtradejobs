package com.qcg.zxtradejobs.util;

import org.apache.log4j.Logger;

public class SdkEntrustStatus {
    public String business_price;
    public String business_amount;
    public String entrust_date;
    public String entrust_no;
    public String entrust_status;
    private static Logger log = Logger.getLogger("SdkEntrustStatus.class");
    public void showMsg(){
        log.debug("business_price=" + business_price);
        log.debug("business_amount=" + business_amount);
        log.debug("entrust_date=" + entrust_date);
        log.debug("entrust_no=" + entrust_no);
        log.debug("entrust_status=" + entrust_status);
    }
}



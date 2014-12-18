package com.qcg.zxtradejobs.util;

import java.util.List;
import java.util.ArrayList;
import java.nio.ByteBuffer;
import org.apache.log4j.Logger;

public class SdkResult {
    public int error_code;
    public ByteBuffer error_msg;
    public String sh_account;
    public String sz_account;
    public String current_balance;
    public String entrust_no;
    public List<SdkEntrustStatus> entrust_status = new ArrayList<SdkEntrustStatus>();
    private static Logger log = Logger.getLogger("SdkResult.class");
    public void showMsg(){
        log.debug("error_code=" + error_code);
        // 构建一个byte数组
        byte [] content = new byte[error_msg.limit()];
        // 从ByteBuffer中读取数据到byte数组中
        error_msg.get(content);
        // 把byte数组的内容写到标准输出
        log.debug("error_msg=" + new String(content));
        log.debug("sh_account=" + sh_account);
        log.debug("sz_account=" + sz_account);
        log.debug("current_balance=" + current_balance);
        log.debug("entrust_no=" + entrust_no);
        int pos=0;
        for(SdkEntrustStatus item : entrust_status){
            log.debug("pos=" + pos);
            item.showMsg();
            pos++;
        }
    }
}




package com.qcg.zxtradejobs.jobStatus;

//fourinone
import com.fourinone.Contractor;
import com.fourinone.WareHouse;
import com.fourinone.WorkerLocal;
//beanstalk
import com.surftools.BeanstalkClient.BeanstalkException;
import com.surftools.BeanstalkClient.Client;
import com.surftools.BeanstalkClient.Job;
import com.surftools.BeanstalkClientImpl.*;
//redis
import net.sf.json.JSONException;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import redis.clients.jedis.Jedis;
//json
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

//comm
import java.util.*;
//
import com.qcg.zxtradejobs.util.*;

public class JobStatus extends JobWorker{
    //log4j
    private static Logger log = Logger.getLogger("JobSell.class");
    //Ctor
    public JobStatus(){
    }
    public JobStatus(String work_name) {
        super(work_name);
    }
    public JobStatus(String work_name,ConfigParm config_parm){
        super(work_name,config_parm);
    }
    public JobStatus(String work_name,
                    String beanstalk_server_ip, int beanstalk_server_port,
                    String redis_server_ip,int redis_server_port,
                    String thrift_server_ip,int thrift_server_port,int thrift_server_timeout){
        super(work_name,
                beanstalk_server_ip,beanstalk_server_port,
                redis_server_ip,redis_server_port,
                thrift_server_ip,thrift_server_port,thrift_server_timeout);
    }

    public int do_job_work(Client beanstalk_client,Job job,
                           String src_tube_name,String dest_tube_name,String error_tube_name){
        int ret = OK;
        ZxTradeSdkHelp help = new ZxTradeSdkHelp();
        SdkResult result = null;
        JobItemDetail job_item = new JobItemDetail();
        try {
            //
            //1.parse json job string to beans.
            String jobstring = new String(job.getData());
            JSONObject job_obj = JSONObject.fromObject(jobstring);
            job_item = (JobItemDetail)JSONObject.toBean(job_obj, JobItemDetail.class);
            job_item.job_msg_id = job.getJobId();
            result = help.serach_entrust_status(job_item.account, job_item.entrust_no);
            if(result.error_code == 0) {
                for (SdkEntrustStatus item : result.entrust_status) {
                    //public String business_price;
                    //public String business_amount;
                    //public String entrust_date;
                    //public String entrust_no;
                    //public String entrust_status;
                    //
                    /**
                     *委托状态。
                     '0'	未报
                     '1'	待报
                     '2'	已报
                     '3'	已报待撤
                     '4'	部成待撤
                     '5'	部撤
                     '6'	已撤
                     '7'	部成
                     '8'	已成
                     '9'	废单
                     --撤单委托，只有entrust_status为2或7时才能撤单，不然会报“委托状态错误”
                     */
                    if(item.entrust_status.equals("8")){
                        if(job_item.buyorsell.equals("1")){
                            job_item.sj_buy_num = Double.valueOf(item.business_amount).intValue();;
                            job_item.sj_buy_price = Double.parseDouble(item.business_price);
                        }else{
                            job_item.sj_sell_num = Double.valueOf(item.business_amount).intValue();;
                            job_item.sj_sell_price = Double.parseDouble(item.business_price);
                        }
                        job_item.end_date = item.entrust_date;
                        job_item.err_num = 0;
                        ret = delet_old_and_put_new_job_to_next_tube(beanstalk_client,job_item,job,src_tube_name,dest_tube_name);
                    }else if(item.entrust_status.equals("6") || item.entrust_status.equals("9")){
                        log.debug(item.entrust_status + ":" + jobstring);
                        //1.delete old msg
                        beanstalk_client.useTube(src_tube_name);
                        boolean is_suc = beanstalk_client.delete(job.getJobId());
                    }else {
                        //2.release with delay 60 second.
                        beanstalk_client.useTube(src_tube_name);
                        boolean is_suc = beanstalk_client.release(job.getJobId(),1024,60);
                    }
                    sleep_timeout(1000L);
                }
            }else{
                job_item.error_code = result.error_code;
                byte [] content = new byte[result.error_msg.limit()];
                result.error_msg.get(content);
                job_item.error_msg = new String(content);
                add_job_error_num_and_put_to_error_queue(beanstalk_client,job_item,job,src_tube_name,error_tube_name);
            }

        } catch (BeanstalkException be) {
            log.debug(be.getMessage());
            return BEANSTALK_ERROR;
        } catch (Exception e) {
            log.debug(e.getMessage());
            return OTHER_ERROR;
        }
        return ret;
    }

    public static void main( String[] args ){
        //
        PropertyConfigurator.configure("E:\\project\\IDEA\\zxtradejobs\\Log4j.properties");
        //
        String beanstalk_server_ip = System.getProperty("beanstalk.server.ip");
        String beanstalk_server_port = System.getProperty("beanstalk.server.port");
        String redis_server_ip = System.getProperty("redis.server.ip");
        String redis_server_port = System.getProperty("redis.server.port");
        String thrift_server_ip = System.getProperty("thrift.server.ip");
        String thrift_server_port = System.getProperty("thrift.server.port");
        String thrift_server_timeout = System.getProperty("thrift.server.timeout");
        String worker_name = System.getProperty("worker.name");
        String worker_ip = System.getProperty("worker.ip");
        String worker_port = System.getProperty("worker.port");
        String b_ip = (beanstalk_server_ip != null) ? beanstalk_server_ip : "127.0.0.1";
        int b_port = (beanstalk_server_port != null) ? Integer.getInteger(beanstalk_server_port) : 11300;
        String r_ip = (redis_server_ip != null) ? redis_server_ip : "127.0.0.1";
        int r_port = (redis_server_port != null) ? Integer.getInteger(redis_server_port) : 6379;
        String t_ip = (thrift_server_ip != null) ? thrift_server_ip : "127.0.0.1";
        int t_port = (thrift_server_port != null) ? Integer.getInteger(thrift_server_port) : 9090;
        int t_timeout = (thrift_server_timeout != null) ? Integer.getInteger(thrift_server_timeout) : 30000;
        //
        String w_n = (worker_name != null) ? worker_name : "job-status1";
        String w_ip = (worker_ip != null) ? worker_ip : "127.0.0.1";

        int w_port = (worker_port != null) ? Integer.parseInt(worker_port) : 9001;
        log.debug("w_n=" + w_n);
        log.debug("w_ip=" + w_ip);
        log.debug("w_port=" + w_port);
        JobStatus jobstatus = new JobStatus(w_n);
        jobstatus.waitWorking(w_ip,w_port,"job-status");
        //

/*        WareHouse inhouse = new WareHouse();
        inhouse.setObj("recv_tube_name", "status");
        inhouse.setObj("send_tube_name", "result");
        inhouse.setObj("error_tube_name", "error");
        JobStatus jobstatus = new JobStatus();
        WareHouse outhouse = jobstatus.doTask(inhouse);
        log.debug(outhouse);*/
    }
}

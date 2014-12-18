package com.qcg.zxtradejobs.util;
//fourinone
import com.fourinone.Contractor;
import com.fourinone.MigrantWorker;
import com.fourinone.WareHouse;
import com.fourinone.WorkerLocal;
//beanstalk
import com.surftools.BeanstalkClient.BeanstalkException;
import com.surftools.BeanstalkClient.Client;
import com.surftools.BeanstalkClient.Job;
import com.surftools.BeanstalkClientImpl.*;
//redis
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import redis.clients.jedis.Jedis;
//json
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
//comm
import java.util.*;

//
public abstract class JobWorker extends MigrantWorker{
    //
    public static final int OK = 1;
    public static final int NOW_NOT_HAVE_JOB = 2;
    public static final int BEANSTALK_ERROR = 3;
    public static final int NO_WAITING_WORKERS_ERROR = 4;
    public static final int BEANSTALK_PUT_JOB_ERROR = 5;
    public static final int PARSER_JSON_ERROR = 6;
    public static final int OTHER_ERROR = 99;
    //
    public static final int JOB_STATUS_RUN = 2;
    public static final int JOB_STATUS_PAUSE = 3;
    public static final int JOB_STATUS_RESTART = 4;
    public static final int JOB_STATUS_STOP = 5;
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
     */

    private String work_name;

    protected ConfigParm config_parm = new ConfigParm();
    //log4j
    private static Logger log = Logger.getLogger("JobWorker.class");
    //Ctor
    public JobWorker(){
    }
    public JobWorker(String work_name) {
        this.work_name = work_name;
    }
    public JobWorker(String work_name,ConfigParm config_parm){
        this.work_name = work_name;
        this.config_parm = config_parm;
    }
    public JobWorker(String work_name,
                     String beanstalk_server_ip, int beanstalk_server_port,
                     String redis_server_ip,int redis_server_port,
                     String thrift_server_ip,int thrift_server_port,int thrift_server_timeout) {
        this.work_name = work_name;
        this.config_parm.beanstalk_server_ip = beanstalk_server_ip;
        this.config_parm.beanstalk_server_port = beanstalk_server_port;
        this.config_parm.redis_server_ip = redis_server_ip;
        this.config_parm.redis_server_port = redis_server_port;
        this.config_parm.thrift_server_ip = thrift_server_ip;
        this.config_parm.thrift_server_port = thrift_server_port;
        this.config_parm.thrift_server_timeout = thrift_server_timeout;
    }

    public void sleep_timeout(long micsec){
        try{
            Thread.sleep(micsec);
        } catch (Exception ex){
            log.debug(ex.getMessage());
        }
    }
    public int delet_old_and_put_new_job_to_next_tube(Client beanstalk_client,JobItemDetail job_item,
                                                      Job job,
                                                      String src_tube_name,
                                                      String dst_tube_name)
    {
        try {
            int err_num = 0;
            while(err_num < 3) {
                JSONObject jsonObject = JSONObject.fromObject(job_item);
                beanstalk_client.useTube(src_tube_name);
                //1.delete old msg
                boolean is_suc = beanstalk_client.delete(job.getJobId());
                //2.add new msg
                long jobId = 0;
                if(dst_tube_name != null){
                    beanstalk_client.useTube(dst_tube_name);
                    jobId = beanstalk_client.put(1024, 0, 120, (jsonObject.toString()).getBytes());
                }
                if(jobId > 0) {
                    break;
                }else{
                    err_num++;
                }
            }
            if(err_num >= 3){
                return BEANSTALK_PUT_JOB_ERROR;
            }
        } catch (BeanstalkException be) {
            log.debug(be.getMessage());
            return BEANSTALK_ERROR;
        } catch (Exception e) {
            log.debug(e.getMessage());
            return OTHER_ERROR;
        }
        return OK;
    }

    public int add_job_error_num_and_put_to_error_queue(Client beanstalk_client,JobItemDetail job_item,
                                                        Job job,
                                                        String src_tube_name,
                                                        String dst_tube_name)
    {
        try {
            job_item.err_num++;
            if(job_item.err_num > 3){
                //4.put it to error queue.
                delet_old_and_put_new_job_to_next_tube(beanstalk_client, job_item, job, src_tube_name, dst_tube_name);
            }else{
                //add err num,put it back to queue.
                delet_old_and_put_new_job_to_next_tube(beanstalk_client, job_item, job, src_tube_name, src_tube_name);
            }
        } catch (BeanstalkException be) {
            log.debug(be.getMessage());
            return BEANSTALK_ERROR;
        } catch (Exception e) {
            log.debug(e.getMessage());
            return OTHER_ERROR;
        }
        return OK;
    }

    public int do_buy_sell_comm_work(Client beanstalk_client,Job job,
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
            //1.first msg ,send
            String yj_do_num = null;
            if(job_item.buyorsell.equals("1")) {
                yj_do_num = Integer.toString(job_item.yj_buy_num);
            }else{
                yj_do_num = Integer.toString(job_item.yj_sell_num);
            }
            log.debug(yj_do_num);
            result = help.run_normal_entrust(job_item.account,
                    job_item.stock_code, job_item.exchange_type, yj_do_num,job_item.price,job_item.buyorsell);
            //2.put new a job msg to beanstalk queue.
            if(result.error_code == 0){
                //3.SUCCESS. put entrust_no to queue
                job_item.entrust_no = result.entrust_no;
                job_item.begin_sec = new Date().getTime()/1000;
                job_item.err_num = 0;
                ret = delet_old_and_put_new_job_to_next_tube(beanstalk_client,job_item,job,src_tube_name,dest_tube_name);
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
    public abstract int do_job_work(Client beanstalk_client,Job job,
                                     String src_tube_name,String dest_tube_name,String error_tube_name);
    public WareHouse doTask(WareHouse inhouse) {
        int ret = OK;
        try {
            //1.get parm : tube name
            String recv_tube_name = (String)inhouse.getObj("recv_tube_name");
            String send_tube_name = (String)inhouse.getObj("send_tube_name");
            String error_tube_name = (String)inhouse.getObj("error_tube_name");
            //1. get a job from jobs queue, tube name : jobs
            Client beanstalk_client = new ClientImpl(config_parm.beanstalk_server_ip,config_parm.beanstalk_server_port);
            beanstalk_client.watch(recv_tube_name);
            beanstalk_client.useTube(recv_tube_name);
            while(!isInterrupted()) {
                //get job with 2 sec timeout
                Job job = beanstalk_client.reserve(2);
                if (job == null) {
                    ret = NOW_NOT_HAVE_JOB;
                    break;
                }else{
                    ret = do_job_work(beanstalk_client,job,recv_tube_name,send_tube_name,error_tube_name);
                    log.debug("do_job_work:" + ret);
                }
            }
            //
            beanstalk_client.ignore(recv_tube_name);
            beanstalk_client.close();
            //
        } catch (BeanstalkException be) {
            ret = BEANSTALK_ERROR;
            log.debug(be.getMessage());
        } catch (Exception e) {
            ret = OTHER_ERROR;
            log.debug(e.getMessage());
        }
        //
        inhouse.setObj("worker_error_code",ret);
        return inhouse;
    }
}

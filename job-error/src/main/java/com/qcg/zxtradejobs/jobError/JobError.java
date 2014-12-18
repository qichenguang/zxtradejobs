package com.qcg.zxtradejobs.jobError;

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

public class JobError extends JobWorker{
    //log4j
    private static Logger log = Logger.getLogger("JobResult.class");
    //Ctor
    public JobError(){
    }
    public JobError(String work_name) {
        super(work_name);
    }
    public JobError(String work_name,ConfigParm config_parm){
        super(work_name,config_parm);
    }
    public JobError(String work_name,
                    String beanstalk_server_ip, int beanstalk_server_port,
                    String redis_server_ip,int redis_server_port,
                    String thrift_server_ip,int thrift_server_port,int thrift_server_timeout){
        super(work_name,
                beanstalk_server_ip,beanstalk_server_port,
                redis_server_ip,redis_server_port,
                thrift_server_ip,thrift_server_port,thrift_server_timeout);
    }
    public int do_error_msg(Client beanstalk_client,Job job,
                                   String src_tube_name,String dest_tube_name,String error_tube_name)
    {
        int ret = OK;
        JobItemDetail job_item = new JobItemDetail();
        try{
            //1.parse json job string to beans.
            String jobstring = new String(job.getData());
            JSONObject job_obj = JSONObject.fromObject(jobstring);
            job_item = (JobItemDetail)JSONObject.toBean(job_obj, JobItemDetail.class);
            //1.parse msg
            String buyorsell = job_item.buyorsell.equals("1") ? "buy" : "sell";
            String stock_code = job_item.stock_code;
            String pro_id = job_item.pro_id;
            String id = job_item.id;
            String entrust_no = job_item.entrust_no;
            //2.save to file
            log.fatal(jobstring);
            //5.delete cur msg
            beanstalk_client.useTube(src_tube_name);
            boolean is_suc = beanstalk_client.delete(job.getJobId());
        } catch (BeanstalkException be) {
            log.debug(be.getMessage());
            return BEANSTALK_ERROR;
        } catch (Exception e) {
            log.debug(e.getMessage());
            return OTHER_ERROR;
        }
        return ret;
    }
    public int do_job_work(Client beanstalk_client, Job job,
                           String src_tube_name,String dest_tube_name,String error_tube_name)
    {
        return do_error_msg(beanstalk_client, job, src_tube_name, dest_tube_name, error_tube_name);
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
/*        String w_n = (worker_name != null) ? worker_name : "job-error";
        String w_ip = (worker_ip != null) ? worker_ip : "127.0.0.1";
        int w_port = (worker_port != null) ? Integer.parseInt(worker_port) : 19007;
        log.debug("w_n=" + w_n);
        log.debug("w_ip=" + w_ip);
        log.debug("w_port=" + w_port);
        JobError joberror = new JobError(w_n);
        joberror.waitWorking(w_ip, w_port, "job-error");*/
        //

        WareHouse inhouse = new WareHouse();
        inhouse.setObj("recv_tube_name", "error");
        inhouse.setObj("send_tube_name", "");
        inhouse.setObj("error_tube_name", "");
        JobError joberror = new JobError();
        WareHouse outhouse = joberror.doTask(inhouse);
        log.debug(outhouse);
    }
}

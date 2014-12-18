package com.qcg.zxtradejobs.jobResult;

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

public class JobResult extends JobWorker{
    //log4j
    private static Logger log = Logger.getLogger("JobResult.class");
    //Ctor
    public JobResult(){
    }
    public JobResult(String work_name) {
        super(work_name);
    }
    public JobResult(String work_name,ConfigParm config_parm){
        super(work_name,config_parm);
    }
    public JobResult(String work_name,
                    String beanstalk_server_ip, int beanstalk_server_port,
                    String redis_server_ip,int redis_server_port,
                    String thrift_server_ip,int thrift_server_port,int thrift_server_timeout){
        super(work_name,
                beanstalk_server_ip,beanstalk_server_port,
                redis_server_ip,redis_server_port,
                thrift_server_ip,thrift_server_port,thrift_server_timeout);
    }
    public int put_result_to_redis(Client beanstalk_client,Job job,
                                   String src_tube_name,String dest_tube_name,String error_tube_name)
    {
        int ret = OK;
        SdkResult result = null;
        JobItemDetail job_item = new JobItemDetail();
        try{
            //
            Jedis jedis = new Jedis(config_parm.getRedis_server_ip(), config_parm.getRedis_server_port());
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
            //
            String buyorsell = job_item.buyorsell.equals("1") ? "buy" : "sell";
            String stock_code = job_item.stock_code;
            String pro_id = job_item.pro_id;
            String id = job_item.id;
            //
            int cur_job_all_sj_num = 0;
            double cur_job_all_sj_price = 0.0;
            //1.加入 项目 stock_code 总表
            jedis.sadd("cur:stocks:" + pro_id,stock_code);
            //2.加入 jobs 子表
            JSONObject jsonObject = JSONObject.fromObject(job_item);
            jedis.rpush("jobs:" + buyorsell + ":result:" + id, jsonObject.toString());
            //3.累计 jobs 表：all_sj_num , all_sj_price
            String cur_account = job_item.getAccount();
            if(buyorsell.equals("buy")) {
                cur_job_all_sj_num += job_item.getSj_buy_num();
                cur_job_all_sj_price += job_item.getSj_buy_price()*job_item.getSj_buy_num();
            }else{
                //sell
            }
            jedis.hincrBy("jobs:" + buyorsell + ":result:all_sj_num", id, cur_job_all_sj_num);
            //jedis.hincrByFloat("jobs:" + buyorsell + ":result:all_sj_price", job_map.get("id"), cur_job_all_sj_price);
            //jedis.hincrBy("jobs:" + buyorsell + ":result:all_sj_price", job_map.get("id"), (int)cur_job_all_sj_price);
            String old_price = jedis.hget("jobs:" + buyorsell + ":result:all_sj_price", id);
            if(old_price == null){
                old_price = "0";
            }
            jedis.hset("jobs:" + buyorsell + ":result:all_sj_price", id, Double.toString(cur_job_all_sj_price + Double.parseDouble(old_price)));
            //4.用于 项目当前库存表
            String account = job_item.account;
            Integer num = cur_job_all_sj_num;
            Double  price = cur_job_all_sj_price;
            String n_p = jedis.hget("cur:stocks:" + pro_id +":" + stock_code, account);
            log.debug("n_p=" + n_p);
            if(null == n_p){
                jedis.hset("cur:stocks:" + pro_id +":" + stock_code,account,num + "|" + price);
            }else{
                String n_p_array[] = n_p.split("\\|");
                if(buyorsell.equals("buy")){
                    num += Integer.parseInt(n_p_array[0]);
                    price += Double.parseDouble(n_p_array[1]);
                }else{
                    num -= Integer.parseInt(n_p_array[0]);
                    price -= Double.parseDouble(n_p_array[1]);
                }
                jedis.hset("cur:stocks:" + pro_id +":" + stock_code,account,num + "|" + price);
            }
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
        return put_result_to_redis(beanstalk_client,job,src_tube_name,dest_tube_name,error_tube_name);
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
        String w_n = (worker_name != null) ? worker_name : "job-result1";
        String w_ip = (worker_ip != null) ? worker_ip : "127.0.0.1";
        int w_port = (worker_port != null) ? Integer.parseInt(worker_port) : 19007;
        log.debug("w_n=" + w_n);
        log.debug("w_ip=" + w_ip);
        log.debug("w_port=" + w_port);
        JobResult jobresult = new JobResult(w_n);
        jobresult.waitWorking(w_ip,w_port,"job-result");
        //

/*        WareHouse inhouse = new WareHouse();
        inhouse.setObj("recv_tube_name", "result");
        inhouse.setObj("send_tube_name", "");
        inhouse.setObj("error_tube_name", "error");
        JobResult jobresult = new JobResult();
        WareHouse outhouse = jobresult.doTask(inhouse);
        log.debug(outhouse);*/
    }
}

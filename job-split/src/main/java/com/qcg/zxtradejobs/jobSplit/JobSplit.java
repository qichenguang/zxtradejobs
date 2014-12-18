package com.qcg.zxtradejobs.jobSplit;

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

public class JobSplit extends JobWorker{
    //log4j
    private static Logger log = Logger.getLogger("JobSplit.class");
    //Ctor
    public JobSplit(){
    }
    public JobSplit(String work_name) {
        super(work_name);
    }
    public JobSplit(String work_name,ConfigParm config_parm){
        super(work_name,config_parm);
    }
    public JobSplit(String work_name,
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
        try {
            //1.
            Map<String, String> job_map = new HashMap<String, String>();
            Map<String, String> pro_account_csje_map = new HashMap<String, String>();
            List msg_list = new ArrayList();
            //2.
            ret = get_job_msg_to_map(job,job_map,pro_account_csje_map);
            //3.
            ret = general_jobs_for_workers(beanstalk_client, job_map, pro_account_csje_map,msg_list);
            //4.
            ret = put_msg_list_to_beanstalk_queue(beanstalk_client,job_map,msg_list);
            //5. delete job
            beanstalk_client.useTube(src_tube_name);
            boolean is_suc = beanstalk_client.delete(job.getJobId());
            log.debug("deltete job:" + is_suc);
        } catch (BeanstalkException be) {
            log.debug(be.getMessage());
            return BEANSTALK_ERROR;
        } catch (Exception e) {
            log.debug(e.getMessage());
            return OTHER_ERROR;
        }
        return ret;
    }
    public int get_job_msg_to_map(Job job,Map job_map,Map pro_account_csje_map){
        try {
            long newJobId = job.getJobId();
            String dstString = new String(job.getData());
            log.debug(dstString);
            //1.get job detail,set to map
            JSONObject job_obj = JSONObject.fromObject(dstString);
            for (Iterator iter = job_obj.keys(); iter.hasNext(); ) {
                String key = (String) iter.next();
                String value = job_obj.get(key).toString();
                job_map.put(key, value);
            }
            //2.get account=>csje, set to map
            JSONObject pro_account_csje_obj = job_obj.getJSONObject("pro_account_csje");
            for (Iterator iter = pro_account_csje_obj.keys(); iter.hasNext(); ) {
                String key = (String) iter.next();
                String value = pro_account_csje_obj.get(key).toString();
                pro_account_csje_map.put(key, value);
            }
        } catch (JSONException je) {
            log.debug(je.getMessage());
            return PARSER_JSON_ERROR;
        }catch(BeanstalkException be) {
            log.debug(be.getMessage());
            return BEANSTALK_ERROR;
        } catch (Exception e) {
            log.debug(e.getMessage());
            return OTHER_ERROR;
        }
        return OK;
    }
    public int general_jobs_for_workers(Client beanstalk_client,Map job_map,Map pro_account_csje_map,List msg_list){
        try
        {
            //1.tube name
            String tube_name = null;
            //2.calc stock num
            String buyorsell = (String)job_map.get("buyorsell");
            if(buyorsell.equals("1")){
                //buy
                tube_name = "buy";
                general_buy_jobs(msg_list,job_map,pro_account_csje_map);
            }else{
                //sell
                tube_name = "sell";
                general_sell_jobs(msg_list,job_map,pro_account_csje_map);
            }
        }catch (BeanstalkException be) {
            log.debug(be.getMessage());
            return BEANSTALK_ERROR;
        } catch (Exception e) {
            log.debug(e.getMessage());
            return OTHER_ERROR;
        }finally {
            beanstalk_client.ignore("jobs");
        }
        //
        return OK;
    }
    public int set_json_obj_comm_field(Map job_map,JSONObject jsonObject){
        try {
            jsonObject.put("pro_id", job_map.get("pro_id"));
            jsonObject.put("id", job_map.get("id"));
            jsonObject.put("stock_code", job_map.get("stock_code"));
            jsonObject.put("exchange_type", job_map.get("exchange_type"));
            jsonObject.put("buyorsell", job_map.get("buyorsell"));
            jsonObject.put("price", job_map.get("price"));
        } catch (Exception e) {
            log.debug(e.getMessage());
            return OTHER_ERROR;
        }
        return OK;
    }
    public int general_buy_jobs(List msg_list,Map job_map,Map pro_account_csje_map){
        try
        {
            float percent = Float.parseFloat((String)job_map.get("percent"));
            float price = Float.parseFloat((String)job_map.get("price"));
            //3.geneal list : account => num
            Set<String> keys = pro_account_csje_map.keySet();
            for (Iterator it = keys.iterator(); it.hasNext();) {
                //account => 100% csje
                String account = (String) it.next();
                float csje = Float.parseFloat((String) pro_account_csje_map.get(account));
                //cur csje : percent% csje
                float cur_csje = csje * percent / 100;
                int num_100 = (int)((cur_csje / price) / 100);
                while(num_100 > 0){
                    Random rand = new Random();
                    //rand num => (0 - 4)  => 1 - 5
                    int buy_num = rand.nextInt(5) + 1;
                    JSONObject jsonObject = new JSONObject();
                    //set default field
                    set_json_obj_comm_field(job_map,jsonObject);
                    jsonObject.put("account", account);
                    if(num_100 <= buy_num){
                        jsonObject.put("yj_buy_num", num_100 * 100);
                        msg_list.add(jsonObject.toString());
                        break;
                    }else{
                        jsonObject.put("yj_buy_num", buy_num * 100);
                        msg_list.add(jsonObject.toString());
                        num_100 -= buy_num;
                    }
                }
            }
        } catch (Exception e) {
            log.debug(e.getMessage());
            return OTHER_ERROR;
        }
        return OK;
    }
    public int general_sell_jobs(List msg_list,Map job_map,Map pro_account_csje_map){
        try
        {
        } catch (Exception e) {
            log.debug(e.getMessage());
            return OTHER_ERROR;
        }
        return OK;
    }
    public int put_msg_list_to_beanstalk_queue(Client beanstalk_client,Map job_map,List msg_list){
        try
        {
            //1.tube name
            String tube_name = null;
            String buyorsell = (String)job_map.get("buyorsell");
            int interval = 30;
            if(buyorsell.equals("1")){
                tube_name = "buy";
            }else{
                tube_name = "sell";
            }
            //4.put jobs to beanstalk
            int delaySeconds = 0;
            beanstalk_client.useTube(tube_name);
            for(int i = 0;i < msg_list.size(); i++){
                int err_num = 0;
                while(err_num < 3) {
                    long jobId = beanstalk_client.put(1024, delaySeconds, 120, ((String) msg_list.get(i)).getBytes());
                    delaySeconds += interval;
                    if(jobId > 0) {
                        break;
                    }else{
                        delaySeconds -= interval;
                        err_num++;
                    }
                }
                if(err_num >= 3){
                    return BEANSTALK_PUT_JOB_ERROR;
                }
            }
        }catch (BeanstalkException be) {
            log.debug(be.getMessage());
            return BEANSTALK_ERROR;
        } catch (Exception e) {
            log.debug(e.getMessage());
            return OTHER_ERROR;
        }finally {
            beanstalk_client.ignore("jobs");
        }
        return OK;
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
        String w_n = (worker_name != null) ? worker_name : "jobsplit";
        String w_ip = (worker_ip != null) ? worker_ip : "127.0.0.1";
        log.debug("worker_port=" + worker_port);
        int w_port = (worker_port != null) ? Integer.parseInt(worker_port) : 9001;
        log.debug("w_port=" + w_port);
        JobWorker jobworker = new JobSplit(w_n);
        jobworker.waitWorking(w_ip,w_port,"job-split");
        //

/*        WareHouse inhouse = new WareHouse();
        inhouse.setObj("recv_tube_name", "jobs");
        JobSplit jobsplit = new JobSplit();
        WareHouse outhouse = jobsplit.doTask(inhouse);
        log.debug(outhouse);*/
    }
}

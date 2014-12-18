package com.qcg.zxtradejobs.jobController;

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
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import redis.clients.jedis.Jedis;
//json
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

//comm
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.Random;
//
import com.qcg.zxtradejobs.util.*;

public class JobController extends Contractor{
    //
    public static final int OK = 1;
    public static final int NOW_NOT_HAVE_JOB = 2;
    public static final int BEANSTALK_ERROR = 3;
    public static final int NO_WAITING_WORKERS_ERROR = 4;
    public static final int BEANSTALK_PUT_JOB_ERROR = 5;
    public static final int PARSER_JSON_ERROR = 6;
    public static final int UNKNOWN_TUBE_NAME = 7;
    public static final int ALL_WORKERS_STOP = 7;
    public static final int OTHER_ERROR = 99;
    //
    public static final int JOB_STATUS_RUN = 2;
    public static final int JOB_STATUS_PAUSE = 3;
    public static final int JOB_STATUS_RESTART = 4;
    public static final int JOB_STATUS_STOP = 5;

    private String ctor_name;

    private Map all_workers = new HashMap<String,HashMap<WareHouse,WorkerLocal>>();

    ConfigParm config_parm = new ConfigParm();
    //log4j
    private static Logger log = Logger.getLogger("JobController.class");
    //Ctor
    public JobController(){
    }
    public JobController(ConfigParm config_parm){
        this.config_parm = config_parm;
    }
    public JobController(String beanstalk_server_ip, int beanstalk_server_port,
                         String redis_server_ip,int redis_server_port,
                         String thrift_server_ip,int thrift_server_port,int thrift_server_timeout) {
        this.config_parm.beanstalk_server_ip = beanstalk_server_ip;
        this.config_parm.beanstalk_server_port = beanstalk_server_port;
        this.config_parm.redis_server_ip = redis_server_ip;
        this.config_parm.redis_server_port = redis_server_port;
        this.config_parm.thrift_server_ip = thrift_server_ip;
        this.config_parm.thrift_server_port = thrift_server_port;
        this.config_parm.thrift_server_timeout = thrift_server_timeout;
    }
    public JobController(String ctor_name) {
        this.ctor_name = ctor_name;
    }
    public JobController(String ctor_name,ConfigParm config_parm){
        this.ctor_name = ctor_name;
        this.config_parm = config_parm;
    }
    public JobController(String ctor_name,
                     String beanstalk_server_ip, int beanstalk_server_port,
                     String redis_server_ip,int redis_server_port,
                     String thrift_server_ip,int thrift_server_port,int thrift_server_timeout) {
        this.ctor_name = ctor_name;
        this.config_parm.beanstalk_server_ip = beanstalk_server_ip;
        this.config_parm.beanstalk_server_port = beanstalk_server_port;
        this.config_parm.redis_server_ip = redis_server_ip;
        this.config_parm.redis_server_port = redis_server_port;
        this.config_parm.thrift_server_ip = thrift_server_ip;
        this.config_parm.thrift_server_port = thrift_server_port;
        this.config_parm.thrift_server_timeout = thrift_server_timeout;
    }



    public WorkerLocal[] get_waiting_workers(String worker_type){
        WorkerLocal[] worker_arr = null;
        try {
            worker_arr = getWaitingWorkers(worker_type);
            log.debug("worker_arr.length:" + ((worker_arr == null) ? 0 :worker_arr.length));
        }catch(Exception e){
            log.debug(e.getMessage());
        }
        return worker_arr;
    }
    public int add_new_workes(String tube_name,WorkerLocal wl,WareHouse wh){
        int ret = OK;
        try {
            if(tube_name == null){
                return UNKNOWN_TUBE_NAME;
            }
            String [] tube_names = {"jobs","buy","sell","status","result","error"};
            boolean is_find = false;
            for(String item : tube_names){
                if(item.equals(tube_name)){
                    is_find = true;
                    break;
                }
            }
            if(false == is_find){
                return UNKNOWN_TUBE_NAME;
            }
            HashMap<WareHouse, WorkerLocal> workers = (HashMap<WareHouse, WorkerLocal>) all_workers.get(tube_name);
            if (null == workers) {
                workers = new HashMap<WareHouse, WorkerLocal>();
            }
            log.debug(wl.toString());
            workers.put(wh, wl);
            all_workers.put(tube_name, workers);
        }catch(Exception e){
            log.debug(e.getMessage());
            return OTHER_ERROR;
        }
        return ret;
    }
    public int stop_all_works(){
        int ret = OK;
        try {
            String [] tube_names = {"jobs","buy","sell","status","result","error"};
            for(String item : tube_names){
                HashMap<WareHouse, WorkerLocal> workers = (HashMap<WareHouse, WorkerLocal>) all_workers.get(item);
                if (null == workers) {
                    continue;
                }
                Iterator<Map.Entry<WareHouse, WorkerLocal>> it = workers.entrySet().iterator();
                while(it.hasNext()){
                    Map.Entry<WareHouse, WorkerLocal> entry = it.next();
                    WareHouse key = entry.getKey();
                    WorkerLocal value = entry.getValue();
                    if (key.getStatus() == WareHouse.NOTREADY) {
                        value.interrupt();
                    }
                    it.remove();
                }
            }
        }catch(Exception e){
            log.debug(e.getMessage());
            return OTHER_ERROR;
        }
        return ret;
    }
    public int remove_ready_works(){
        int ret = OK;
        String [] tube_names = {"jobs","buy","sell","status","result","error"};
        for(String item : tube_names){
            HashMap<WareHouse, WorkerLocal> workers = (HashMap<WareHouse, WorkerLocal>) all_workers.get(item);
            if(workers != null) {
                Iterator<Map.Entry<WareHouse, WorkerLocal>> it = workers.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<WareHouse, WorkerLocal> entry = it.next();
                    WareHouse value = entry.getKey();
                    if (value.getStatus() == WareHouse.READY) {
                        it.remove();
                    } else if (value.getStatus() == WareHouse.EXCEPTION) {
                        it.remove();
                    }
                }
            }
        }
        return ret;
    }
    public int get_tube_stats_and_can_use_workers(Client beanstalk_client,int pos,String[] tube_names,String[] worker_types,WorkerLocal[][] workers) {
        try {
            //1. check tube size
            Map<String, String> tube_stats = beanstalk_client.statsTube(tube_names[pos]);
            for(Map.Entry<String, String> entry:tube_stats.entrySet()){
                log.debug(entry.getKey() + "--->" + entry.getValue());
                if(entry.getKey().equals("current-jobs-ready")){
                    if(Integer.parseInt(entry.getValue()) <= 0){
                        return NOW_NOT_HAVE_JOB;
                    }
                }
            }
            //2.find workers.
            WorkerLocal[] worker_arr = get_waiting_workers(worker_types[pos]);
            if(worker_arr != null && worker_arr.length > 0){
                //delete job
                workers[pos] = worker_arr;
            }else{
                return NO_WAITING_WORKERS_ERROR;
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
    public int check_global_jobs_status(Jedis jedis){
        int ret = OK;
        try {
            //1.check gloabl jobs flasg.
            String job_status = jedis.hget("jobs:status","all");
            log.debug("jobs:status:all,status=" + job_status);
            if(job_status!=null){
                if(JOB_STATUS_PAUSE == Integer.parseInt(job_status)
                        || JOB_STATUS_STOP == Integer.parseInt(job_status)){
                    //停止所有工人计算
                    stop_all_works();
                    return ALL_WORKERS_STOP;
                }
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
    public WareHouse giveTask(WareHouse inhouse){
        return inhouse;
    }

    public void sleep_timeout(long micsec){
        try{
            Thread.sleep(micsec);
        } catch (Exception ex){
            log.debug(ex.getMessage());
        }
    }

    public void run(){
        int ret = OK;
        while(true){
            try {
                //1.init
                Client beanstalk_client = new ClientImpl(config_parm.getBeanstalk_server_ip(),
                        config_parm.getBeanstalk_server_port());
                Jedis jedis = new Jedis(config_parm.getRedis_server_ip(),
                        config_parm.getRedis_server_port());
                //1.
                //7.check statsu, then if stop, interrupt all works.
                ret = check_global_jobs_status(jedis);
                if(ret != OK){
                    //9.休眠等待 1 秒
                    sleep_timeout(1000L);
                    continue;
                }

                String [] tube_names = {"jobs","buy","sell","status","result","error"};
                String [] worker_types = {"job-split","job-buy","job-sell","job-status","job-result","job-error"};
                WorkerLocal[][] workers = new  WorkerLocal[6][];
                String [] recv_tube_names =  {"jobs","buy",   "sell",  "status", "result","error"};
                String [] send_tube_names =  {null  ,"status","status","result", null,     null};
                String [] error_tube_names = {null  ,"error", "error" ,"error",  "error",  null};
                //2.get a job from beanstalk queue, will return NOW_NOT_HAVE_JOB in timeout second.
                for(int pos=0;pos<tube_names.length;pos++){
                    ret = get_tube_stats_and_can_use_workers(beanstalk_client,pos,tube_names, worker_types,workers);
                    //3.put job to workers tube queue.
                    if (OK == ret) {
                        //4.set parm : tube_name
                        WareHouse in_parm = new WareHouse();
                        in_parm.setObj("recv_tube_name", recv_tube_names[pos]);
                        in_parm.setObj("send_tube_name", send_tube_names[pos]);
                        in_parm.setObj("error_tube_name", error_tube_names[pos]);
                        //5.start up workers.
                        for(int i=0;i<workers[pos].length;i++){
                            WareHouse wh = workers[pos][i].doTask(in_parm);
                            add_new_workes(recv_tube_names[pos],workers[pos][i],wh);
                        }
                    }
                    sleep_timeout(1000L);
                }
                //6.remove un use works.
                remove_ready_works();
                //8.close conn obj.
                beanstalk_client.close();
                jedis.close();
            }catch (BeanstalkException be) {
                log.debug(be.getMessage());
            } catch (Exception e) {
                log.debug(e.getMessage());
            }
            //9.休眠等待 1 秒
            sleep_timeout(1000L);
        }
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
/*
        JobController jobctrl = new JobController(b_ip,b_port,r_ip,r_port,t_ip,t_port,t_timeout);
        jobctrl.run();

*/
        //
        JobController jobctrl = new JobController();
        jobctrl.run();
    }
}

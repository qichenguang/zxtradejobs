package com.qcg.zxtradejobs.util;

/**
 * Created by chenguang2 on 2014/11/14.
 */
public class ConfigParm {
    public String beanstalk_server_ip = "172.16.193.114";
    public int beanstalk_server_port = 11300;
    public String redis_server_ip = "127.0.0.1";
    public int redis_server_port = 6379;
    public String thrift_server_ip = "127.0.0.1";
    public int thrift_server_port = 9090;
    public int thrift_server_timeout = 30000;

    public String getThrift_server_ip() {
        return thrift_server_ip;
    }

    public void setThrift_server_ip(String thrift_server_ip) {
        this.thrift_server_ip = thrift_server_ip;
    }
    public int getThrift_server_port() {
        return thrift_server_port;
    }

    public void setThrift_server_port(int thrift_server_port) {
        this.thrift_server_port = thrift_server_port;
    }

    public int getThrift_server_timeout() {
        return thrift_server_timeout;
    }

    public void setThrift_server_timeout(int thrift_server_timeout) {
        this.thrift_server_timeout = thrift_server_timeout;
    }

    public String getBeanstalk_server_ip() {
        return beanstalk_server_ip;
    }

    public void setBeanstalk_server_ip(String beanstalk_server_ip) {
        this.beanstalk_server_ip = beanstalk_server_ip;
    }

    public int getBeanstalk_server_port() {
        return beanstalk_server_port;
    }

    public void setBeanstalk_server_port(int beanstalk_server_port) {
        this.beanstalk_server_port = beanstalk_server_port;
    }

    public String getRedis_server_ip() {
        return redis_server_ip;
    }

    public void setRedis_server_ip(String redis_server_ip) {
        this.redis_server_ip = redis_server_ip;
    }

    public int getRedis_server_port() {
        return redis_server_port;
    }

    public void setRedis_server_port(int redis_server_port) {
        this.redis_server_port = redis_server_port;
    }


}



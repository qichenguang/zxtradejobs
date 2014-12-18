package com.qcg.zxtradejobs.util;

/**
 * Created by chenguang2 on 2014/11/14.
 */
public class JobItemDetail {

    public long job_msg_id;
    public String pro_id;
    public String id; //job_id
    public String job_id;
    public String account;
    public String stock_code;
    public String exchange_type;
    public String price;
    public String buyorsell;
    public String entrust_no;
    public int yj_buy_num;
    public int sj_buy_num;
    public double sj_buy_price;
    public int yj_sell_num;
    public int sj_sell_num;
    public double sj_sell_price;
    public int err_num;
    public long begin_sec;
    public String end_date;

    public int error_code;
    public String error_msg;

    @Override
    public String toString() {
        return "JobItemDetail{" +
                "job_msg_id=" + job_msg_id +
                ", pro_id='" + pro_id + '\'' +
                ", id='" + id + '\'' +
                ", job_id='" + job_id + '\'' +
                ", account='" + account + '\'' +
                ", stock_code='" + stock_code + '\'' +
                ", exchange_type='" + exchange_type + '\'' +
                ", price='" + price + '\'' +
                ", buyorsell='" + buyorsell + '\'' +
                ", entrust_no='" + entrust_no + '\'' +
                ", yj_buy_num=" + yj_buy_num +
                ", sj_buy_num=" + sj_buy_num +
                ", sj_buy_price=" + sj_buy_price +
                ", yj_sell_num='" + yj_sell_num + '\'' +
                ", sj_sell_num='" + sj_sell_num + '\'' +
                ", sj_sell_price='" + sj_sell_price + '\'' +
                ", err_num=" + err_num +
                ", begin_sec=" + begin_sec +
                ", end_date='" + end_date + '\'' +
                '}';
    }

    public int getError_code() {
        return error_code;
    }

    public void setError_code(int error_code) {
        this.error_code = error_code;
    }

    public String getError_msg() {
        return error_msg;
    }

    public void setError_msg(String error_msg) {
        this.error_msg = error_msg;
    }
    public int getYj_sell_num() {
        return yj_sell_num;
    }

    public void setYj_sell_num(int yj_sell_num) {
        this.yj_sell_num = yj_sell_num;
    }

    public int getSj_sell_num() {
        return sj_sell_num;
    }

    public void setSj_sell_num(int sj_sell_num) {
        this.sj_sell_num = sj_sell_num;
    }

    public double getSj_sell_price() {
        return sj_sell_price;
    }

    public void setSj_sell_price(double sj_sell_price) {
        this.sj_sell_price = sj_sell_price;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    public String getExchange_type() {
        return exchange_type;
    }

    public void setExchange_type(String exchange_type) {
        this.exchange_type = exchange_type;
    }

    public long getJob_msg_id() {
        return job_msg_id;
    }

    public void setJob_msg_id(long job_msg_id) {
        this.job_msg_id = job_msg_id;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public int getYj_buy_num() {
        return yj_buy_num;
    }

    public void setYj_buy_num(int yj_buy_num) {
        this.yj_buy_num = yj_buy_num;
    }

    public int getSj_buy_num() {
        return sj_buy_num;
    }

    public void setSj_buy_num(int sj_buy_num) {
        this.sj_buy_num = sj_buy_num;
    }

    public double getSj_buy_price() {
        return sj_buy_price;
    }

    public void setSj_buy_price(double sj_buy_price) {
        this.sj_buy_price = sj_buy_price;
    }

    public String getEntrust_no() {
        return entrust_no;
    }

    public void setEntrust_no(String entrust_no) {
        this.entrust_no = entrust_no;
    }

    public String getBuyorsell() {
        return buyorsell;
    }

    public void setBuyorsell(String buyorsell) {
        this.buyorsell = buyorsell;
    }

    public String getStock_code() {
        return stock_code;
    }

    public void setStock_code(String stock_code) {
        this.stock_code = stock_code;
    }

    public int getErr_num() {
        return err_num;
    }

    public void setErr_num(int err_num) {
        this.err_num = err_num;
    }

    public long getBegin_sec() {
        return begin_sec;
    }

    public void setBegin_sec(long begin_sec) {
        this.begin_sec = begin_sec;
    }

    public String getEnd_date() {
        return end_date;
    }

    public void setEnd_date(String end_date) {
        this.end_date = end_date;
    }

    public String getPro_id() {
        return pro_id;
    }

    public void setPro_id(String pro_id) {
        this.pro_id = pro_id;
    }

    public String getJob_id() {
        return job_id;
    }

    public void setJob_id(String job_id) {
        this.job_id = job_id;
    }


}



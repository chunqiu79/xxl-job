package com.xxl.job.admin.core.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * xxl_job_group 表对应的记录
 */
public class XxlJobGroup {

    private int id;
    /**
     * 执行器应用名
     */
    private String appname;
    private String title;

    /**
     * 执行器地址类型
     * 0-自动注册   1-手动录入
     */
    private int addressType;

    /**
     * 执行器地址列表，多地址逗号分隔
     * 其实就是客户端地址
     */
    private String addressList;
    private Date updateTime;

    /**
     * 执行器地址列表
     */
    private List<String> registryList;
    public List<String> getRegistryList() {
        if (addressList != null && !addressList.trim().isEmpty()) {
            registryList = new ArrayList<>(Arrays.asList(addressList.split(",")));
        }
        return registryList;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getAppname() {
        return appname;
    }

    public void setAppname(String appname) {
        this.appname = appname;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public int getAddressType() {
        return addressType;
    }

    public void setAddressType(int addressType) {
        this.addressType = addressType;
    }

    public String getAddressList() {
        return addressList;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public void setAddressList(String addressList) {
        this.addressList = addressList;
    }

}

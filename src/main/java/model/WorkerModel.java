package model;

import com.google.common.collect.Lists;

import java.util.List;

public class WorkerModel {
    private int status;
    private int taskNum;
    private long beginTime;
    private List<TaskModel> taskList;
    private String name;
    private String address;


    public WorkerModel() {
    }

    public WorkerModel(String name) {
        this.taskNum = 0;
        this.beginTime = System.currentTimeMillis();
        this.taskList = Lists.newArrayList();
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getTaskNum() {
        return taskNum;
    }

    public void setTaskNum(int taskNum) {
        this.taskNum = taskNum;
    }

    public long getBeginTime() {
        return beginTime;
    }

    public void setBeginTime(long beginTime) {
        this.beginTime = beginTime;
    }

    public List<TaskModel> getTaskList() {
        return taskList;
    }

    public void setTaskList(List<TaskModel> taskList) {
        this.taskList = taskList;
    }

    public boolean submitTask(TaskModel taskModel) {
        return false;
    }
}

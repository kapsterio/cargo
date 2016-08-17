/*
  Created by zhangheng on 8/10/16.
         quu..__
         $$$b  `---.__
          "$$b        `--.                          ___.---uuudP
          `$$b           `.__.------.__     __.---'      $$$$"              .
             "$b          -'            `-.-'            $$$"              .'|
               ".                                       d$"             _.'  |
                 `.   /                              ..."             .'     |
                   `./                           ..::-'            _.'       |
                    /                         .:::-'            .-'         .'
                   :                          ::''\          _.'            |
                  .' .-.             .-.           `.      .'               |
                  : /'$$|           .@"$\           `.   .'              _.-'
                 .'|$u$$|          |$$,$$|           |  <            _.-'
                 | `:$$:'          :$$$$$:           `.  `.       .-'
                 :                  `"--'             |    `-.     \
                :**.       ==             .***.       `.      `.    `\
                |**:                      :***:        |        >     >
                |#'     `..'`..'          `***'        x:      /     /
                 \                                   xXX|     /    ./
                  \                                xXXX'|    /   ./
                  /`-.                                  `.  /   /
                 :    `-  ...........,                   | /  .'
                 |         ``:::::::'       .            |<    `.
                 |             ```          |           x| \ `.:``.
                 |                         .'    /'   xXX|  `:`M`M':.
                 |    |                    ;    /:' xXXX'|  -'MMMMM:'
                 `.  .'                   :    /:'       |-'MMMM.-'
                  |  |                   .'   /'        .'MMM.-'
                  `'`'                   :  ,'          |MMM<
                    |                     `'            |tbap\
                     \                                  :MM.-'
                      \                 |              .''
                       \.               `.            /
                        /     .:::::::.. :           /
                       |     .:::::::::::`.         /
                       |   .:::------------\       /
                      /   .''               >::'  /
                      `',:                 :    .'
                                           `:.:'
  Give me strength! Pikachu ~
  一个master-worker架构的分布式任务调度系统

  master:
    master采用主备结构的高可用设计，主挂掉后，备能快速take over,并且恢复状态（包括所有可用的worker、任务分配情况、未分配的任务）
    保证at-least-once交付任务
    问题：为什么我要设计成单主，为什么不能多主（因为，单主实现方便）

    master负责：接受用户提交任务请求、调度任务（分配任务以及worker挂掉时重分配任务）

    内存数据结构：
        1）workersMap 保存着当前所有worker，以及每个work正在执行的任务集
        2) tasksQueue 保存当前待分配的任务
    ZK数据结构：
        1）/master :短生节点 用于主备切换
        2) /workers/worker- : worker-是短生节点，用于worker列表监听
        3) /assign/worker- /task- : 非短生节点，用于任务分配情况记录
        4) /tasks/task- : 非短生节点，用于当前未分配任务记录
        // 单纯用于记录状态的也可以使用其他组件来完成

    当用户提交任务时，先向ZK发出写请求 "/task/"，收到ZK成功响应后，响应用户，并入taskQueue(生产者)。(这里可以做些优化，比如批量写入ZK)。
    消费者线程（多个）从taskQueue中取出一个task，然后分配任务给worker (assignTask),在这里会选择一个worker，调用它的提交任务的RPC方法，
    然后记录变动到ZK。

    当一个worker做完任务时，需要告知master，怎么告知？rpc or zk notification, 通过RPC告知的话，worker不能在ZK中修改assign下的信息
    不然就存在隐藏信道问题。通过zk notification告知的话，master就需要监听/assign下所有worker，实现起来比较费事，所以选择RPC。
    引入新的组件——数据库（sql/nonsql都可以），worker做完任务后将任务的结果入库，并通知master，master则更新内存数据结构和ZK数据结构




 */

import model.TaskModel;
import model.WorkerModel;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

public class Master implements Watcher {
    ZooKeeper zk;
    String host;
    private boolean isLeader;
    //master状态机: -1:down, 0:选主中, 1: 备, 2: 主
    private int status;
    private String serverID;
    private Map<String, WorkerModel> workersMap;
    BlockingDeque<TaskModel> taskQueue;

    static Logger LOG = LoggerFactory.getLogger(Master.class);

    Master(String host) {
        this.host = host;
        Random random = new Random(System.currentTimeMillis());
        serverID = Integer.toHexString(random.nextInt());
    }

    Master(String host, String serverID) {
        this.host = host;
        this.serverID = serverID;
        this.workersMap = new ConcurrentHashMap<>();
        this.status = -1;
        this.taskQueue = new LinkedBlockingDeque<>();
    }

    synchronized void startZK() throws Exception {
        status = 0;
        zk = new ZooKeeper(host, 15000, this);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        LOG.info(serverID + ":" + watchedEvent.toString() + "," + host);
        if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
            assert "/master".equals(watchedEvent.getPath());
            runForMasterAsync();
        } else if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
            //assert "/workers".equals(watchedEvent.getPath());
            getWorkers();
        }
    }

    //会在zk的callback线程里调用，watch到worker列表变动后，也会在普通线程里调用
    void getWorkers() {
        AsyncCallback.ChildrenCallback callback = (rc, path, ctx, list) -> {
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    LOG.info(serverID + ": got a list of workers : " + list.size()
                            + " workers");
                    updateWorkMap(list);
                    break;
                case CONNECTIONLOSS:
                    getWorkers();
                    break;
                default:
                    LOG.error(serverID + ": getChildren failed", KeeperException.create(KeeperException.Code.get(rc), path));
                    notifyDown();
            }
        };
        zk.getChildren("/workers", true, callback, null);
    }

    //在普通线程中调用
    void getTasks() {
        // 读取"/tasks"下的所有task，这些是目前还没有分配给worker的task，加到taskQueue中
    }

    //在普通线程中调用
    void getAssigns() {
        this.workersMap.entrySet().forEach(entry -> {
            //同步读取 "/assign/work-XXX"下的所有分配任务，构建workerMap
        });
    }


    //提供给用户的方法，用于提交任务, 任务生成者
    void submitTask(TaskModel task) {
        //写ZK
        this.taskQueue.add(task);
    }

    //任务消费者，从taskQueue中取任务，然后分配给一个worker，并写入 "/assign/worker-XX/"，
    //同时删掉 "/task/task-XXX",二者是原子操作
    void assignTask() throws InterruptedException{
        TaskModel task = taskQueue.take();
        WorkerModel worker = chooseWoker(task);
        worker.getTaskList().add(task);
        while (!worker.submitTask(task)) {
            worker = chooseWoker(task);
        }
        Op createAssign = Op.create("/assign/" + worker.getName() + "/" + task.getName(), "pre".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        Op deleteTask = Op.delete("/tasks/" + task.getName(), -1);
        try {
            List<OpResult> results = zk.multi(Arrays.asList(createAssign, deleteTask));
        } catch (KeeperException e) {
            //该干嘛干嘛
        }
    }

    //为task选择一个worker
    WorkerModel chooseWoker(TaskModel task) {
        return null;
    }






    void updateWorkMap(List<String> workers) {
        //内存中有，zk中无
        //Iterables.removeIf(this.workersMap.entrySet(), entry -> !workers.contains(entry.getKey()));
        Iterator<Map.Entry<String, WorkerModel>> iter = this.workersMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, WorkerModel> entry = iter.next();
            //zk的worker列表中已经不包含这个work了
            if(!workers.contains(entry.getKey())) {
                //taskQueue.addAll(entry.getValue().getTaskList());
                entry.getValue().getTaskList().forEach(task -> this.submitTask(task));
                //todo 删掉 "/assign/worker/task-XX"
                iter.remove();

            }
        }

        //再增加上zk中有，内存中无的
        workers.forEach(worker -> workersMap.computeIfAbsent(worker, w -> {
            //todo create /assign/worker
            return  new WorkerModel(w);
        }));
    }


    synchronized void changeStatus(int status) {
        this.status = status;
    }

    synchronized void notifyDown() {
        status = -1;
        notify();
    }


    synchronized void waitForDown() throws InterruptedException {
        while (status != -1) {
            wait();
        }
    }

    synchronized int getStatus() {
        return status;
    }


    //异步检查是否选主成功
    //这里和书上不同，书上写法有问题，问题在于没有OK的case
    void checkMasterAsync() {
        AsyncCallback.DataCallback callback = (rc, path, ctx, data, stat) -> {
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    isLeader = new String(data).equals(serverID);
                    if (isLeader) {
                        takeLeaderShip();
                    } else {
                        masterExists();
                    }
                    //有结果
                    changeStatus(isLeader ? 2 : 1);
                    break;
                case NONODE:
                    changeStatus(0);
                    runForMasterAsync();
                    break;
                case CONNECTIONLOSS:
                    checkMasterAsync();
                    break;
                default:
                    LOG.error(serverID + ": something went wrong" + KeeperException.create(KeeperException.Code.get(rc)));
                    notifyDown();
            }

        };
        zk.getData("/master", false, callback, null);
    }

    //异步选主, 这里和书上也有点不同，default情况下我们都没法判断结果，只能再发起一起checkMasterAsync
    void runForMasterAsync() {
        AsyncCallback.StringCallback callback = (rc, path, ctx, name) -> {
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    isLeader = true;
                    takeLeaderShip();
                    break;
                case NODEEXISTS:
                    isLeader = false;
                    //通过exist来监听/master
                    masterExists();
                    break;
                case CONNECTIONLOSS:
                    //为什么这里不能直接runForMasterAsync呢，因为这个动作不幂等
                    checkMasterAsync();
                    return;
                default:
                    //isLeader = false;
                    notifyDown();
                    LOG.error(serverID + ":" + "something went wrong", KeeperException.create(KeeperException.Code.get(rc), path));
                    return;
            }
            //有结果了
            changeStatus(isLeader ? 2 : 1);
        };
        zk.create("/master", serverID.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                callback, null);
    }

    void takeLeaderShip() {
        //do something that a leader should do
        System.out.println(serverID + ":" + "taken the leadership");
        getWorkers();
        getTasks();
    }


    void masterExists() {
        AsyncCallback.StatCallback callback = (rc, path, ctx, stat) -> {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    masterExists();
                    break;
                case OK:
                    if (stat == null) {
                        //这步很重要，因为从create到exist，获取到master的节点在这中间可能挂掉了，
                        //此时stat为null，那么就需要runForMaster了
                        changeStatus(0);
                        runForMasterAsync();
                    }
                    break;
                default:
                    //这里可能session过期了
                    checkMasterAsync();
                    break;

            }
        };
        zk.exists("/master", true, callback, null);
    }

    //同步选主
    //注意这里逻辑和书上有些不同，因为一旦create成功了，说明选主成功没有必要再checkMaster了
    //除了NodeExistsException外的其他情况，我们都无法判断选主结果，因此都需要去checkMaster
    void runForMaster() throws InterruptedException {
        while (true) {
            try {
                zk.create("/master", serverID.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                isLeader = true;
                return;
            } catch (KeeperException.NodeExistsException e) {
                isLeader = false;
                break;
            } catch (KeeperException e) {
                //对于其他的KeeperException包括ConnectionLossException，先check下有没有主
                if (checkMaster()) break;
            }

        }
    }

    //同步check有没有选出主
    boolean checkMaster() throws InterruptedException {
        while (true) {
            try {
                Stat stat = new Stat();
                byte data[] = zk.getData("/master", false, stat);
                isLeader = new String(data).equals(serverID);
                //已经选出主了
                return true;
            } catch (KeeperException.NoNodeException e) {
                //没有主
                return false;
            } catch (KeeperException e) {
            }
        }
    }

    void close() throws InterruptedException {
        zk.close();
    }

    public void bootstrap() {
        AsyncCallback.StringCallback callback = new AsyncCallback.StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                switch (KeeperException.Code.get(rc)) {
                    case OK:
                        LOG.info(serverID + ":" + "parent created");
                        break;
                    case NODEEXISTS:
                        LOG.info(serverID + ":" + "parent already registered :" + path);
                        break;
                    case CONNECTIONLOSS:
                        LOG.warn(serverID + ":" + "connection loss, so retry" + path);
                        createParent(path, (byte[]) ctx, this);
                        break;
                    default:
                        LOG.error(serverID + ":" + "something went wrong: " +
                                KeeperException.create(KeeperException.Code.get(rc), path));
                }
            }
        };
        createParent("/workers", new byte[0], callback);
        createParent("/assign", new byte[0], callback);
        createParent("/tasks", new byte[0], callback);
        createParent("/status", new byte[0], callback);
    }

    void createParent(String path, byte[] data, AsyncCallback.StringCallback callback) {
        zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                callback, data);
    }


    public static void main(String args[]) throws Exception {
        Master master = new Master(args[0]);
        master.startZK();
        //zkTest.runForMaster();
        master.runForMasterAsync();
        /**
         if (zkTest.isLeader) {
         System.out.println("I am leader");
         Thread.sleep(6000);
         } else {
         System.out.println("I failed");
         }
         **/
        Thread.sleep(5000);
        master.close();
    }

}

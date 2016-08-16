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
 */

import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import model.TaskModel;
import model.WorkerModel;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

public class Master implements Watcher {
    ZooKeeper zk;
    String host;
    boolean isLeader;
    //master状态机: -1:down, 0:选主中, 1: 备, 2: 主
    int status = -1;
    String serverID;
    List<WorkerModel> workers;
    List<TaskModel> tasks;

    static Logger LOG = LoggerFactory.getLogger(Master.class);

    Master(String host) {
        this.host = host;
        Random random = new Random(System.currentTimeMillis());
        serverID = Integer.toHexString(random.nextInt());
    }

    Master(String host, String serverID) {
        this.host = host;
        this.serverID = serverID;
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
            if ("/workers".equals(watchedEvent.getPath())) {
                getWorkers();
            } else if ("/tasks".equals(watchedEvent.getPath())) {
                //getTasks();
            }

        }
    }

    void getWorkers() {
        AsyncCallback.ChildrenCallback callback = new AsyncCallback.ChildrenCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, List<String> list) {
                switch (KeeperException.Code.get(rc)) {
                    case OK:
                        LOG.info(serverID + ": got a list of workers : " + list.size()
                                + " workers");
                        reassignAndSet(list);
                        break;
                    case CONNECTIONLOSS:
                        getWorkers();
                        break;
                    default:
                        LOG.error(serverID + ": getChildren failed", KeeperException.create(KeeperException.Code.get(rc), path));
                        notifyDown();
                }
            }
        };
        zk.getChildren("/workers", true, callback, null);
    }

    boolean isWorkerInList(String work) {
        for (WorkerModel workerModel : this.workers) {
            if (workerModel.getName().equals(work)) {
                return true;
            }
        }
        return false;
    }

    void reassignAndSet(List<String> workers) {
        //this.workers 当前内存中的worker对象列表
        //workers 从zk获取的变动后的worker列表

        //先删掉内存中有，zk中无的
        Iterables.removeIf(this.workers, workerModel -> !workers.contains(workerModel.getName()));

        //再增加上zk中有，内存中无的
        List<String> currentWorkers = Lists.transform(this.workers, WorkerModel::getName);

        //1) 第一种实现
        for (String worker : workers) {
            if (!currentWorkers.contains(worker)) {
                WorkerModel wm = new WorkerModel();
                wm.setName(worker);
                wm.setBeginTime((int) System.currentTimeMillis());
                wm.setTaskNum(0);
                wm.setTaskList(Lists.newArrayList());
                this.workers.add(wm);
            }
        }

        //2) 第二种实现
        this.workers.addAll(Lists.newArrayList(
                FluentIterable.from(workers)
                .filter(worker -> !currentWorkers.contains(worker))
                .transform(s -> {
                    WorkerModel wm = new WorkerModel();
                    wm.setName(s);
                    wm.setBeginTime((int) System.currentTimeMillis()/1000);
                    wm.setTaskNum(0);
                    wm.setTaskList(Lists.newArrayList());
                    return wm;
                })));
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
        AsyncCallback.DataCallback callback = new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
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

            }
        };
        zk.getData("/master", false, callback, null);
    }

    //异步选主, 这里和书上也有点不同，default情况下我们都没法判断结果，只能再发起一起checkMasterAsync
    void runForMasterAsync() {
        AsyncCallback.StringCallback callback = new AsyncCallback.StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
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
            }
        };
        zk.create("/master", serverID.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                callback, null);
    }

    void takeLeaderShip() {
        //do something that a leader should do
        System.out.println(serverID + ":" + "taked the leadership");
    }


    void masterExists() {
        AsyncCallback.StatCallback callback = new AsyncCallback.StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
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

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/*
  Created by zhangheng on 8/13/16.
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
public class Worker implements Watcher {
    static Logger LOG = LoggerFactory.getLogger(Worker.class);
    ZooKeeper zk;
    String hostPort;
    String serverID;
    String status;

    public Worker(String hostPort) {
        this.hostPort = hostPort;
        Random random = new Random(System.currentTimeMillis());
        serverID = Integer.toHexString(random.nextInt());
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        LOG.info(watchedEvent.toString() + "," + hostPort);
    }

    void startZK() throws Exception{
        zk = new ZooKeeper(hostPort, 15000, this);
    }
    void close() throws InterruptedException{
        zk.close();
    }

    void register() {
        AsyncCallback.StringCallback callback = new AsyncCallback.StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                switch (KeeperException.Code.get(rc)) {
                    case OK:
                        LOG.info("registered successfully: " + serverID);
                        break;
                    case NODEEXISTS:
                        LOG.warn("already registered: " + serverID);
                        break;
                    case CONNECTIONLOSS:
                        LOG.warn("connection loss" + serverID);
                        //为什么这里又能重试呢，因为worker重新register是幂等的
                        register();
                        break;
                    default:
                        LOG.error("something went wrong: " +
                                KeeperException.create(KeeperException.Code.get(rc),path));

                }
            }
        };
        zk.create("/workers/worker-" + serverID, "Idle".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                callback, null);
    }


    synchronized private void updateSatus(final String status) {
        //这里的check很重要
        if (status == this.status) {
            AsyncCallback.StatCallback callback = new AsyncCallback.StatCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, Stat stat) {
                    switch (KeeperException.Code.get(rc)) {
                        case OK:
                            LOG.info("update status" + ctx);
                            break;
                        case CONNECTIONLOSS:
                            LOG.warn("connection loss when updateStatus" + serverID + "," + ctx);
                            updateSatus((String) ctx);
                            break;
                        default:
                            LOG.error("something went wrong" + KeeperException.create(KeeperException.Code.get(rc)), path);

                    }
                }
            };
            //uncondotional update
            zk.setData("/workers/worker-" + serverID, status.getBytes(), -1,
                    callback, status);
        }
    }

    //work创建伊始，status是Idle，后续状态会发生变化，因此封装一个setStatus方法来setData
    //然而setData可能会产生connectionloss，因此需要将status保存下，以便重试
    void setStatus(String status) {
        this.status = status;
        updateSatus(status);
    }

    public static void main(String []args) throws Exception{
        Worker worker = new Worker(args[0]);
        worker.startZK();
        worker.register();
        Thread.sleep(3000);
        worker.close();
    }
}

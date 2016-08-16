import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

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
public class AdminClient implements Watcher {
    static Logger LOG = LoggerFactory.getLogger(AdminClient.class);
    ZooKeeper zk;
    String hostPort;

    void startZK() throws Exception{
        zk = new ZooKeeper(hostPort, 15000, this);
    }
    void close() throws InterruptedException{
        zk.close();
    }

    public AdminClient(String hostPort) {
        this.hostPort = hostPort;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        LOG.info(watchedEvent.toString() + "," + hostPort);
    }

    void listState() throws Exception {
        try {
            Stat stat = new Stat();
            byte data[] = zk.getData("/master", false, stat);
            Date startDate = new Date(stat.getCtime());
            System.out.println("Master: " + new String(data) + " since " + startDate);
        } catch (KeeperException.NoNodeException e) {
            System.out.println("No master");
        }

        System.out.println("workers:");
        for (String w: zk.getChildren("/workers", false)) {
            byte data[] = zk.getData("/workers/" + w, false, null);
            String status = new String(data);
            System.out.println("\t" + w + " : " + status);
        }

        System.out.println("Tasks:");
        for (String t : zk.getChildren("/tasks", false)) {
            System.out.println("\t" + t);
        }
    }


    public static void main(String args[]) throws Exception{
        AdminClient client = new AdminClient(args[0]);
        client.startZK();
        client.listState();
        client.close();
    }
}

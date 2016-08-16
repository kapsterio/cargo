import org.junit.Test;

import java.util.Random;

/*
  Created by zhangheng on 8/14/16.
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
public class TestMaster {

    Runnable createAMaster(final Master master) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    master.startZK();
                    master.runForMasterAsync();
                    master.waitForDown();
                    master.close();
                    System.out.println(master.serverID + " closed");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
    }
    @Test
    public void testMasterShip() throws Exception{
        Random random = new Random(System.currentTimeMillis());
        String id1 = Integer.toHexString(random.nextInt());
        String id2 = Integer.toHexString(random.nextInt());
        final Master master1 = new Master("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183", id1);
        final Master master2 = new Master("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183", id2);
        Thread thread1 = new Thread(createAMaster(master1));
        Thread thread2 = new Thread(createAMaster(master2));
        thread1.start();
        thread2.start();
        Thread.sleep(5000);
        master1.notifyDown();
        Thread.sleep(1000);
        master2.notifyDown();
        Thread.sleep(2000);

    }
}

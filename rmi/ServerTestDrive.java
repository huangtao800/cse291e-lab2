package rmi;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Created by tao on 4/23/16.
 */
public class ServerTestDrive {

    public static void main(String[] args) throws RMIException {
        TestImpl impl = new TestImpl();
        Skeleton<TestInterface> skeleton = new Skeleton<TestInterface>(TestInterface.class, impl,
                new InetSocketAddress("127.0.0.1", 8081));
        skeleton.start();
        System.out.println("Server starts...");
//        skeleton.stop();
    }
}

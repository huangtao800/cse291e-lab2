package rmi;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by tao on 4/23/16.
 */
public class ClientTestDrive {

    public static void main(String[] args) throws RMIException {
//        InetSocketAddress address = new InetSocketAddress(7000);
//        try {
//            ServerSocket socket = new ServerSocket();
//            TestInterface stub = Stub.create(TestInterface.class, address);
//            socket.bind(address);
//
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }


        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 8081);
        TestInterface testInterface = Stub.create(TestInterface.class, address);

        boolean t = testInterface.equals(testInterface, 12);
        System.out.println(t);
    }

    private class ConnectionCheckThread implements Runnable{
        ServerSocket socket;
        ConnectionCheckThread(ServerSocket socket){
            this.socket = socket;
        }
        @Override
        public void run() {
            try {
                Socket connected = socket.accept();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

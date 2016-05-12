package rmi;

import java.io.*;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Created by tao on 4/15/16.
 */
public class MyInvocationHandler<T> extends Stub implements InvocationHandler {
    private Socket socket;
    private Skeleton<T> skeleton = null;
    private String hostname;
    private int port;
    private Class<T> c;

    public MyInvocationHandler(InetSocketAddress address, Class<T> c) throws IOException {
        this.hostname = address.getHostName();
        this.port = address.getPort();
        this.c = c;
    }

    public MyInvocationHandler(Skeleton skeleton, Class<T> c) throws IOException {
//        this.socket = new Socket(skeleton.iAddress.getAddress(), skeleton.iAddress.getPort());
        this.skeleton = skeleton;
        this.c = c;
        this.hostname = skeleton.iAddress.getHostName();
        this.port = skeleton.iAddress.getPort();
    }

    public MyInvocationHandler(String hostName, int port, Class<T> c) throws IOException {
        this.hostname = hostName;
        this.port = port;
        this.c = c;
    }


    private boolean isInRemoteInterface(Method method){
        Method[] methods = this.c.getDeclaredMethods();
        for(Method m : methods){
//            if(m.getName().equals(method.getName()))    return true;
            if(m.equals(method))    return true;
        }
        return false;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String name = method.getName();
        boolean isRemote = isInRemoteInterface(method);
        if(isRemote){
            return invokeRemoteMethod(proxy, method, args);
        }
        if(name.equals("equals") || name.equals("toString") || name.equals("hashCode")) {
            return invokeObjectMethod(proxy, method, args);
        } else {
            return invokeRemoteMethod(proxy, method, args);
        }

    }

    /**
     * Handles java.lang.Object methods
     * @param proxy
     * @param method
     * @param args
     * @return
     */
    public Object invokeObjectMethod(Object proxy, Method method, Object[] args) throws Throwable {
        String name = method.getName();
        if(name.equals("hashCode")) {
            if(proxy == null)   return proxy.hashCode();
            MyInvocationHandler handler = (MyInvocationHandler) Proxy.getInvocationHandler(proxy);
            return handler.hashCode();

        } else if(name.equals("equals")) {
            Object obj = args[0];
            if(proxy == null && obj == null)    return true;
            if(obj == null)   return false;
            MyInvocationHandler proxyHandler = (MyInvocationHandler) Proxy.getInvocationHandler(proxy);
            if(!Proxy.isProxyClass(obj.getClass())) {
                return false;
            }
            MyInvocationHandler objHandler = (MyInvocationHandler) Proxy.getInvocationHandler(obj);

            if(proxyHandler.skeleton != null) {
                System.out.println("skeleton is not null");
                return proxy.getClass().equals(obj.getClass())
                        && equals(objHandler);
            }
//            System.out.println("skeleton is null");
            return proxyHandler.equals(objHandler);
            //return proxyHandler.hashCode() == objHandler.hashCode();

        } else if(name.equals("toString")) {
            return proxyToString(proxy);
        } else {
            throw new IllegalArgumentException(
                    "unexpected Object method: " + method);
        }
    }

    public Serializable[] marshall(Method method, Object[] args) throws NotSerializableException {
        int argNum = 0;
        Class[] argTypes = method.getParameterTypes();
        argNum = argTypes.length;

        Serializable[] request = new Serializable[argNum + 2];
        int pos = 0;
        String methodName = method.getName();
        request[pos++] = methodName;
        request[pos++] = method.getParameterTypes();

        try{
            for(int i=0;i<argNum;i++){
                try{
                    Serializable t = (Serializable) args[i];
                    new ObjectOutputStream(new ByteArrayOutputStream()).writeObject(t);
                    request[pos++] = t;
                } catch (IOException e) {
//                    e.printStackTrace();
                    request[pos++] = null;
                }


            }
        }catch (ClassCastException e){
            throw new NotSerializableException("Argument(s) Not Serializable");
        }
        return request;
    }

    public Object invokeRemoteMethod(Object proxy, Method method, Object[] args) throws Throwable {
        // Global variables needs synchronization.
        synchronized (this) {
            if (this.skeleton != null) {
                this.socket = new Socket(skeleton.iAddress.getAddress(), skeleton.iAddress.getPort());
            } else {
                this.socket = new Socket(hostname, port);
            }
        }

        Object ret = null;
        ObjectOutputStream oos;
        ObjectInputStream ois;
        try {
            oos = new ObjectOutputStream(socket.getOutputStream());
            Serializable[] request = marshall(method, args);

            oos.writeObject(request);
            oos.flush();
        }catch (Exception e){
            if(method.getName().equals("equals")) {
                e.printStackTrace();
            }
            throw new RMIException("Remote call fails. Throws " + e.getClass().getName());
        }

        Class returnType = method.getReturnType();
        try{
            ois = new ObjectInputStream(socket.getInputStream());
            ret = ois.readObject();
        }catch (Exception e){
            throw new RMIException("Remote call fails");
        }

        if(ret instanceof String && returnType==void.class){
            String retString = (String) ret;
            if(retString.equals("Complete"))    return null;
        }
        if(ret instanceof Object[]){
            Object[] arr = (Object[]) ret;
            if(arr[0] instanceof String){
                String str = (String) arr[0];
                if(str.equals("Throw error")){
                    Object excep = arr[1];
                    throw (Exception) excep;
                }
            }
        }

        if(oos!=null)   oos.close();
        if(ois!=null)   ois.close();
        this.socket.close();
        return ret;
    }

    private String proxyToString(Object proxy) {
        String address = "Host name: " + this.hostname + ", port: "
                + this.port;
        Class<?>[] interfaces;
        interfaces = proxy.getClass().getInterfaces();
        if(interfaces.length == 0) {
            return "Proxy[" + this + "]" + ", " + address;
        }
        String iface = interfaces[0].getName();
        if (iface.equals("java.rmi.Remote") && interfaces.length > 1) {
            iface = interfaces[1].getName();
        }
        int dot = iface.lastIndexOf('.');
        if (dot >= 0) {
            iface = iface.substring(dot + 1);
        }
        return "Proxy[" + iface + "," + this + "]" + ", " + address;
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public int hashCode() {
        return c.hashCode() + hostname.hashCode() + port;
    }

    @Override
    public boolean equals(Object obj) {
        if(this == null && obj == null) {
            return true;
        }
        if(this == null || obj == null) {
            return false;
        }

        if(!(obj instanceof MyInvocationHandler)) {
            return false;
        }
        if(!(((MyInvocationHandler) obj).c.equals(this.c))) {
            return false;
        }
        if(this.skeleton != null && ((MyInvocationHandler) obj).skeleton != null) {
            if(!(this.skeleton.getClass().equals(((MyInvocationHandler) obj).skeleton.getClass()))) {
                return false;
            }
        }
        return obj.hashCode() == this.hashCode();
    }
}

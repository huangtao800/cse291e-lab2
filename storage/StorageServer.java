package storage;

import java.io.*;
import java.net.*;
import java.util.ArrayList;

import common.*;
import rmi.*;
import naming.*;

/** Storage server.

    <p>
    Storage servers respond to client file access requests. The files accessible
    through a storage server are those accessible under a given directory of the
    local filesystem.
 */
public class StorageServer implements Storage, Command
{
    private File root;
    private int client_port = 8801;
    private int command_port = 8802;
    private Skeleton<Storage> clientSkeleton = null;
    private Skeleton<Command> commandSkeleton = null;
    /** Creates a storage server, given a directory on the local filesystem, and
        ports to use for the client and command interfaces.

        <p>
        The ports may have to be specified if the storage server is running
        behind a firewall, and specific ports are open.

        @param root Directory on the local filesystem. The contents of this
                    directory will be accessible through the storage server.
        @param client_port Port to use for the client interface, or zero if the
                           system should decide the port.
        @param command_port Port to use for the command interface, or zero if
                            the system should decide the port.
        @throws NullPointerException If <code>root</code> is <code>null</code>.
    */
    public StorageServer(File root, int client_port, int command_port)
    {
        if(root == null) throw new NullPointerException("Root is null");
        this.root = root;
        if(client_port != 0)  this.client_port = client_port;
        if(command_port != 0)   this.command_port = command_port;
    }

    /** Creats a storage server, given a directory on the local filesystem.

        <p>
        This constructor is equivalent to
        <code>StorageServer(root, 0, 0)</code>. The system picks the ports on
        which the interfaces are made available.

        @param root Directory on the local filesystem. The contents of this
                    directory will be accessible through the storage server.
        @throws NullPointerException If <code>root</code> is <code>null</code>.
     */
    public StorageServer(File root)
    {
        if(root == null)    throw new NullPointerException("Root is null");
        this.root = root;
    }

    /** Starts the storage server and registers it with the given naming
        server.

        @param hostname The externally-routable hostname of the local host on
                        which the storage server is running. This is used to
                        ensure that the stub which is provided to the naming
                        server by the <code>start</code> method carries the
                        externally visible hostname or address of this storage
                        server.
        @param naming_server Remote interface for the naming server with which
                             the storage server is to register.
        @throws UnknownHostException If a stub cannot be created for the storage
                                     server because a valid address has not been
                                     assigned.
        @throws FileNotFoundException If the directory with which the server was
                                      created does not exist or is in fact a
                                      file.
        @throws RMIException If the storage server cannot be started, or if it
                             cannot be registered.
     */
    public synchronized void start(String hostname, Registration naming_server)
        throws RMIException, UnknownHostException, FileNotFoundException
    {
        if(!this.root.exists()) throw new FileNotFoundException("Root not found");
        Storage clientStub = null;
        Command commandStub = null;

        try{
            InetSocketAddress clientAddress = new InetSocketAddress(hostname, client_port);
            clientSkeleton = new Skeleton<>(Storage.class, this, clientAddress);
            clientStub = Stub.create(Storage.class, clientAddress);

            InetSocketAddress commandAddress = new InetSocketAddress(hostname, command_port);
            commandSkeleton = new Skeleton<>(Command.class, this, commandAddress);
            commandStub = Stub.create(Command.class, commandAddress);
        }catch (Exception e){
            e.printStackTrace();
            throw new UnknownHostException("Invalid address");
        }

        try {
            ArrayList<Path> paths = new ArrayList<>();
            Path rootPath = new Path();
            paths.add(rootPath);
            recursiveList(this.root, rootPath, paths);

            clientSkeleton.start();
            commandSkeleton.start();
            Path[] toDelete = naming_server.register(clientStub, commandStub, paths.toArray(new Path[0]));
            for(Path p : toDelete){
                this.delete(p);
            }
        }catch (Exception e){
            e.printStackTrace();
            throw new RMIException("Storage cannot be registered");
        }
    }

    private void recursiveList(File file, Path parent, ArrayList<Path> paths){
        File[] files = file.listFiles();
        for(File f : files){
            Path p = new Path(parent, f.getName());
            paths.add(p);
            if(f.isDirectory()){
                recursiveList(f, p, paths);
            }
        }
    }

    /** Stops the storage server.

        <p>
        The server should not be restarted.
     */
    public void stop()
    {
        clientSkeleton.stop();
        commandSkeleton.stop();
    }

    /** Called when the storage server has shut down.

        @param cause The cause for the shutdown, if any, or <code>null</code> if
                     the server was shut down by the user's request.
     */
    protected void stopped(Throwable cause)
    {
    }

    // The following methods are documented in Storage.java.
    @Override
    public synchronized long size(Path file) throws FileNotFoundException
    {
        File f = file.toFile(this.root);
        if(!f.exists() || f.isDirectory())  throw new FileNotFoundException("File not found or is a directory");
        return f.length();
    }

    @Override
    public synchronized byte[] read(Path file, long offset, int length)
        throws FileNotFoundException, IOException
    {
        if(length < 0)  throw new IndexOutOfBoundsException("Length is negative");

        File f = file.toFile(this.root);
        RandomAccessFile rf = new RandomAccessFile(f, "r"); // may throw FileNotFoundException
        rf.seek(offset);
        byte[] b = new byte[length];
        int r = rf.read(b);
        rf.close();
        if(r < length) throw new IndexOutOfBoundsException("Length exceed file boundary");
        return b;
    }

    @Override
    public synchronized void write(Path file, long offset, byte[] data)
        throws FileNotFoundException, IOException
    {
        if(offset < 0)  throw new IndexOutOfBoundsException("Offset negative");
        File f = file.toFile(this.root);
        if(!f.exists() || f.isDirectory())  throw new FileNotFoundException("File not found or is a directory");
        RandomAccessFile rf = new RandomAccessFile(f, "w");
        rf.seek(offset);

        rf.write(data); // may throw IOException
        rf.close();
    }

    // The following methods are documented in Command.java.
    @Override
    public synchronized boolean create(Path file)
    {
        Path parent = file.parent();
        File parentFile = parent.toFile(this.root);
        if(!parentFile.exists()){
            boolean createParent = parentFile.mkdirs();  // Recursively create parent
            if(!createParent)   return false;   // Cannot create parent file
        }
        // Create new file
        return createFile(file);
    }

    private boolean createFile(Path file){
        String rootPath = this.root.getAbsolutePath();
        String filePath = file.toString();
        String absPath = rootPath + filePath;
        try{
            File newFile = new File(absPath);
            newFile.createNewFile();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public synchronized boolean delete(Path path)
    {
        if(path.isRoot()) return false;   // Cannot delete root directory
        File f = path.toFile(this.root);
        boolean t1 = deleteFile(f);

        // Checking if parent becomes empty should be done in NamingServer.
//        Path parent = path.parent();
//        File parentFile = parent.toFile(this.root);
//        if(!parent.isRoot() && parentFile.listFiles().length == 0){
//            return delete(parent);
//        }
        return t1;
    }

    private boolean deleteFile(File f){
        if(!f.exists()) return true;
        if(f.isDirectory()){
            File[] fs = f.listFiles();
            for(File file : fs){
                boolean t = deleteFile(file);
                if(!t)  return false;
            }
        }
        return f.delete();
    }

    @Override
    public synchronized boolean copy(Path file, Storage server)
        throws RMIException, FileNotFoundException, IOException
    {
        long size = server.size(file);
        boolean create = create(file);
        if(!create) throw new IOException("Cannot create file");

        long remain = size;
        long offset = 0;
        try{
            while(remain > 1000){
                byte[] data = new byte[1000];
                server.read(file, offset, 1000);
                this.write(file, offset, data);
                offset += 1000;
                remain -= 1000;
            }
            if(remain > 0){
                byte[] data = new byte[(int)remain];
                server.read(file, offset, (int)remain);
                this.write(file, offset, data);
            }
        }catch (IOException e){
            this.delete(file);  // Some error occurs. Delete the file
            throw e;
        }
        return true;
    }

    public static void main(String[] args){
        StorageServer ss = new StorageServer(new File("/Users/tao/Documents/UCSD_Study/2016_Spring/cse291E00/labs/lab2/cse291e-lab2/naming/"));
        Path file = new Path("/directory/a.txt");
        ss.createFile(file);
    }
}

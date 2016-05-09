package naming;

import java.io.*;
import java.net.*;
import java.util.*;

import rmi.*;
import common.*;
import storage.*;

/** Naming server.

    <p>
    Each instance of the filesystem is centered on a single naming server. The
    naming server maintains the filesystem directory tree. It does not store any
    file data - this is done by separate storage servers. The primary purpose of
    the naming server is to map each file name (path) to the storage server
    which hosts the file's contents.

    <p>
    The naming server provides two interfaces, <code>Service</code> and
    <code>Registration</code>, which are accessible through RMI. Storage servers
    use the <code>Registration</code> interface to inform the naming server of
    their existence. Clients use the <code>Service</code> interface to perform
    most filesystem operations. The documentation accompanying these interfaces
    provides details on the methods supported.

    <p>
    Stubs for accessing the naming server must typically be created by directly
    specifying the remote network address. To make this possible, the client and
    registration interfaces are available at well-known ports defined in
    <code>NamingStubs</code>.
 */
public class NamingServer implements Service, Registration
{
    private Skeleton<Service> serviceSkeleton;
    private Skeleton<Registration> registrationSkeleton;
    private Hashtable<Path, Storage> storageTable;
    private Hashtable<Path, Command> commandTable;
    private Set<Storage> storages;
    private Set<Command> commands;
    /** Creates the naming server object.

        <p>
        The naming server is not started.
     */
    public NamingServer()
    {
        this.storageTable = new Hashtable<>();
        this.commandTable = new Hashtable<>();
        this.storages = Collections.synchronizedSet(new HashSet<Storage>());
        this.commands = Collections.synchronizedSet(new HashSet<Command>());
//        throw new UnsupportedOperationException("not implemented");
    }

    /** Starts the naming server.

        <p>
        After this method is called, it is possible to access the client and
        registration interfaces of the naming server remotely.

        @throws RMIException If either of the two skeletons, for the client or
                             registration server interfaces, could not be
                             started. The user should not attempt to start the
                             server again if an exception occurs.
     */
    public synchronized void start() throws RMIException
    {
        InetSocketAddress serviceAddress = new InetSocketAddress("127.0.0.1", NamingStubs.SERVICE_PORT);
        this.serviceSkeleton = new Skeleton<>(Service.class, this, serviceAddress);
        InetSocketAddress registrationAddress = new InetSocketAddress("127.0.0.1", NamingStubs.REGISTRATION_PORT);
        this.registrationSkeleton = new Skeleton<>(Registration.class, this, registrationAddress);

        this.serviceSkeleton.start();
        this.registrationSkeleton.start();
    }

    /** Stops the naming server.

        <p>
        This method commands both the client and registration interface
        skeletons to stop. It attempts to interrupt as many of the threads that
        are executing naming server code as possible. After this method is
        called, the naming server is no longer accessible remotely. The naming
        server should not be restarted.
     */
    public void stop()
    {
//        throw new UnsupportedOperationException("not implemented");
        this.serviceSkeleton.stop();
        this.registrationSkeleton.stop();
    }

    /** Indicates that the server has completely shut down.

        <p>
        This method should be overridden for error reporting and application
        exit purposes. The default implementation does nothing.

        @param cause The cause for the shutdown, or <code>null</code> if the
                     shutdown was by explicit user request.
     */
    protected void stopped(Throwable cause)
    {
    }

    // The following public methods are documented in Service.java.
    @Override
    public void lock(Path path, boolean exclusive) throws FileNotFoundException
    {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void unlock(Path path, boolean exclusive)
    {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public boolean isDirectory(Path path) throws FileNotFoundException, RMIException {
        Storage storage = storageTable.get(path);
        if(storage == null) throw new FileNotFoundException("File not found");
        try {
            storage.size(path);
            return false;
        } catch(FileNotFoundException e){
            return false;
        }catch (RMIException e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public String[] list(Path directory) throws FileNotFoundException, RMIException {
        // This method only lists direct children
        if(!this.isDirectory(directory))    throw new FileNotFoundException("Not a directory");

        ArrayList<String> ret = new ArrayList<>();
        for(Path p : this.storageTable.keySet()){
            if(p.equals(directory)) continue;
            if(p.isSubpath(directory))  ret.add(p.toString());
        }
        return ret.toArray(new String[0]);
    }

    @Override
    public boolean createFile(Path file)
        throws RMIException, FileNotFoundException
    {
        Path parent = file.parent();
        if(!this.storageTable.containsKey(parent)) throw new FileNotFoundException("Parent not exist");
        if(this.storages.size() == 0)   throw new IllegalStateException("No connected storage servers");
        Command command = this.commandTable.get(parent);
        return command.create(file);
    }

    @Override
    public boolean createDirectory(Path directory) throws FileNotFoundException
    {
        // This method is not finished!!!!!!!!!!! -- Tao
        Path parent = directory.parent();
        if(!this.storageTable.containsKey(parent))  throw new FileNotFoundException("Parent not exist");
        if(this.storageTable.containsKey(directory))    return false;   // Existing name
        Storage storage = this.storageTable.get(parent);
        Command command = this.commandTable.get(parent);

        // Only create the directory in the directory tree, but does not create actual folder in storage server
        this.storageTable.put(directory, storage);
        this.commandTable.put(directory, command);
        return true;
    }

    @Override
    public boolean delete(Path path) throws FileNotFoundException
    {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Storage getStorage(Path file) throws FileNotFoundException
    {
        throw new UnsupportedOperationException("not implemented");
    }

    // The method register is documented in Registration.java.
    @Override
    public Path[] register(Storage client_stub, Command command_stub,
                           Path[] files)
    {
        ArrayList<Path> toDelete = new ArrayList<>();
        for(Path f : files){
            if(storageTable.containsKey(f) || commandTable.containsKey(f)){
                toDelete.add(f);
            }else{
                storageTable.put(f, client_stub);
                commandTable.put(f, command_stub);
            }
        }
        this.storages.add(client_stub);
        this.commands.add(command_stub);
        Path[] ret = toDelete.toArray(new Path[0]);
        return ret;
    }
}

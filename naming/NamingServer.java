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
    private HashMap<Path, ArrayList<Storage>> storageTable;
    private HashMap<Path, ArrayList<Command>> commandTable;
    private Set<Storage> storages;  // Stores connected Storage stubs.
    private Set<Command> commands;  // Stores connected Command stubs.
    private Set<Path> createdDirs;  // Stores all created directories to distinguish them from files.
    private HashMap<Path, Integer> accessCount; // Stores the accessCount for each path

    private List<Pair> queue;
    /** Creates the naming server object.

        <p>
        The naming server is not started.
     */
    public NamingServer()
    {
        this.storageTable = new HashMap<>();
        this.commandTable = new HashMap<>();
        this.storages = new HashSet<Storage>();
        this.commands = new HashSet<Command>();
        this.createdDirs = new HashSet<Path>();
        this.accessCount = new HashMap<>();

        this.queue = new ArrayList<>();
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
        try{
            this.serviceSkeleton.stop();
            this.registrationSkeleton.stop();
            this.stopped(null);
        }catch (Exception e){
            this.stopped(e);
        }
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

    private void replicate(Path path){

    }


    private void incrementAccessCount(Path path){
        Integer count = this.accessCount.get(path);
        if(count == null){
            count = 1;
            this.accessCount.put(path, 1);
        }
        else    this.accessCount.put(path, ++count);
        if(count % 20 == 0){
            replicate(path);
        }
    }

    // The following public methods are documented in Service.java.
    @Override
    public synchronized void lock(Path path, boolean exclusive) throws FileNotFoundException
    {
        if(path == null)    throw new NullPointerException();

        Pair pair = new Pair(path, exclusive);
        this.queue.add(pair);

        while(true){
            if(!path.isRoot() && !this.contains(path)){ // remove from queue before throwing exception
                int index = 0;
                for(;index < this.queue.size();index++){
                    if(queue.get(index) == pair)    break;
                }
                this.queue.remove(index);

                throw new FileNotFoundException("Lock path not found");
            }
            if(!exclusive){
                int i = 0;
                boolean violate = false;
                while(i<this.queue.size()){
                    Pair current = this.queue.get(i);
                    if(current == pair) break;  // All previous requests have no conflicts

                    if(current.exclusive){
                        violate = checkViolateWithRead(current.path, path);
                        if(violate) try {
                            wait();
                            break;
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    i++;
                }
                if(!violate){
                    incrementAccessCount(path);
                    return;
                }

            }else{
                int i = 0;
                boolean violate = false;
                while(i < this.queue.size()){
                    Pair current = this.queue.get(i);
                    if(current == pair) break;

                    violate = checkViolateWithWrite(current.path, current.exclusive, path);
                    if(violate) try {
                        wait();
                        break;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    i++;
                }

                if(!violate){
                    return;
                }
            }
        }
    }

    // writePath comes before readPath
    private boolean checkViolateWithRead(Path writePath, Path readPath){
        if(writePath.equals(readPath))  return true;
        if(writePath.isSubpath(readPath))   return false;    // write /a/b/c, read /a/b
        if(readPath.isSubpath(writePath))   return true;   // write /a/b, read /a/b/c
        return false;
    }

    // path comes before writePath
    private boolean checkViolateWithWrite(Path path, boolean exclusive, Path writePath){
        if(path.equals(writePath))  return true;
        if(!exclusive){
            if(writePath.isSubpath(path))   return false;    // read /a/b, write /a/b/c
            if(path.isSubpath(writePath))   return true;    // read /a/b/c, write /a/b
            return false;
        }else{
            if(writePath.isSubpath(path))   return true;    // write /a/b, write /a/b/c
            if(path.isSubpath(writePath))   return true;    // write /a/b/c, write /a/b
            return false;
        }
    }

    @Override
    public synchronized void unlock(Path path, boolean exclusive)
    {
        if(path == null)    throw new NullPointerException();
        boolean b = this.queue.remove(new Pair(path, exclusive));
        if(!b)  throw new IllegalArgumentException("Path not found");
        notifyAll();

//        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public boolean isDirectory(Path path) throws FileNotFoundException, RMIException {
        if(path == null)    throw new NullPointerException("Input null");
        if(path.isRoot())   return true;
        lock(path, false);
        if(!contains(path)){
            unlock(path, false);
            throw new FileNotFoundException("File not found");
        }
        if(this.createdDirs.contains(path)){
            unlock(path, false);
            return true;
        }
        for(Path p : this.storageTable.keySet()){
            if(p.equals(path)){
                unlock(path, false);
                return false;
            }
            if(p.isSubpath(path) && !p.equals(path)){
                unlock(path, false);
                return true;
            }
        }
        unlock(path, false);
        return false;
    }

    // This private method is only called when the caller already has the lock.
    private boolean isDirectoryNoLock(Path path) throws FileNotFoundException {
        if(path == null)    throw new NullPointerException("Input null");
        if(path.isRoot())   return true;
        if(!contains(path)) throw new FileNotFoundException("File not found");
        if(this.createdDirs.contains(path)) return true;
        for(Path p : this.storageTable.keySet()){
            if(p.equals(path))  return false;
            if(p.isSubpath(path) && !p.equals(path))   return true;
        }
        return false;
    }

    @Override
    public String[] list(Path directory) throws FileNotFoundException, RMIException {
        // This method only lists direct children
        if(directory == null)    throw new NullPointerException("Input null");
        if(!this.isDirectoryNoLock(directory))    throw new FileNotFoundException("Not a directory");

        HashSet<String> ret = new HashSet<>();
        for(Path p : this.storageTable.keySet()){
            if(p.isSubpath(directory) && !p.equals(directory)){
                ret.add(p.getDirectChild(directory));
            }
        }
        return ret.toArray(new String[0]);
    }


    @Override
    public boolean createFile(Path file)
        throws RMIException, FileNotFoundException
    {
        if(file == null)    throw new NullPointerException("Input null");

        if(this.contains(file)) return false;   // Existing file name
        Path parent = file.parent();
        if(!this.isDirectoryNoLock(parent))   throw new FileNotFoundException("Parent is a file");
        Storage storage = this.getDirStorage(parent);
        if(storage == null) throw new FileNotFoundException("Parent not exist");
        if(this.storages.size() == 0)   throw new IllegalStateException("No connected storage servers");
        Command command = this.getDirCommand(parent);
        boolean b = command.create(file);
        if(!b)  return b;
        this.addToStorageMap(file, storage);
        this.addToCommandMap(file, command);
//        this.storageTable.put(file, storage);
//        this.commandTable.put(file, command);
        return b;
    }

    @Override
    public boolean createDirectory(Path directory) throws FileNotFoundException, RMIException {
        if(directory == null)    throw new NullPointerException("Input null");

        if(directory.isRoot())  return false;
        Path parent = directory.parent();
        if(!this.isDirectoryNoLock(parent))   throw new FileNotFoundException("Parent is a file");
        if(this.contains(directory))    return false;   // Existing name
        Storage storage = this.getDirStorage(parent);
        Command command = this.getDirCommand(parent);

        // Only create the directory in the directory tree, but does not create actual folder in storage server
        this.addToStorageMap(directory, storage);
        this.addToCommandMap(directory, command);
//        this.storageTable.put(directory, storage);
//        this.commandTable.put(directory, command);
        this.createdDirs.add(directory);
        return true;
    }

    // This method is not tested in the checkpoint!
    @Override
    public boolean delete(Path path) throws FileNotFoundException, RMIException {
        if(path == null)    throw new NullPointerException("Input null");

        if(path.isRoot())   return false;
        if(!this.contains(path))    throw new FileNotFoundException("Path not found");
        Command command = this.getDirCommand(path);

        boolean b = command.delete(path);
        if(!b)  return b;
        if(this.isDirectoryNoLock(path)){ // If path is directory, all children are also deleted from the tree.
            Iterator<Map.Entry<Path, ArrayList<Command>>> iterator = this.commandTable.entrySet().iterator();
            while(iterator.hasNext()){
                Map.Entry<Path, ArrayList<Command>> entry = iterator.next();
                Path key = entry.getKey();
                if(key.isSubpath(path)){
                    this.storageTable.remove(key);
                    iterator.remove();
                    if(this.createdDirs.contains(key))  this.createdDirs.remove(key);
                }
            }
        }
        this.commandTable.remove(path);
        this.storageTable.remove(path);
        if(this.createdDirs.contains(path)) this.createdDirs.remove(path);

        // May need to delete empty parents here!
        return b;
    }


    private Command getDirCommand(Path file) throws FileNotFoundException{
        for(Path p : this.commandTable.keySet()){
            if(p.isSubpath(file)){
                ArrayList<Command> commandList = this.commandTable.get(p);
                if(commandList.size()==0)   throw new FileNotFoundException("Path not found");
                return commandList.get(0);  // It may return any element of the list.
            }
        }
        throw new FileNotFoundException("Path not found");
    }

    private Storage getDirStorage(Path dir) throws FileNotFoundException{
        for(Path p : storageTable.keySet()){
            if(p.isSubpath(dir)){
                ArrayList<Storage> storageList = this.storageTable.get(p);
                if(storageList.size() == 0) throw new FileNotFoundException("Path not found");
                return storageList.get(0);  // It may return any element of the list.
            }
        }
        throw new FileNotFoundException("Path not found");
    }

    @Override
    public Storage getStorage(Path file) throws FileNotFoundException
    {
        if(file==null)  throw new NullPointerException();
        ArrayList<Storage> storageList = this.storageTable.get(file);
        if(storageList==null || storageList.size()==0)   throw new FileNotFoundException("Path not found");
        return storageList.get(0);  // It may return any element of the list.
    }


    // check if path exists
    private boolean contains(Path path){
        for(Path p: this.storageTable.keySet()){
            if(p.isSubpath(path))   return true;
        }
        return false;
    }

    private void addToStorageMap(Path f, Storage client_stub){
        ArrayList<Storage> storageList = this.storageTable.get(f);
        if(storageList == null){
            storageList = new ArrayList<>();
            storageList.add(client_stub);
            this.storageTable.put(f, storageList);
        }else   storageList.add(client_stub);
    }

    private void addToCommandMap(Path f, Command command_stub){
        ArrayList<Command> commandList = this.commandTable.get(f);
        if(commandList == null){
            commandList = new ArrayList<>();
            commandList.add(command_stub);
            this.commandTable.put(f, commandList);
        }else   commandList.add(command_stub);
    }

    // The method register is documented in Registration.java.
    @Override
    public Path[] register(Storage client_stub, Command command_stub,
                           Path[] files)
    {
        if(client_stub==null || command_stub==null || files==null) throw new NullPointerException();


        if(this.storages.contains(client_stub) || this.commands.contains(command_stub))
            throw new IllegalStateException("Already registered");

        ArrayList<Path> toDelete = new ArrayList<>();
        for(Path f : files){
            if(this.contains(f) && !f.isRoot()){
                toDelete.add(f);
            }else{
                this.addToStorageMap(f, client_stub);
                this.addToCommandMap(f, command_stub);
//                storageTable.put(f, client_stub);
//                commandTable.put(f, command_stub);
            }
        }
        this.storages.add(client_stub);
        this.commands.add(command_stub);
        Path[] ret = toDelete.toArray(new Path[0]);
        return ret;
    }

    private class Pair{
        Path path;
        boolean exclusive;

        private Pair(Path p, boolean b){
            path = p;
            exclusive = b;
        }

        @Override
        public boolean equals(Object obj) {
            if(obj == null) return false;
            if(obj instanceof Pair){
                Pair o = (Pair) obj;
                return path.equals(o.path) && exclusive == o.exclusive;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return path.hashCode();
        }
    }

}

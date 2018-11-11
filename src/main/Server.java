package com.robaho.jnatsd;

import com.robaho.jnatsd.util.ChannelIO;
import com.robaho.jnatsd.util.CharSeq;
import com.robaho.jnatsd.util.JSON;
import com.robaho.jnatsd.util.RingBuffer;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Server {
    private int port;
    private Thread listener, flusher, io;
    private Set<Connection> connections = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private Set<Connection> toregister = new HashSet();
    Logger logger = Logger.getLogger("server");

    private int maxMsgSize = 1024*1024;

    public Server(int port) {
        this.port = port;
    }

    private volatile Subscription[] subs = new Subscription[0];
    private volatile Map<CharSeq, SubscriptionMatch> cache = new ConcurrentHashMap();
    private AtomicInteger clientIDs = new AtomicInteger(0);
    private boolean tlsRequired;
    private final RingBuffer<IOFuture> wqueue = new RingBuffer<>(1024);
    private volatile boolean done;

    static final long spinForTimeoutThreshold = 1000L;

    public boolean isTLSRequired() {
        return tlsRequired;
    }

    public int getMaxMsgSize() {
        return maxMsgSize;
    }

    private Selector selector;

    public void bgWrite(Connection c) {
        IOFuture f = new IOFuture();
        c.writeRequest = f;

        synchronized(c.ch) {
            try {
                ChannelIO.write(c.fd,c.wBuffer0);
//                c.ch.write(c.wBuffer0);
                if (c.wBuffer0.remaining()==0) {
                    f.setResult(false);
                    return;
                }
            } catch (Exception e) {
                closeConnection(c);
            }
        }

        synchronized(c) {
            SelectionKey key = c.ch.keyFor(selector);
            if(key==null || !key.isValid()){
                f.setResult(true);
            } else {
                if((key.interestOps()& SelectionKey.OP_WRITE)!=0){
                    return;
                }
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            }
        }
        selector.wakeup();
    }

    public void bgRead(Connection c) {
        IOFuture f = new IOFuture();
        c.readRequest = f;

        synchronized(c.ch) {
            try {
//                int n = c.ch.read(c.rBuffer0);
                int n = ChannelIO.read(c.fd,c.rBuffer0);
                if (n > 0) {
                    f.setResult(false);
                    return;
                }
            } catch (Exception e) {
                closeConnection(c);
            }
        }

        synchronized(c) {
            SelectionKey key = c.ch.keyFor(selector);
            if(key==null || !key.isValid()){
                f.setResult(true);
            } else {
                if((key.interestOps()& SelectionKey.OP_READ)!=0){
                    return;
                }
                key.interestOps(key.interestOps() | SelectionKey.OP_READ);
            }
        }
        selector.wakeup();
    }

    private static class SubscriptionMatch {
        long lastUsed; // for LRU cache purge
        CharSeq subject;
        Subscription[] subs;
        Map<CharSeq, List<Subscription>> groups = new HashMap<>();
    }

    private final AtomicBoolean writerSync = new AtomicBoolean();

    private class IOFuture implements Future<Boolean> {
        Boolean result;

        IOFuture(){
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return result!=null;
        }

        @Override
        public synchronized Boolean get() throws InterruptedException, ExecutionException {
            while(result==null){
                wait();
            }
            return result;
        }

        @Override
        public Boolean get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            while(result==null){
                wait(unit.toMillis(timeout));
            }
            return result;
        }

        synchronized void setResult(Boolean b){
            result=b;
            notifyAll();
        }
    }

    public void start() throws IOException {

        selector = Selector.open();

        ServerSocketChannel socket = ServerSocketChannel.open().bind(new InetSocketAddress(port),128);

        listener = new Thread("Listener") {
            public void run() {
                while (!done) {
                    try {
                        SocketChannel s = socket.accept();
                        s.configureBlocking(false);
                        logger.info("Connection from " + s.getRemoteAddress());
                        Connection c = new Connection(Server.this, s);

                        synchronized (toregister){
                            toregister.add(c);
                        }
                        selector.wakeup();

                    } catch (Exception e) {
                        logger.log(Level.SEVERE,"acceptor failed",e);
                    }
                }

            }
        };
        listener.start();

        flusher = new Thread(new Flusher(),"Flusher");
        flusher.start();

        io = new Thread(new ReaderWriter(),"ReaderWriter");
        io.start();
    }

    /**
     * used to background flush connections that need them. pretty dumb at this point.
     */
    private class Flusher implements Runnable {
        public void run() {
            while (!done) {
                long now = System.nanoTime();
                for(Connection c : connections) {
                    long nanos = c.lastWrite;
                    if(nanos!=0 && now-nanos > 1000*1000*10) {
                        try {
                            c.flush();
                        } catch (IOException e) {
                            logger.log(Level.SEVERE,"unable to flush",e);
                            closeConnection(c);
                        }
                    }
                }
                if(connections.isEmpty())
                    LockSupport.park();
                else {
                    LockSupport.parkNanos(1000 * 1000 * 10);
                }
            }
        }
    }

    /**
     * background writes (flushes) data to socket
     */
    private class ReaderWriter implements Runnable {
        private void addConnections(){
            if(toregister.isEmpty())
                return;
            synchronized (toregister){
                for(Connection c : toregister){
                    try {
                        c.ch.register(selector, 0, c);
                        synchronized (connections) {
                            connections.add(c);
                            LockSupport.unpark(flusher);
                        }
                        c.processConnection();
                    } catch(IOException e){
                        closeConnection(c);
                    }
                }
                toregister.clear();
            }
        }
        public void run() {

            while(!done) {
                try {
                    addConnections();
                    selector.select();
                    for(SelectionKey key : selector.selectedKeys()) {
                        Connection c = (Connection) key.attachment();
                        try {
                            if (key.isValid() && key.isReadable()) {
                                synchronized (c.ch) {
                                    IOFuture f = (IOFuture) c.readRequest;
                                    if (f.isDone())
                                        continue;
                                    int result = ChannelIO.read(c.fd, c.rBuffer0);
                                    // int result = c.ch.read(c.rBuffer0);
                                    if (result < 0) {
                                        f.setResult(true);
                                        key.cancel();
                                    } else {
                                        if (result > 0) {
                                            key.interestOps(key.interestOps() & (~SelectionKey.OP_READ));
                                            f.setResult(false);
                                        }
                                    }
                                }
                            }
                            if (key.isValid() && key.isWritable()) {
                                IOFuture f = (IOFuture) c.writeRequest;
                                if (f.isDone())
                                    continue;
                                //                            c.ch.write(c.wBuffer0);
                                ChannelIO.write(c.fd, c.wBuffer0);
                                if (!c.wBuffer0.hasRemaining()) {
                                    key.interestOps(key.interestOps() & (~SelectionKey.OP_WRITE));
                                    f.setResult(false);
                                }
                            }
                        } catch(IOException e){
                            closeConnection(c);
                        }
                        if(c.closed()){
                            ((IOFuture)c.writeRequest).setResult(true);
                            ((IOFuture)c.readRequest).setResult(true);
                        }
                    }
                } catch (IOException e) {
                    logger.log(Level.SEVERE,"select failed",e);
                }
            }
        }
    }



    public int getNextClientID() {
        return clientIDs.incrementAndGet();
    }

    void routeMessage(InMessage m) {
//        System.out.println("received message "+m);

        Map<CharSeq, SubscriptionMatch> _cache = cache;

        SubscriptionMatch cached = _cache.get(m.subject);
        if (cached != null) {
            routeToMatch(m,cached);
            return;
        }

        cached = buildSubscriptionMatch(m.subject);
        SubscriptionMatch old = _cache.putIfAbsent(cached.subject,cached);
        if(old!=null)
            cached=old;
        routeToMatch(m,cached);
    }

    private SubscriptionMatch buildSubscriptionMatch(CharSeq subject) {
        SubscriptionMatch match = new SubscriptionMatch();

        match.subject = subject;

        Map<CharSeq, List<Subscription>> groups = new HashMap<>();

        Subscription[] _subs = subs;

        Set<Subscription> set = new HashSet();

        // TODO rather than linear search, since the subscriptions are sorted, a log(n) search
        // can be used to find the next match
        // TODO maybe store all connections for the same 'subscription' in a list by subscription
        // to reduce the search comparisons, gets more complex with groups though

        Subscription s = new Subscription(null, 0, match.subject, CharSeq.EMPTY);
        for (int i = 0; i < _subs.length; i++) {
            Subscription sub = _subs[i];
            if (!sub.matches(s)) {
                continue;
            }
            if (!sub.group.equals(CharSeq.EMPTY)) {
                List<Subscription> gsubs = groups.get(sub.group);
                if (gsubs == null) {
                    gsubs = new ArrayList<>();
                    groups.put(sub.group, gsubs);
                }
                gsubs.add(sub);
                continue;
            } else {
                set.add(sub);
            }
        }

        match.groups = groups;
        match.subs = set.toArray(new Subscription[set.size()]);

        return match;
    }

    private void routeToMatch(InMessage msg,SubscriptionMatch match) {
        match.lastUsed = System.currentTimeMillis();

        final Connection from = msg.connection;

        for (Subscription s : match.subs) {
            if(s.connection==from && from.isEcho())
                continue;
            s.connection.sendMessage(s, msg);
            from.wroteTo.add(s.connection);
        }

        if(match.groups.isEmpty())
            return;

        for (Map.Entry<CharSeq, List<Subscription>> group : match.groups.entrySet()) {
            List<Subscription> subs = group.getValue();
            Subscription gs = subs.get((int) System.currentTimeMillis() % subs.size());
            gs.connection.sendMessage(gs, msg);
            from.wroteTo.add(gs.connection);
        }
    }

    public void stop() throws InterruptedException {
        done=true;

        listener.interrupt();
        listener.join();
    }

    public void waitTillDone() throws InterruptedException {
        listener.join();
    }

    public void closeConnection(Connection connection) {
        synchronized (connections) {
            connections.remove(connection);
            ArrayList<Subscription> copy = new ArrayList<>();
            for (Subscription s : subs) {
                if (s.connection != connection) {
                    copy.add(s);
                }
            }
            subs = copy.toArray(new Subscription[copy.size()]);
            cache = new ConcurrentHashMap<>();
        }
        connection.close();
        logger.info("connection terminated " + connection.getRemote());
    }

    public String getInfoAsJSON(Connection connection) {
        ServerInfo info = new ServerInfo();
        info.client_id = connection.getClientID();
        info.tls_required = tlsRequired;
        return "INFO " + JSON.save(info) +"\r\n";
    }

    public void addSubscription(Subscription toAdd) {

        synchronized (connections) {
            ArrayList<Subscription> copy = new ArrayList<>();
            for (Subscription s : subs) {
                if (s.connection == toAdd.connection && s.ssid == toAdd.ssid) {
                    continue;
                } else {
                    copy.add(s);
                }
            }
            copy.add(toAdd);
            subs = copy.toArray(new Subscription[copy.size()]);
            cache = new ConcurrentHashMap<>();
        }
    }

    public void removeSubscription(Subscription toRemove) {
        synchronized (connections) {
            ArrayList<Subscription> copy = new ArrayList<>();
            for (Subscription s : subs) {
                if (s.connection == toRemove.connection && s.ssid == toRemove.ssid) {
                    continue;
                } else {
                    copy.add(s);
                }
            }
            subs = copy.toArray(new Subscription[copy.size()]);
            cache = new ConcurrentHashMap<>();
        }
    }

    public static class ServerInfo {
        public String server_id = "8hPWDls9DjyXnFVf1AjnQq";
        public String version = "1.3.1";
        public int proto = 1;
        public String java = "jdk1.8";
        public String host = "0.0.0.0";
        public int port = 4222;
        public int max_payload = 1048576;
        public boolean auth_required;
        public boolean tls_required;
        public boolean tls_verify;
        public int client_id = 1;
        private ServerInfo(){}
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        long start = System.nanoTime();
        for(int i=0;i<10000000;i++) {
            ChannelIO.dummy(0,0,0);
        }
        long diff = System.nanoTime()-start;
        System.out.println("time = "+(diff/10000000));
        Server server = new Server(4222);
        for(String s : args){
            if("--tls".equals(s)){
                server.tlsRequired=true;
            }
        }
        server.start();
        server.waitTillDone();
    }
}

package com.robaho.jnatsd;

import com.robaho.jnatsd.util.CharSeq;
import com.robaho.jnatsd.util.JSON;
import com.robaho.jnatsd.util.RingBuffer;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Server {
    private int port;
    private Thread listener, handler, flusher;
    private Set<Connection> connections = Collections.newSetFromMap(new ConcurrentHashMap<>());
    Logger logger = Logger.getLogger("server");

    private int maxMsgSize = 1024*1024;

    public Server(int port) {
        this.port = port;
    }

    private volatile Subscription[] subs = new Subscription[0];
    private volatile Map<CharSeq, SubscriptionMatch> cache = new ConcurrentHashMap();
    private AtomicInteger clientIDs = new AtomicInteger(0);
    private boolean tlsRequired;
    private final RingBuffer<InMessage> queue = new RingBuffer<>(64*1024);
    private volatile boolean done;

    static final long spinForTimeoutThreshold = 1000L;

    public boolean isTLSRequired() {
        return tlsRequired;
    }

    public int getMaxMsgSize() {
        return maxMsgSize;
    }

    private static class SubscriptionMatch {
        long lastUsed; // for LRU cache purge
        CharSeq subject;
        Subscription[] subs;
        Map<CharSeq, List<Subscription>> groups = new HashMap<>();
    }

    private final AtomicBoolean handlerSync = new AtomicBoolean();

    public void start() throws IOException {
        ServerSocketChannel socket = ServerSocketChannel.open().bind(new InetSocketAddress(port),128);

        listener = new Thread("Listener") {
            public void run() {
                while (!done) {
                    try {
                        SocketChannel s = socket.accept();

                        logger.info("Connection from " + s.getRemoteAddress());
                        Connection c = new Connection(Server.this, s.socket());
                        synchronized (connections) {
                            connections.add(c);
                        }
                        c.processConnection();
                    } catch (IOException e) {
                        logger.log(Level.SEVERE,"acceptor failed",e);
                    }
                }

            }
        };
        listener.start();


        flusher = new Thread("Flusher") {
            public void run() {
                long flushes=0;
                while (!done) {
                    long now = System.nanoTime();
                    for(Connection c : connections) {
                        long nanos = c.lastWrite;
                        if(nanos!=0 && now-nanos > 500*1000) {
                            try {
                                c.flush();
                                flushes++;
                                if(flushes%10000==0){
                                    System.out.println("flushes "+flushes);
                                }
                            } catch (IOException e) {
                                logger.log(Level.SEVERE,"unable to flush",e);
                                closeConnection(c);
                            }
                        }
                    }
                    LockSupport.parkNanos(500*1000);
                }
            }
        };
        flusher.start();

        handler = new Thread(new MessageRouter(),"MessageRouter");
        handler.start();
    }

    /**
     * routes 'in' messages to subscribed connections
     */
    private class MessageRouter implements Runnable {
        long routed =0;
        public void run() {
            InMessage m;
            while (!done) {
                long deadline = 0;
                while((m=queue.poll())==null){
                    if(deadline==0)
                        deadline=System.nanoTime()+spinForTimeoutThreshold*10000;
                    if(System.nanoTime()>deadline) {
                        if(handlerSync.compareAndSet(true,false))
                            continue;
                        LockSupport.park();
                    }
                }
                routed++;
                routeMessage(m);
            }
        }
    }

    public int getNextClientID() {
        return clientIDs.incrementAndGet();
    }

    long waits;
    void queueMessage(InMessage m){
//        routeMessage(m);
        long deadline=System.nanoTime()+spinForTimeoutThreshold;
        long backoff=1;
        while(!queue.offer(m)){
            if(System.nanoTime()>deadline){
                LockSupport.parkNanos(1000*(backoff*=2));
            }
            backoff=Math.min(backoff,1000);
        }
        if(handlerSync.compareAndSet(false,true)) {
            LockSupport.unpark(handler);
        }
    }

    private void routeMessage(InMessage m) {
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
        }

        if(match.groups.isEmpty())
            return;

        for (Map.Entry<CharSeq, List<Subscription>> group : match.groups.entrySet()) {
            List<Subscription> subs = group.getValue();
            Subscription gs = subs.get((int) System.currentTimeMillis() % subs.size());
            gs.connection.sendMessage(gs, msg);
        }
    }

    public void stop() throws InterruptedException {
        done=true;

        listener.interrupt();
        listener.join();
        handler.interrupt();
        handler.join();
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

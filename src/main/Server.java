package com.robaho.jnatsd;

import com.robaho.jnatsd.util.CharSeq;
import com.robaho.jnatsd.util.JSON;
import com.robaho.jnatsd.util.RingBuffer;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Server {
    private int port;
    private Thread listener, handler, flusher;
    private Set<Connection> connections = new CopyOnWriteArraySet<>();
    Logger logger = Logger.getLogger("server");

    private int maxMsgSize = 1024*1024;

    public Server(int port) {
        this.port = port;
    }

    private volatile Subscription[] subs = new Subscription[0];
    private volatile Map<CharSeq, SubscriptionMatch> cache = new ConcurrentHashMap();
    private AtomicInteger clientIDs = new AtomicInteger(0);
    private boolean tlsRequired;
    private volatile boolean done;

    private final AtomicBoolean flushPermit = new AtomicBoolean();

    public boolean isTLSRequired() {
        return tlsRequired;
    }

    public int getMaxMsgSize() {
        return maxMsgSize;
    }

    public void needsFlush(Connection connection) {
        if(flushPermit.compareAndSet(false,true))
            LockSupport.unpark(flusher);
    }

    private static class SubscriptionMatch {
        long lastUsed; // for LRU cache purge
        CharSeq subject;
        Subscription[] subs;
        Map<CharSeq, List<Subscription>> groups = new HashMap<>();
    }

    private class Listener implements Runnable {
        public void run() {
            ServerSocketChannel socket = null;
            try {
                socket = ServerSocketChannel.open();
                socket.bind(new InetSocketAddress(port),256);
            } catch (IOException e) {
                logger.log(Level.SEVERE,"unable to open server socket",e);
                return;
            }

            while (!done) {
                try {
                    Socket s = socket.accept().socket();
                    s.getChannel().configureBlocking(true);
                    logger.info("Connection from " + s.getRemoteSocketAddress());
                    Connection c = new Connection(Server.this, s);
                    connections.add(c);
                    c.processConnection();
                } catch (IOException e) {
                    logger.log(Level.WARNING,"acceptor failed",e);
                }
            }
        }
    }

    public void start() throws IOException {

        logger.setLevel(Level.WARNING);

        listener = new Thread(new Listener(),"Listener");
        listener.start();

        flusher = new Thread(new Flusher(),"Flusher");
        flusher.start();
    }

    private class Flusher implements Runnable {
        @Override
        public void run() {
            while(!done) {
                boolean delayPark = false;
                for(Connection c : connections) {
                    if(!c.maybeFlush()) {
                        delayPark=true;
                    }
                }
                if(delayPark) {
                    LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(500));
                } else {
                    if (flushPermit.compareAndSet(true, false))
                        continue;
                    LockSupport.park();
                }
            }
        }
    }

    public int getNextClientID() {
        return clientIDs.incrementAndGet();
    }

    void queueMessage(InMessage m){
        routeMessage(m);
    }

    private long lastSlowWarning;
    private void routeMessage(InMessage m) {
        try {
//        System.out.println("received message "+m);

            Map<CharSeq, SubscriptionMatch> _cache = cache;

            SubscriptionMatch cached = _cache.get(m.subject);
            if (cached != null) {
                routeToMatch(m, cached);
                return;
            }

            cached = buildSubscriptionMatch(m.subject);
            SubscriptionMatch old = _cache.putIfAbsent(cached.subject, cached);
            if (old != null)
                cached = old;
            routeToMatch(m, cached);
        } finally {
            long now = System.currentTimeMillis();
            long time = now-m.when;
            if(time>2000 && now-lastSlowWarning>5000) {
                logger.log(Level.WARNING,"too long "+time+" ms to process message");
                lastSlowWarning=now;
            }
        }
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
        flusher.interrupt();
        flusher.join();
    }

    public void waitTillDone() throws InterruptedException {
        listener.join();
    }

    public void closeConnection(Connection connection) {
        synchronized (connections) {
            if(!connections.remove(connection))
                return;
            ArrayList<Subscription> copy = new ArrayList<>();
            for (Subscription s : subs) {
                if (s.connection != connection) {
                    copy.add(s);
                }
            }
            subs = copy.toArray(new Subscription[copy.size()]);
            cache = new ConcurrentHashMap<>();
        }
        // call connection.close() from background thread since, to
        // avoid deadlock with reader/writer join()
        ForkJoinPool.commonPool().execute(new Runnable() {
            @Override
            public void run() {
                connection.close();
            }
        });
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

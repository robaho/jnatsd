package com.robaho.jnatsd;

import com.robaho.jnatsd.util.CharSeq;
import com.robaho.jnatsd.util.JSON;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class Server {
    private int port;
    private Thread listener, io;
    private Set<Connection> connections = Collections.newSetFromMap(new ConcurrentHashMap<>());
    Logger logger = Logger.getLogger("server");

    public Server(int port) {
        this.port = port;
    }

    private volatile Subscription[] subs = new Subscription[0];
    private volatile Map<CharSeq, SubscriptionMatch> cache = new ConcurrentHashMap();
    private AtomicInteger clientIDs = new AtomicInteger(0);
    private boolean tlsRequired;

    public boolean isTLSRequired() {
        return tlsRequired;
    }

    private static class SubscriptionMatch {
        long lastUsed; // for LRU cache purge
        CharSeq subject;
        Subscription[] subs;
        Map<CharSeq, List<Subscription>> groups = new HashMap<>();
    }

    public void start() throws IOException {
        ServerSocket socket = new ServerSocket(port);

        listener = new Thread("Listener") {
            public void run() {
                while (listener != null) {
                    try {
                        Socket s = socket.accept();

                        logger.info("Connection from " + s.getRemoteSocketAddress());
                        synchronized (connections) {
                            Connection c = new Connection(Server.this, s);
                            connections.add(c);
                            c.processConnection();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            }
        };
        listener.start();
    }

    public int getNextClientID() {
        return clientIDs.incrementAndGet();
    }

    void processMessage(Connection from, CharSeq subject, CharSeq reply, byte[] data, int datalen) {
//        System.out.println("received message on "+subject+", reply "+reply+", "+new String(data));

        Map<CharSeq, SubscriptionMatch> _cache = cache;

        SubscriptionMatch cached = _cache.get(subject);
        if (cached != null) {
            processMessage(from,cached, subject, reply, data, datalen);
            return;
        }

        cached = buildSubscriptionMatch(subject);
        _cache.put(cached.subject,cached);
        processMessage(from,cached, subject, reply, data, datalen);
    }

    private SubscriptionMatch buildSubscriptionMatch(CharSeq subject) {
        SubscriptionMatch match = new SubscriptionMatch();

        match.subject = subject.dup();

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
                    groups.put(sub.group.dup(), gsubs);
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

    private void processMessage(Connection from,SubscriptionMatch match, CharSeq subject, CharSeq reply, byte[] data, int datalen) {
        match.lastUsed = System.currentTimeMillis();

        byte[] data2 = null;

        for (Subscription s : match.subs) {
            if(s.connection==from && from.isEcho())
                continue;
            if(data2==null) {
                data2 = Arrays.copyOf(data,datalen);
                subject = subject.dup();
                reply = reply.dup();
            }
            s.connection.sendMessage(s, subject, reply, data);
        }

        if(match.groups.isEmpty())
            return;

        for (Map.Entry<CharSeq, List<Subscription>> group : match.groups.entrySet()) {
            List<Subscription> subs = group.getValue();
            Subscription gs = subs.get((int) System.currentTimeMillis() % subs.size());
            if(data2==null) {
                data2 = Arrays.copyOf(data,datalen);
                subject = subject.dup();
                reply = reply.dup();
            }
            gs.connection.sendMessage(gs, subject, reply, data);
        }
    }

    public void stop() throws InterruptedException {
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

    public void addSubscription(Subscription s) {
        synchronized (connections) {
            Subscription[] copy = new Subscription[subs.length + 1];
            System.arraycopy(subs, 0, copy, 0, subs.length);
            copy[subs.length] = s;
            Arrays.sort(copy);
            subs = copy;
            cache = new ConcurrentHashMap<>();
        }
    }

    public void removeSubscription(Subscription s) {
        synchronized (connections) {
            Subscription[] copy = new Subscription[subs.length - 1];
            int dstI = 0;
            for (int i = 0; i < subs.length; i++) {
                if (s.connection == subs[i].connection && s.ssid == subs[i].ssid)
                    continue;
                copy[dstI++] = subs[i];
            }
            subs = copy;
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

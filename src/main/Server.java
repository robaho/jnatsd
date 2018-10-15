package com.robaho.jnatsd;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class Server {
    private int port;
    private Thread listener, io;
    private volatile boolean stopped;
    private Set<Connection> connections = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private Logger logger = Logger.getLogger("server");

    public Server(int port) {
        this.port = port;
    }

    private volatile Subscription[] subs = new Subscription[0];
    private volatile Map<String,SubscriptionMatch> cache = new ConcurrentHashMap();

    private static class SubscriptionMatch {
        Set<Subscription> subs = new HashSet<>();
        Map<String,List<Subscription>> groups = new HashMap<>();
    }

    public void start() throws IOException {
        ServerSocket socket = new ServerSocket(port);

        listener = new Thread("Listener") {
            public void run() {
                while (listener != null) {
                    try {
                        Socket s = socket.accept();

                        System.out.println("Connection from " + s.getRemoteSocketAddress());
                        synchronized (connections) {
                            Connection c = new Connection(Server.this,s);
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

    void processMessage(String subject, String reply, byte[] data) {
        byte[] replyb = reply.getBytes();

//        System.out.println("received message on "+subject+", reply "+reply+", "+new String(data));

        Map<String,SubscriptionMatch> _cache = cache;

        SubscriptionMatch cached = _cache.get(subject);
        if(cached!=null){
            processMessage(cached,subject,replyb,data);
            return;
        }

        cached = new SubscriptionMatch();

        Map<String,List<Subscription>> groups = new HashMap<>();

        Subscription[] _subs = subs;
        Subscription s = new Subscription(null,0,subject,"");
        for(int i=0;i<_subs.length;i++) {
            Subscription sub = _subs[i];
            if(!sub.matches(s)) {
                continue;
            }
            if(!"".equals(sub.group)){
                List<Subscription> gsubs = groups.get(sub.group);
                if(gsubs==null) {
                    gsubs = new ArrayList<>();
                    groups.put(sub.group,gsubs);
                }
                gsubs.add(sub);
                continue;
            } else {
                cached.subs.add(sub);
            }
        }

        cached.groups=groups;
        _cache.put(subject,cached);
        processMessage(cached,subject,replyb,data);
    }

    private void processMessage(SubscriptionMatch match, String subject, byte[] replyb, byte[] data) {
        for(Subscription s : match.subs){
            try {
                s.connection.sendMessage(s,subject,replyb,data);
            } catch (IOException e) {
                closeConnection(s.connection);
            }
        }
        for(Map.Entry<String,List<Subscription>> group : match.groups.entrySet()){
            List<Subscription> subs = group.getValue();
            Subscription gs = subs.get((int) System.currentTimeMillis() % subs.size());
            try {
                gs.connection.sendMessage(gs, subject, replyb, data);
            } catch (IOException e) {
                closeConnection(gs.connection);
            }
        }
    }

    public void stop() throws InterruptedException {
        this.stopped = true;
        listener.interrupt();
        listener.join();
    }

    public void waitTillDone() throws InterruptedException {
        listener.join();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Server server = new Server(4222);
        server.start();
        server.waitTillDone();
    }

    public void closeConnection(Connection connection) {
        synchronized (connections) {
            connections.remove(connection);
            ArrayList<Subscription> copy = new ArrayList<>();
            for(Subscription s : subs){
                if(s.connection!=connection){
                    copy.add(s);
                }
            }
            subs = copy.toArray(new Subscription[copy.size()]);
            cache = new ConcurrentHashMap<>();
        }
        System.out.println("connection terminated "+connection.getRemote());
    }

    public String getInfoAsJSON() {
        // TODO real server info object
        return "INFO {\"server_id\":\"8hPWDls9DjyXnFVf1AjnQq\",\"version\":\"1.3.1\",\"proto\":1,\"java\":\"jdk1.8\",\"host\":\"0.0.0.0\",\"port\":4222,\"max_payload\":1048576,\"client_id\":1}\r\n";
    }

    public void addSubscription(Subscription s) {
        synchronized(connections) {
            Subscription[] copy = new Subscription[subs.length+1];
            System.arraycopy(subs,0,copy,0,subs.length);
            copy[subs.length]=s;
            Arrays.sort(copy);
            subs=copy;
            cache = new ConcurrentHashMap<>();
        }
    }
}

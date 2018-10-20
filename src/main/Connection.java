package com.robaho.jnatsd;

import com.robaho.jnatsd.util.JSON;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

class Connection {
    private BufferedInputStream r;
    private BufferedOutputStream w;
    private final Server server;
    private final Socket socket;
    private final String remote;
    private boolean closed;
    private int clientID;
    private boolean verbose;

    public Connection(Server server,Socket s) throws IOException {
        this.socket=s;
        this.server=server;

        clientID = server.getNextClientID();

        remote = s.getRemoteSocketAddress().toString();

        r = new BufferedInputStream(s.getInputStream());
        w = new BufferedOutputStream(s.getOutputStream());

        w.write(server.getInfoAsJSON(this).getBytes());
        w.flush();
    }
    void processConnection(){
        Thread processor = new Thread("Processor("+socket.getRemoteSocketAddress()+")"){
            public void run() {
                while(true) {
                    try {
                        readMessages();
                    } catch (IOException e) {
                        server.closeConnection(Connection.this);
                        break;
                    }
                }
            }
        };
        processor.start();
    }
    private void readMessages() throws IOException {
        for (String line; (line = readLine(r)) != null; ) {
            try {
                processLine(line);
            } catch(Exception e){
                sendError(e);
                e.printStackTrace();
            }
        }
    }
    private void processLine(String line) throws IOException {
        int index=1;
        System.out.println("rec: " + line);
        String[] segs = line.split("\\s+");
        segs[0]=segs[0].toUpperCase();
        if ("PUB".equals(segs[0])) {
            String subject = segs[index++];
            String reply = "";
            if (segs.length == 4) {
                reply = segs[index++];
            }
            int len = Integer.parseInt(segs[index]);
            byte[] data = new byte[len];
            readPayload(r, data);
            server.processMessage(subject, reply, data);
        } else if ("PING".equals(segs[0])){
            w.write("PONG\r\n".getBytes());
            w.flush();
        } else if ("SUB".equals(segs[0])) {
            String subject = segs[index++];
            String group = "";
            if(segs.length==4) { // we have a group
                group = segs[index++];
            }
            int ssid = Integer.parseInt(segs[index]);
            addSubscription(subject, group, ssid);
        } else if("UNSUB".equals(segs[0])){
            int ssid = Integer.parseInt(segs[1]);
            removeSubscription(ssid);
        } else if("CONNECT".equals(segs[0])){
            processConnectionOptions(segs[1]);
        }
    }

    private void processConnectionOptions(String json) {
        ConnectionOptions opts = new ConnectionOptions();
        JSON.load(json,opts);
        verbose = opts.verbose;
    }

    private void addSubscription(String subject, String group, int ssid) throws IOException {
        System.out.println("subscribing subject="+subject+",group="+group+",ssid="+ssid);
        Subscription s = new Subscription(this,ssid,subject,group);
        server.addSubscription(s);
        if(isVerbose())
            sendOK();
    }

    private void removeSubscription(int ssid) throws IOException {
        System.out.println("un-subscribing ssid = "+ssid);
        Subscription s = new Subscription(this,ssid,"","");
        server.removeSubscription(s);
        if(isVerbose())
            sendOK();
    }

    private synchronized void sendOK() throws IOException {
        w.write("+OK\r\n".getBytes());
        w.flush();
    }

    private synchronized void sendError(Exception e) throws IOException {
        w.write(("-ERR "+e.toString()+"\r\n").getBytes());
        w.flush();
    }

    private boolean isVerbose() {
        return verbose;
    }

    synchronized void sendMessage(Subscription sub,String subject, byte[] reply, byte[] msg) throws IOException {
        if(closed)
            return;
//        System.out.println("sending to "+sub+", subject="+subject);

        w.write("MSG ".getBytes());
        w.write(subject.getBytes());
        w.write(" ".getBytes());
        w.write(Integer.toString(sub.ssid).getBytes());
        w.write(" ".getBytes());
        w.write(reply);
        w.write(" ".getBytes());
        w.write(Integer.toString(msg.length).getBytes());
        w.write("\r\n".getBytes());
        w.write(msg);
        w.write("\r\n".getBytes());
        w.flush();
    }

    private static String readLine(InputStream r) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int c = r.read(); ; c = r.read()) {
            if (c == -1)
                throw new IOException("end of file");
            if (c == '\r')
                continue;
            if (c == '\n')
                break;
            sb.append((char) c);
        }
        return sb.toString();
    }

    private static void readPayload(InputStream r, byte[] b) throws IOException {
        int len = b.length;
        int offset = 0;
        while (len > 0) {
            int read = r.read(b, offset, len);
            offset += read;
            len -= read;
        }
        r.read();
        r.read(); // skip CR-LF
    }

    public void close() {
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closed=true;
        }
    }

    public String getRemote() {
        return remote;
    }

    public boolean closed() {
        return closed;
    }

    public int getClientID() {
        return clientID;
    }

    public static class ConnectionOptions {
        public boolean verbose;
        public boolean pedantic;
        public boolean tls_required;
        public String name;
        public int protocol;
        public boolean echo;
    }
}

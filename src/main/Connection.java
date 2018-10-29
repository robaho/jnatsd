package com.robaho.jnatsd;

import com.robaho.jnatsd.util.CharSeq;
import com.robaho.jnatsd.util.JSON;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.*;
import java.net.Socket;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;

class Connection {
    private BufferedInputStream r;
    private BufferedOutputStream w;
    private final Server server;
    private Socket socket;
    private final String remote;
    private volatile boolean closed;
    private int clientID;
    private ConnectionOptions options = new ConnectionOptions();
    private boolean isSSL;
    private CharSeq[] args = new CharSeq[4];
    private byte[] msg = new byte[1024*1024];
    private final ConcurrentLinkedQueue<Message> queue = new ConcurrentLinkedQueue<>();
    private Thread writer;

    public Connection(Server server,Socket s) throws IOException {
        this.socket=s;
        this.server=server;

        clientID = server.getNextClientID();

        remote = s.getRemoteSocketAddress().toString();

        s.setReceiveBufferSize(1024*1024);
        s.setSendBufferSize(1024*1024);

        r = new BufferedInputStream(s.getInputStream(),256*1024);
        w = new BufferedOutputStream(s.getOutputStream(),256*1024);

        w.write(server.getInfoAsJSON(this).getBytes());
        flush();

        if(server.isTLSRequired()){
            upgradeToSSL();
        }
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

        writer = new Thread("Writer("+socket.getRemoteSocketAddress()+")"){
            public void run() {
                while(!closed) {
                    int count=0;
                    try {
                        Message m=null;
                        while((m=queue.poll())!=null){
                            writeMessage(m);
                            count++;
                        }
                        if(count>1) {
                            LockSupport.parkNanos(500*1000);
                        }
                        if(queue.isEmpty()){
                            flush();
                            LockSupport.park();
                        }
                    } catch (IOException e) {
                        server.closeConnection(Connection.this);
                        break;
                    }
                }
            }
        };
        writer.start();
    }

    private void readMessages() throws IOException {
        char[] buffer = new char[1024];

        for (CharSeq line; (line = readLine(buffer,r)) != null; ) {
            try {
                processLine(line);
            } catch(IOException e){
                throw e;
            } catch(Exception e){
                sendError(e);
                server.logger.log(Level.WARNING,"error processing connection",e);
            }
        }
    }
    private void processLine(CharSeq line) throws IOException {
        int index=1;
//        System.out.println("rec: " + line);
        int nargs = line.split(args);
        CharSeq cmd = args[0];
        if (cmd.equalsIgnoreCase("PUB")) {
            CharSeq subject = args[index++];
            CharSeq reply = CharSeq.EMPTY;
            if (nargs == 4) {
                reply = args[index++];
            }
            int len = args[index].toInt();
            readPayload(r,len);
            server.processMessage(this,subject, reply,msg,len);
        } else if (cmd.equalsIgnoreCase("PING")){
            sendPong();
        } else if (cmd.equalsIgnoreCase("SUB")) {
            CharSeq subject = args[index++];
            CharSeq group = CharSeq.EMPTY;
            if(nargs==4) { // we have a group
                group = args[index++];
            }
            int ssid = args[index].toInt();
            addSubscription(subject, group, ssid);
        } else if(cmd.equalsIgnoreCase("UNSUB")){
            int ssid = args[1].toInt();
            removeSubscription(ssid);
        } else if(cmd.equalsIgnoreCase("CONNECT")){
            processConnectionOptions(args[1].toString());
        } else {
            server.logger.warning("error: "+ line+", "+Arrays.toString(args));
            sendError("Unknown Protocol Operation");
        }
    }

    private static final byte[] PONG = "PONG\r\n".getBytes();
    private synchronized void sendPong() throws IOException {
        w.write(PONG);
        flush();
    }

    private void processConnectionOptions(String json) throws IOException {
        ConnectionOptions opts = new ConnectionOptions();
        JSON.load(json,opts);
        options = opts;

        if(options.tls_required || server.isTLSRequired()){
            upgradeToSSL();
        }
    }

    private synchronized void upgradeToSSL() throws IOException {
        if(isSSL)
            return;

//        System.out.println("upgrading socket to SSL");
        SSLSocketFactory ssf =
                (SSLSocketFactory)SSLSocketFactory.getDefault();

//        printCiphers(ssf);

        SSLSocket sslSocket =
                (SSLSocket)ssf.
                        createSocket(socket,
                                socket.getInetAddress().getHostAddress(),
                                socket.getPort(),
                                false);
        sslSocket.setUseClientMode(false);
        sslSocket.startHandshake();
        socket = sslSocket;

        isSSL=true;

        r = new BufferedInputStream(socket.getInputStream());
        w = new BufferedOutputStream(socket.getOutputStream());
    }

    private void printCiphers(SSLSocketFactory ssf) {
        String[] defaultCiphers = ssf.getDefaultCipherSuites();
        String[] availableCiphers = ssf.getSupportedCipherSuites();

        TreeMap ciphers = new TreeMap();

        for(int i=0; i<availableCiphers.length; ++i )
            ciphers.put(availableCiphers[i], Boolean.FALSE);

        for(int i=0; i<defaultCiphers.length; ++i )
            ciphers.put(defaultCiphers[i], Boolean.TRUE);

        System.out.println("Default\tCipher");
        for(Iterator i = ciphers.entrySet().iterator(); i.hasNext(); ) {
            Map.Entry cipher=(Map.Entry)i.next();

            if(Boolean.TRUE.equals(cipher.getValue()))
                System.out.print('*');
            else
                System.out.print(' ');

            System.out.print('\t');
            System.out.println(cipher.getKey());
        }
    }

    private void addSubscription(CharSeq subject, CharSeq group, int ssid) throws IOException {
        server.logger.fine("subscribing subject="+subject+",group="+group+",ssid="+ssid);
        Subscription s = new Subscription(this,ssid,subject.dup(),group.dup());
        server.addSubscription(s);
        if(isVerbose())
            sendOK();
    }

    private void removeSubscription(int ssid) throws IOException {
        server.logger.fine("un-subscribing ssid = "+ssid);
        Subscription s = new Subscription(this,ssid,CharSeq.EMPTY,CharSeq.EMPTY);
        server.removeSubscription(s);
        if(isVerbose())
            sendOK();
    }

    private static byte[] OK = "+OK\r\n".getBytes();
    private synchronized void sendOK() throws IOException {
        w.write(OK);
        flush();
    }

    private synchronized void sendError(Exception e) throws IOException {
        sendError(e.toString());
    }
    private synchronized void sendError(String err) throws IOException {
        w.write(("-ERR '"+err+"'\r\n").getBytes());
        flush();
    }

    private synchronized void flush() throws IOException {
        // TODO, avoid flushing if possible - use heuristics to try and determine that
        // more messages are probably going to arrive and if so, set a timer in the server
        // to callback with the flush
        w.flush();
    }

    private boolean isVerbose() {
        return options.verbose;
    }

    public boolean isEcho() {
        return options.echo;
    }

    private static byte[] MSG = "MSG ".getBytes();
    private static byte[] CF_LF = "\r\n".getBytes();

    private static class Message {
        final Subscription sub;
        final byte[] data;
        final CharSeq subject;
        final CharSeq reply;

        public Message(Subscription sub, CharSeq subject, CharSeq reply, byte[] data) {
            this.sub=sub;
            this.subject=subject;
            this.reply=reply;
            this.data=data;
        }
    }

    void sendMessage(Subscription sub,CharSeq subject, CharSeq reply, byte[] data)  {
        if (closed)
            return;

        queue.add(new Message(sub, subject, reply, data));
        LockSupport.unpark(writer);
    }

    private synchronized void writeMessage(Message msg) throws IOException {
//        System.out.println("sending to "+sub+", subject="+subject);
        w.write(MSG);
        msg.subject.write(w);
        w.write(' ');
        writeInt(w,msg.sub.ssid);
        w.write(' ');
        msg.reply.write(w);
        w.write(' ');
        writeInt(w,msg.data.length);
        w.write(CF_LF);
        w.write(msg.data);
        w.write(CF_LF);
    }

    private static byte[] intToBytes = new byte[32];
    private static void writeInt(OutputStream w,int i) throws IOException {
        int offset=intToBytes.length-1;
        do {
            char c = (char) (i%10+'0');
            intToBytes[offset--]=(byte)c;
            i/=10;
        } while(i>0);
        w.write(intToBytes,offset+1,intToBytes.length-1-offset);
    }

    private CharSeq readLine(char[] buffer,InputStream r) throws IOException {
        int len=0;
        for (int c = r.read(); ; c = r.read()) {
            if (c == -1)
                throw new IOException("end of file");
            if (c == '\r')
                continue;
            if (c == '\n')
                break;
            buffer[len++]=(char)c;
        }

        return new CharSeq(buffer,0,len);
    }

    private void readPayload(InputStream r,int len) throws IOException {
        int offset = 0;
        while (len > 0) {
            int read = r.read(msg, offset, len);
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
            writer.interrupt();
            LockSupport.unpark(writer);
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
        public String auth_token;
        public String user;
        public String pass;
    }

}

package com.robaho.jnatsd;

import com.robaho.jnatsd.util.*;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.*;
import java.net.Socket;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;

class Connection {
    private InputStream r;
    private OutputStream w;
    private final Server server;
    private Socket socket;
    private final String remote;
    private volatile boolean closed;
    private int clientID;
    private ConnectionOptions options = new ConnectionOptions();
    private boolean isSSL;
    private CharSeq[] args = new CharSeq[4];
    private Thread flusher,reader;
    private long nMsgsRead;
    private long nMsgsWrite;

    private final long connectTime;
    private int pingCount=0;
    private volatile long lastWriteNanos=0;

    private final ExclusiveLock lock = new ExclusiveLock();
    private final AtomicBoolean flushPermit = new AtomicBoolean();

    public Connection(Server server,Socket s) throws IOException {
        this.socket=s;
        this.server=server;
        this.connectTime = System.currentTimeMillis();

        clientID = server.getNextClientID();

        remote = s.getRemoteSocketAddress().toString();

        socket.setTcpNoDelay(true);

        r = new UnsyncBufferedInputStream(s.getInputStream(),64*1024);
//        w = new UnsyncBufferedOutputStream(new ChannelOutputStream(s.getChannel(),64*1024),256);
        w = new ChannelOutputStream(s.getChannel(),64*1024);

        w.write(server.getInfoAsJSON(this).getBytes());
        flush();

        log(Level.INFO,"connected");

        if(server.isTLSRequired()){
            upgradeToSSL();
        }
    }

    void processConnection(){
//        reader = new Thread(new ConnectionReader(),"Reader("+socket.getRemoteSocketAddress()+")");
//        reader.start();
        reader = Thread.startVirtualThread(new ConnectionReader());

//        writer = new Thread(new ConnectionWriter(),"Writer("+socket.getRemoteSocketAddress()+")");
//        writer.start();
        flusher = Thread.startVirtualThread(new ConnectionFlusher());
    }

    private class ConnectionReader implements Runnable {
        public void run() {
            while(!closed) {
                try {
                    readMessages();
                } catch (EOFException e) {
                    server.closeConnection(Connection.this);
                    break;
                } catch (IOException e) {
                    log(Level.WARNING,"connection read failed, expected if client closed socket",e);
                    server.closeConnection(Connection.this);
                    break;
                }
            }
        }
    }

    private class ConnectionFlusher implements Runnable {
        public void run() {
            while (!closed) {
                long now = System.nanoTime();
                long lw = lastWriteNanos;
                if (lw != 0 && now - lw > TimeUnit.MICROSECONDS.toNanos(500)) {
                    try {
                        flush();
                    } catch (IOException ex) {
                        log(Level.WARNING,"connection write failed",ex);
                        server.closeConnection(Connection.this);
                    }
                }
                flushPermit.set(false);
                if (lw == 0) {
                    LockSupport.park();
                } else {
                    LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(500));
                }
            }
        }
    }

    private void readMessages() throws IOException {
        byte[] buffer = new byte[1024];

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
    private static final CharSeq PUB = new CharSeq("PUB");
    private static final CharSeq PING = new CharSeq("PING");
    private static final CharSeq SUB = new CharSeq("SUB");
    private static final CharSeq UNSUB = new CharSeq("UNSUB");
    private static final CharSeq CONNECT = new CharSeq("CONNECT");

    private void processLine(CharSeq line) throws IOException {
        int index=1;
//        System.out.println("rec: " + line);
        int nargs = line.split(args);
        CharSeq cmd = args[0];
        if (cmd.equalsIgnoreCase(PUB)) {
            CharSeq subject = args[index++];
            CharSeq reply = CharSeq.EMPTY;
            if (nargs == 4) {
                reply = args[index++];
            }
            int len = args[index].toInt();
            byte[] msg = new byte[len];
            readPayload(r,msg);
            nMsgsRead++;
            server.queueMessage(new InMessage(this,subject.dup(),reply.dup(),msg));
        } else if (cmd.equalsIgnoreCase(PING)){
            if(pingCount++==0 && System.currentTimeMillis()-connectTime>500) {
                log(Level.WARNING,"too long to receive initial PING");
            }
            log(Level.FINE,"PING!");
            sendPong();
        } else if (cmd.equalsIgnoreCase(SUB)) {
            CharSeq subject = args[index++];
            CharSeq group = CharSeq.EMPTY;
            if(nargs==4) { // we have a group
                group = args[index++];
            }
            int ssid = args[index].toInt();
            addSubscription(subject, group, ssid);
        } else if(cmd.equalsIgnoreCase(UNSUB)){
            int ssid = args[1].toInt();
            removeSubscription(ssid);
        } else if(cmd.equalsIgnoreCase(CONNECT)){
            if(System.currentTimeMillis()-connectTime>500) {
                log(Level.WARNING,"too long to receive CONNECT");
            }
            log(Level.FINE,"connection options: "+args[1].toString());
            processConnectionOptions(args[1].toString());
        } else {
            log(Level.WARNING,"error: "+ line+", "+Arrays.toString(args));
            sendError("Unknown Protocol Operation");
        }
    }

    private static final byte[] PONG = "PONG\r\n".getBytes();
    private void sendPong() throws IOException {
        lock.lock();;
        try {
            log(Level.FINE,"Pong!");
            w.write(PONG);
            flushWithLock();
        } finally {
            lock.unlock();
        }
    }

    private void processConnectionOptions(String json) throws IOException {
        ConnectionOptions opts = new ConnectionOptions();
        opts.json = json;
        JSON.load(json,opts);
        options = opts;

        if(options.tls_required || server.isTLSRequired()){
            upgradeToSSL();
        }
    }

    private void upgradeToSSL() throws IOException {
        if(isSSL)
            return;

//        System.out.println("upgrading socket to SSL");
        SSLSocketFactory ssf =
                (SSLSocketFactory)SSLSocketFactory.getDefault();

//        printCiphers(ssf);

        lock.lock();

        try {
            SSLSocket sslSocket =
                    (SSLSocket) ssf.
                            createSocket(socket,
                                    socket.getInetAddress().getHostAddress(),
                                    socket.getPort(),
                                    false);
            sslSocket.setUseClientMode(false);
            sslSocket.startHandshake();
            socket = sslSocket;

            isSSL = true;


            r = new BufferedInputStream(socket.getInputStream());
            w = new BufferedOutputStream(socket.getOutputStream());
        } finally {
            lock.unlock();
        }
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
        server.logger.info("subscribing subject="+subject+",group="+group+",ssid="+ssid);
        Subscription s = new Subscription(this,ssid,subject.dup(),group.dup());
        server.addSubscription(s);
        if(isVerbose())
            sendOK();
    }

    private void removeSubscription(int ssid) throws IOException {
        server.logger.info("un-subscribing ssid = "+ssid);
        Subscription s = new Subscription(this,ssid,CharSeq.EMPTY,CharSeq.EMPTY);
        server.removeSubscription(s);
        if(isVerbose())
            sendOK();
    }

    private static byte[] OK = "+OK\r\n".getBytes();
    private void sendOK() throws IOException {
        lock.lock();
        try {
            w.write(OK);
            flushWithLock();
        } finally {
            lock.unlock();
        }
    }

    private void sendError(Exception e) throws IOException {
        sendError(e.toString());
    }
    private void sendError(String err) throws IOException {
        lock.lock();
        try {
            w.write(("-ERR '" + err + "'\r\n").getBytes());
            flushWithLock();
        } finally {
            lock.unlock();
        }
    }

    private void flush() throws IOException {
        lock.lock();
        try {
            flushWithLock();
        } finally {
            lock.unlock();
        }
    }
    private void flushWithLock() throws IOException {
        w.flush();
        lastWriteNanos = 0;
    }

    private boolean isVerbose() {
        return options.verbose;
    }

    public boolean isEcho() {
        return options.echo;
    }

    private static byte[] MSG = "MSG ".getBytes();
    private static byte[] CR_LF = "\r\n".getBytes();

    private static class OutMessage {
        final Subscription sub;
        final InMessage msg;

        public OutMessage(Subscription sub, InMessage msg) {
            this.sub=sub;
            this.msg=msg;
        }
    }

    void sendMessage(Subscription sub,InMessage msg)  {
        if (closed)
            return;

        OutMessage m = new OutMessage(sub,msg);
        try {
            lock.lock();
            try {
                writeMessage(m);
            } finally {
                lock.unlock();
            }
        } catch (IOException e) {
            server.logger.warning("unable to write message: "+e.getMessage());
            server.closeConnection(Connection.this);
        }
//        try {
//            long start = System.nanoTime();
//            queue.put(m);
//            long time = System.nanoTime()-start;
//            totalSendTime +=time;
//            totalSends++;
//            maxSendTime = Math.max(time,maxSendTime);
//        } catch (InterruptedException e) {
//            server.logger.warning("interrupted, closing connection");
//            server.closeConnection(Connection.this);
//        }
    }

    private void writeMessage(OutMessage out) throws IOException {
        if(out==null)
            return;

        nMsgsWrite++;

        InMessage in = out.msg;

//        System.out.println("sending to "+sub+", subject="+subject);
        w.write(MSG);
        in.subject.write(w);
        w.write(' ');
        writeInt(w,out.sub.ssid);
        if(out.msg.reply.length()!=0) {
            w.write(' ');
            in.reply.write(w);
        }
        w.write(' ');
        writeInt(w,in.data.length);
        w.write(CR_LF);
        w.write(in.data);
        w.write(CR_LF);

        lastWriteNanos = System.nanoTime();
        // no need to wake flusher if there is another writing thread
        if(lock.hasWaiters())
            return;
        if(flushPermit.compareAndSet(false,true)) {
            LockSupport.unpark(flusher);
        }
    }

    private final byte[] intToBytes = new byte[32];
    private void writeInt(OutputStream w,int i) throws IOException {
        int offset=intToBytes.length-1;
        do {
            char c = (char) (i%10+'0');
            intToBytes[offset--]=(byte)c;
            i/=10;
        } while(i>0);
        w.write(intToBytes,offset+1,intToBytes.length-1-offset);
    }

    private CharSeq readLine(byte[] buffer,InputStream r) throws IOException {
        int len=0;
        for (int c = r.read(); ; c = r.read()) {
            if (c == -1)
                throw new EOFException();
            if (c == '\r')
                continue;
            if (c == '\n')
                break;
            if(len==buffer.length)
                throw new IOException("line too long");
            buffer[len++]=(byte)c;
        }

        return new CharSeq(buffer,0,len);
    }

    private void readPayload(InputStream r,byte[] msg) throws IOException {
        int len=msg.length;
        int offset=0;

        while (len > 0) {
            int read = r.read(msg, offset, len);
            if(read <0)
                throw new EOFException();

            offset += read;
            len -= read;
        }

        r.read();
        r.read(); // skip CR-LF
    }

    public void close() {
        try {
            flush();
            socket.close();
        } catch (IOException e) {
//            e.printStackTrace();
        } finally {
            closed=true;
            reader.interrupt();
            flusher.interrupt();
        }

        try {
            flusher.join();
            reader.join();
        } catch (InterruptedException e) {
            log(Level.WARNING,"unable to join reader/writer",e);
        }
        log(Level.WARNING,"connection closed, msgs read "+nMsgsRead+", write "+nMsgsWrite);
    }

    public String getRemote() {
        return remote;
    }

    public int getClientID() {
        return clientID;
    }

    private void log(Level level,String msg,Throwable t) {
        server.logger.log(level,""+remote+": "+msg,t);
    }
    private void log(Level level,String msg) {
        server.logger.log(level,""+remote+": "+msg);
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
        public String json;
    }

}

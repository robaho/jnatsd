package com.robaho.jnatsd;

import com.robaho.jnatsd.util.*;
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.*;
import java.net.Socket;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
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
    private final RingBuffer<OutMessage> queue = new RingBuffer(8192);
//    private Thread writer;
    private long nMsgsRead;
    private long nMsgsWrite;

    private final ByteBuffer rBuffer = ByteBuffer.allocateDirect(64*1024);
    private final ByteBuffer wBuffer = ByteBuffer.allocateDirect(64*1024);

    public Connection(Server server,Socket s) throws IOException {
        this.socket=s;
        this.server=server;

        clientID = server.getNextClientID();

        remote = s.getRemoteSocketAddress().toString();

        rBuffer.flip();

        socket.setTcpNoDelay(true);

        r = new UnsyncBufferedInputStream(s.getInputStream(),256*1024);
        w = new UnsyncBufferedOutputStream(s.getOutputStream(),256*1024);

        w.write(server.getInfoAsJSON(this).getBytes());
        flush();

        if(server.isTLSRequired()){
            upgradeToSSL();
        }
    }

    private final AtomicBoolean writerSync = new AtomicBoolean();

    void processConnection(){
        Thread reader = new Thread(new ConnectionReader(),"Reader("+socket.getRemoteSocketAddress()+")");
        reader.start();

//        writer = new Thread(new ConnectionWriter(),"Writer("+socket.getRemoteSocketAddress()+")");
//        writer.start();
    }

    private class ConnectionReader implements Runnable {
        public void run() {
            try {
                readMessages();
            } catch (IOException e) {
                server.closeConnection(Connection.this);
            }
        }
    }

    private class ConnectionWriter implements Runnable {
        public void run() {

            OutMessage m=null;
            while(!closed) {
                try {
                    long deadline=0;
                    while((m=queue.poll())==null && !closed){
                        if(deadline==0) // the longer the deadline, the more we delay the flush if one is required, hurting latency for individual requests
                            deadline=System.nanoTime() + Server.spinForTimeoutThreshold;
                        if(System.nanoTime()>deadline){
                            if(writerSync.compareAndSet(true,false))
                                continue;
                            flush();
                            LockSupport.park();
                        }
                    }
//                    while((m=queue.poll())==null && !closed){
//                        if(writerSync.compareAndSet(true,false))
//                            continue;
////                        LockSupport.parkNanos(500*1000);
////                        if(writerSync.compareAndSet(true,false))
////                            continue;
//                        flush();
//                        LockSupport.park();
//                    }
                    writeMessage(m);
                } catch (IOException e) {
                    server.closeConnection(Connection.this);
                    break;
                } catch (Exception e) {
                    server.logger.log(Level.WARNING,"connection failed",e);
                    server.closeConnection(Connection.this);
                    break;
                }
            }
        }
    }

    private void readMessages() throws IOException {
        byte[] buffer = new byte[1024];

        for (CharSeq line; (line = readLine(buffer)) != null; ) {
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

    private final byte[] crlf = new byte[2];

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
            readBytes(msg);
            readBytes(crlf);
//            System.out.println("read msg "+msg.length);
            nMsgsRead++;
            server.queueMessage(new InMessage(this,subject.dup(),reply.dup(),msg));
        } else if (cmd.equalsIgnoreCase(PING)){
            System.out.println("GOT PING!");
            server.logger.fine("PING!");
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
        w.flush();
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

    long lastWrite;

    void sendMessage(Subscription sub,InMessage msg)  {
        if (closed)
            return;

        OutMessage m = new OutMessage(sub,msg);
        try {
            writeMessage(m);
            lastWrite=System.nanoTime();
        } catch (IOException e) {
            server.logger.log(Level.WARNING,"write to connection failed",e);
            server.closeConnection(this);
        }

//        while(!queue.offer(m) && !closed);
//        if(writerSync.compareAndSet(false,true))
//            LockSupport.unpark(writer);
    }

    private byte[] header = new byte[1024];
    private synchronized void writeMessage(OutMessage out) throws IOException {
        if(out==null)
            return;

        nMsgsWrite++;

        InMessage in = out.msg;

        ByteBuffer hdr = ByteBuffer.wrap(header);

//        System.out.println("sending to "+sub+", subject="+subject);
        w.write(MSG);
        in.subject.write(w);
        w.write(' ');
        writeInt(w,out.sub.ssid);
        if(out.msg.reply.length()!=0) {
            hdr.put((byte)' ');
            in.reply.write(hdr);
        }
        hdr.put((byte)' ');
        writeInt(hdr,in.data.length);
        hdr.put(CR_LF);

        write(hdr.array(),0,hdr.position());

        write(in.data);
        write(CR_LF);
    }

    private final byte[] intToBytes = new byte[32];
    private void writeInt(ByteBuffer w,int i) throws IOException {
        int offset=intToBytes.length-1;
        do {
            char c = (char) (i%10+'0');
            intToBytes[offset--]=(byte)c;
            i/=10;
        } while(i>0);
        w.put(intToBytes,offset+1,intToBytes.length-1-offset);
    }

    private CharSeq readLine(byte[] buffer) throws IOException {
        int len = 0;

        while(true) {
            while(!rBuffer.hasRemaining()){
                rBuffer.clear();
                if(ch.read(rBuffer)==-1)
                    throw new IOException("stream closed");
                rBuffer.flip();
            }
            int c = rBuffer.get();
            if (c == -1)
                throw new IOException("end of file");
            if (c == '\r')
                continue;
            if (c == '\n') {
                return new CharSeq(buffer,0,len);
            }
            buffer[len++] = (byte) c;
        }
    }

    private void readBytes(byte[] msg) throws IOException {
        int len=msg.length;int offset=0;
        while (len > 0) {
            int n = Math.min(rBuffer.remaining(),len);
            rBuffer.get(msg,offset,n);
            offset+=n;
            len-=n;
            if(n==0){
                rBuffer.clear();
                if(ch.read(rBuffer)==-1) {
                    throw new IOException("stream closed");
                }
                rBuffer.flip();
            }
        }
    }

    public synchronized void close() {
        server.logger.fine("msgs read "+nMsgsRead+", write "+nMsgsWrite);
        try {
            try {
                flush();
            } catch (IOException e){
                server.logger.log(Level.FINE,"exception on final flush",e);
            }
            ch.close();
        } catch (IOException e) {
            server.logger.log(Level.FINE,"exception on close",e);
        } finally {
//            writer.interrupt();
//            LockSupport.unpark(writer);
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

package com.robaho.jnatsd;

import com.robaho.jnatsd.util.*;

import javax.net.ssl.SSLSocketFactory;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Future;
import java.util.logging.Level;

class Connection {
    private final Server server;
    private SocketChannel ch;
    private final String remote;
    private volatile boolean closed;
    private int clientID;
    private ConnectionOptions options = new ConnectionOptions();
    private boolean isSSL;
    private CharSeq[] args = new CharSeq[4];
    private long nMsgsRead;
    private long nMsgsWrite;

    private final ByteBuffer rBuffer = ByteBuffer.allocateDirect(64*1024);
    private final ByteBuffer rBuffer0 = ByteBuffer.allocateDirect(64*1024);

    private final ByteBuffer wBuffer = ByteBuffer.allocateDirect(64*1024);
    private final ByteBuffer wBuffer0 = ByteBuffer.allocateDirect(64*1024);

    private Future<Boolean> writeRequest,readRequest;

    public Connection(Server server, SocketChannel ch) throws IOException {
        this.ch=ch;
        this.server=server;

        clientID = server.getNextClientID();

        remote = ch.getRemoteAddress().toString();

        rBuffer.flip();

        readRequest = server.bgRead(ch,rBuffer0);

        write(server.getInfoAsJSON(this).getBytes());
        flush();

        if(server.isTLSRequired()){
            upgradeToSSL();
        }
    }

    void processConnection(){
        Thread reader = new Thread(new ConnectionReader(),"Reader("+getRemote()+")");
        reader.start();
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

    private synchronized void write(byte[] bytes) throws IOException {
        write(bytes,0,bytes.length);
    }

    private synchronized void write(byte[] bytes,int offset,int len) throws IOException {
        while(len>0) {
            if(wBuffer.remaining()==0){
                flush();
            }
            int n = Math.min(len,wBuffer.remaining());
            wBuffer.put(bytes,offset,n);
            offset+=n;
            len-=n;
        }
        lastWrite=System.nanoTime();
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
    private byte[] msg = new byte[1024];

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
            if(len>msg.length){
                msg = new byte[len];
            }
            readBytes(msg,len+2); // read the cr-lf too
//            System.out.println("read msg "+msg.length);
            nMsgsRead++;
            server.routeMessage(new InMessage(this,subject,reply,msg,len));
        } else if (cmd.equalsIgnoreCase(PING)){
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
        write(PONG);
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

////        System.out.println("upgrading socket to SSL");
//        SSLSocketFactory ssf =
//                (SSLSocketFactory)SSLSocketFactory.getDefault();
//
////        printCiphers(ssf);
//
//        SSLSocket sslSocket =
//                (SSLSocket)ssf.
//                        createSocket(socket,
//                                socket.getInetAddress().getHostAddress(),
//                                socket.getPort(),
//                                false);
//        sslSocket.setUseClientMode(false);
//        sslSocket.startHandshake();
//        socket = sslSocket;

        isSSL=true;
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
        write(OK);
        flush();
    }

    private synchronized void sendError(Exception e) throws IOException {
        sendError(e.toString());
    }
    private synchronized void sendError(String err) throws IOException {
        write(("-ERR '"+err+"'\r\n").getBytes());
        flush();
    }

    synchronized void flush() throws IOException {
        if(closed)
            return;

        if(wBuffer.position()==0)
            return;

        if(writeRequest!=null) {
            try {
                if(writeRequest.get())
                    throw new IOException("stream closed");
            } catch (Exception e) {
                throw new IOException("background write failed",e);
            }
        }

        wBuffer0.clear();
        wBuffer.flip();
        wBuffer0.put(wBuffer);
        wBuffer0.flip();

        wBuffer.clear();

        writeRequest = server.bgWrite(ch, wBuffer0);
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

    volatile long lastWrite;

    void sendMessage(Subscription sub,InMessage msg) {
        if (closed)
            return;

        OutMessage m = new OutMessage(sub,msg);
        try {
            writeMessage(m);
        } catch (IOException e) {
            server.logger.log(Level.WARNING,"write to connection failed",e);
            server.closeConnection(this);
        }
    }

    private byte[] header = new byte[1024];
    private synchronized void writeMessage(OutMessage out) throws IOException {
        if(out==null)
            return;

        nMsgsWrite++;

        InMessage in = out.msg;

        ByteBuffer hdr = ByteBuffer.wrap(header);

//        System.out.println("sending to "+sub+", subject="+subject);

        hdr.put(MSG);
        in.subject.write(hdr);
        hdr.put((byte)' ');
        writeInt(hdr,out.sub.ssid);
        if(out.msg.reply.length()!=0) {
            hdr.put((byte)' ');
            in.reply.write(hdr);
        }
        hdr.put((byte)' ');
        writeInt(hdr,in.datalen);
        hdr.put(CR_LF);

        write(hdr.array(),0,hdr.position());

        write(in.data,0, in.datalen);
        write(CR_LF);
    }

    private final byte[] intToBytes = new byte[32];
    private void writeInt(ByteBuffer w,int i) {
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
                fillBuffer();
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

    private void fillBuffer() throws IOException {
        rBuffer.clear();

        try {
            if(readRequest.get())
                throw new IOException("read failed");
        } catch (Exception e) {
            throw new IOException("read failed",e);
        }
        rBuffer0.flip();
        rBuffer.put(rBuffer0);
        rBuffer.flip();
        rBuffer0.clear();

        readRequest = server.bgRead(ch,rBuffer0);
    }

    private void readBytes(byte[] msg,int len) throws IOException {
        int offset=0;
        while (len > 0) {
            int n = Math.min(rBuffer.remaining(),len);
            rBuffer.get(msg,offset,n);
            offset+=n;
            len-=n;
            if(n==0){
                fillBuffer();
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

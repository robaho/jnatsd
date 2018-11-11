package com.robaho.jnatsd.util;

import sun.nio.ch.SelChImpl;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class ChannelIO {
    private static native int write0(int fd,long address,int len);
    private static native int read0(int fd,long address,int len);
    public static native int dummy(int fd,long address,int len);

    public static int write(int fd, ByteBuffer bb) throws IOException {
        int n = write0(fd,((sun.nio.ch.DirectBuffer)bb).address()+bb.position(),bb.remaining());
        if(n<=0) {
            if(n==-35)
                return 0;
            throw new IOException("read failed " + n);
        }
//        System.out.println("write "+n+" on "+fd);
        bb.position(bb.position()+n);
        return n;
    }
    public static int read(int fd, ByteBuffer bb) throws IOException {
        int max = bb.remaining();
        int n = read0(fd,((sun.nio.ch.DirectBuffer)bb).address()+bb.position(),max);
        if(n<=0) {
            if (n == -35)
                return 0;
            throw new IOException("read failed " + n);
        }
//        System.out.println("read "+n+" on "+fd);
        bb.position(bb.position()+n);
        return n;
    }

    public static int getFD(SocketChannel ch) throws IOException {

        try {
            Method m = SelChImpl.class.getDeclaredMethod("getFDVal");
            m.setAccessible(true);
            return (int) m.invoke(ch);
        } catch (Exception e) {
            throw new IOException("cant get fdfd",e);
        }
    }

    static {
        System.loadLibrary("channelio");
    }
}

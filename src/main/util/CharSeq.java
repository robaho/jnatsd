package com.robaho.jnatsd.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

public final class CharSeq implements CharSequence {
    public static final CharSeq EMPTY = new CharSeq();
    private final byte[] array;
    private final int offset;
    private final int len;
    private int hashCode;

    public CharSeq(String s) {
        this(s.getBytes());
    }

    public CharSeq(byte[] array, int offset, int len, int hashCode) {
        this.array = array;
        this.offset = offset;
        this.len = len;
        this.hashCode = hashCode;
    }

    public CharSeq(byte[] array, int offset, int len) {
        this.array = array;
        this.offset = offset;
        this.len = len;
        int hash=0;
        for(int i=0;i<len;i++) {
            hash = hash * 31 + array[offset+i];
        }
        this.hashCode = hash;
    }

    public CharSeq() {
        array=null;
        offset=0;
        len=0;
        hashCode=0;
    }

    public CharSeq(byte[] array) {
        this(array,0,array.length);
    }

    @Override
    public int length() {
        return len;
    }

    @Override
    public char charAt(int index) {
        return (char)(array[offset+index] &0xFF);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return new CharSeq(array,offset+start,end-start);
    }

    public String toString() {
        if(this==EMPTY)
            return "";
        return new String(array,offset,len);
    }

    public boolean equalsIgnoreCase(CharSeq s) {
        if(len!=s.length())
            return false;
        int off = offset;
        int off0 = s.offset;
        byte[] array0 = s.array;

        for(int i=0;i<len;i++){
            byte c = array[off++];
            byte c0 = array0[off0++];
            if(c!=c0 && toUpperCase(c)!=c0)
                return false;
        }
        return true;
    }

    private static byte toUpperCase(byte c) {
        if(c>='a' && c<='z') {
            return (byte)('A'+(c-'a'));
        }
        return c;
    }

    public boolean equals(Object o) {
        if(!(o instanceof CharSeq))
            return false;
        CharSeq s0 = (CharSeq) o;
        if(hashCode!=s0.hashCode || len!=s0.len)
            return false;

        final int off = offset;
        final int off0 = s0.offset;
        final int l = len;

        for(int i=0;i<l;i++) {
            if(array[off+i]!=s0.array[off0+i])
                return false;
        }
        return true;
    }

    public int hashCode() {
        return hashCode;
    }

    public CharSeq dup() {
        if(this==EMPTY)
            return EMPTY;
        byte[] array2 = Arrays.copyOfRange(array,offset,offset+len);
        return new CharSeq(array2,0,len,hashCode);
    }

    public int toInt() {
        int v = 0;
        for(int i=0;i<len;i++){
            v = v * 10 + charAt(i)-'0';
        }
        return v;

    }

    public int split(CharSeq[] segs) {
        int n=0;
        int start=0;
        int hash=0;

        final byte[] a = array;
        final int l = len;

        for(int i=0;i<l;i++) {
            byte c = a[i];
            if(c==' '){
                if(i-start>0) {
                    segs[n++] = new CharSeq(a, start, i - start, hash);
                }
                start=i+1;
                hash=0;
            }
            hash = hash * 31 + c;
        }
        if(start!=l){
            segs[n++]=new CharSeq(a,start,l-start,hash);
        }
        return n;
    }

    public void write(ByteBuffer os) {
        if(len==0)
            return;
        os.put(array,offset,len);
    }
}

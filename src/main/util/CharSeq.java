package com.robaho.jnatsd.util;

import java.io.IOException;
import java.io.OutputStream;

public class CharSeq implements CharSequence {
    public static final CharSeq EMPTY = new CharSeq();
    private final char[] array;
    private final int offset;
    private final int len;
    private int hashCode;

    public CharSeq(char[] array, int offset, int len, int hashCode) {
        this.array = array;
        this.offset = offset;
        this.len = len;
        this.hashCode = hashCode;
    }

    public CharSeq(char[] array, int offset, int len) {
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

    public CharSeq(char[] array) {
        this(array,0,array.length);
    }

    @Override
    public int length() {
        return len;
    }

    @Override
    public char charAt(int index) {
        return array[offset+index];
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

    public boolean equalsIgnoreCase(CharSequence s) {
        if(len!=s.length())
            return false;
        for(int i=0;i<len;i++){
            char c = array[i];
            char c0 = s.charAt(i);
            if(c!=c0 && Character.toUpperCase(c)!=c0)
                return false;
        }
        return true;
    }

    public boolean equals(Object o) {
        if(!(o instanceof CharSeq))
            return false;
        CharSeq s2 = (CharSeq) o;
        if(len!=s2.len || hashCode!=s2.hashCode)
            return false;
        char[] array2 = s2.array;
        int off = offset;
        int off2 = s2.offset;
        for(int i=0;i<len;i++){
            if(array[off++]!=array2[off2++])
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
        char[] array2 = new char[len];
        System.arraycopy(array,offset,array2,0,len);
        return new CharSeq(array2,0,len,hashCode);
    }

    public int toInt() {
        int v = 0;
        for(int i=0;i<len;i++){
            v = v * 10 + array[i+offset]-'0';
        }
        return v;

    }

    public int split(CharSeq[] segs) {
        int n=0;
        int start=0;
        int hash=0;
        for(int i=0;i<len;i++) {
            char c = array[i];
            if(c==' '){
                segs[n++]=new CharSeq(array,start,i-start,hash);
                while(i<len && c==' '){
                    i++;
                    c=array[i];
                }
                start=i;
                hash=0;
            }
            hash = hash * 31 + c;
        }
        if(start!=len){
            segs[n++]=new CharSeq(array,start,len-start,hash);
        }
        return n;
    }

    public void write(OutputStream os) throws IOException {
        for(int i=0;i<len;i++){
            os.write(array[i+offset] & 0xFF);
        }
    }
}

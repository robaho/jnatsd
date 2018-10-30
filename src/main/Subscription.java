package com.robaho.jnatsd;

import com.robaho.jnatsd.util.CharSeq;

import java.util.regex.Pattern;

/**
 * holds a per connection subscription to a subject
 */
class Subscription implements Comparable<Subscription> {
    Connection connection;
    CharSeq subject;
    CharSeq group;
    int ssid;

    private String[] segments;

    private static Pattern dot = Pattern.compile("\\.");

    public Subscription(Connection connection, int ssid, CharSeq subject, CharSeq group) {
        this.connection=connection;
        this.ssid=ssid;
        this.subject = subject;
        this.group=group;
        segments = dot.split(subject);
    }
    public Subscription(Connection connection, int ssid, String subject, String group) {
        this(connection,ssid,new CharSeq(subject.getBytes()),new CharSeq(group.getBytes()));
    }

    public String toString() {
        return ssid+","+subject+","+group+","+((connection!=null) ? connection.getRemote() : "");
    }

    @Override
    public int compareTo(Subscription o) {
        int len = Math.min(segments.length,o.segments.length);
        int diff = segments.length-o.segments.length;
        for(int i=0;i<len;i++){
            int result = segments[i].compareTo(o.segments[i]);
            if(result==0)
                continue;
            if(segments[i].equals(">"))
                return -1;
            if(o.segments[i].equals(">"))
                return 1;
            if(segments[i].equals("*") && diff<=0)
                return -1;
            if(o.segments[i].equals("*") && diff>=0)
                return 1;
            if(result!=0)
                return result;
        }
        if(segments.length==o.segments.length)
            return 0;
        if(segments.length<o.segments.length)
            return -1;
        return 1;
    }

    public boolean matches(Subscription s) {
        int len = Math.min(segments.length,s.segments.length);
        for(int i=0;i<len;i++) {
            if(segments[i].equals("*") || segments[i].equals(s.segments[i]))
                continue;
            if(segments[i].equals(">"))
                return true;
            return false;
        }
        return s.segments.length==segments.length;
    }
}

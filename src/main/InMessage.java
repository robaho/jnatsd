package com.robaho.jnatsd;

import com.robaho.jnatsd.util.CharSeq;

class InMessage {
    final Connection connection;
    final byte[] data;
    final CharSeq subject;
    final CharSeq reply;

    public InMessage(Connection connection, CharSeq subject, CharSeq reply, byte[] data) {
        this.connection=connection;
        this.subject=subject;
        this.reply=reply;
        this.data=data;
    }
}

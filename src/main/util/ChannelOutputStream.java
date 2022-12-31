package com.robaho.jnatsd.util;


import java.io.IOException;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * buffered output stream to channel backed by direct byte buffer
 */
public
class ChannelOutputStream extends OutputStream {
    private final WritableByteChannel channel;
    private final ByteBuffer buffer;
    public ChannelOutputStream(WritableByteChannel channel, int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("Buffer size <= 0");
        }
        if(channel==null) {
            throw new IllegalArgumentException("channel is null");
        }
        this.channel = channel;
        this.buffer = ByteBuffer.allocateDirect(size);
    }

    /** Flush the internal buffer */
    private void flushBuffer() throws IOException {
        if(buffer.position()!=0) {
            buffer.flip();
            channel.write(buffer);
            while(buffer.hasRemaining()) {
                Thread.yield();
                channel.write(buffer);
            }
            buffer.clear();
        }
    }

    /**
     * Writes the specified byte to this buffered output stream.
     *
     * @param      b   the byte to be written.
     * @exception  IOException  if an I/O error occurs.
     */
    public void write(int b) throws IOException {
        try {
            buffer.put((byte) b);
        } catch (BufferOverflowException e) {
            flush();
            buffer.put((byte)b);
        }
    }
    public void write(byte b[], int off, int len) throws IOException {
        if(len > buffer.remaining()) {
            flush();
        }
        if(len>buffer.capacity()) {
            channel.write(ByteBuffer.wrap(b,off,len));
        } else {
            buffer.put(b,off,len);
        }
    }
    public void flush() throws IOException {
        flushBuffer();
    }
}

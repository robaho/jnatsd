package com.robaho.jnatsd.util;


import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import static com.robaho.jnatsd.util.JvmUtils.bufferAddress;
import static com.robaho.jnatsd.util.JvmUtils.unsafe;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

/**
 * buffered output stream to channel backed by direct byte buffer
 */
public
class ChannelOutputStream extends OutputStream {
    private final WritableByteChannel channel;
    private final ByteBuffer buffer;
    private final long address;
    private final int size;
    private int position;

    public ChannelOutputStream(WritableByteChannel channel, int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("Buffer size <= 0");
        }
        if(channel==null) {
            throw new IllegalArgumentException("channel is null");
        }
        this.channel = channel;
        this.buffer = ByteBuffer.allocateDirect(size);
        this.address = bufferAddress(buffer);
        this.size=size;
    }

    /** Flush the internal buffer */
    private void flushBuffer() throws IOException {
        if(position!=0) {
            buffer.position(position);
            buffer.flip();
            channel.write(buffer);
            while(buffer.hasRemaining()) {
                Thread.yield();
                channel.write(buffer);
            }
            buffer.clear();
            position=0;
        }
    }

    /**
     * Writes the specified byte to this buffered output stream.
     *
     * @param      b   the byte to be written.
     * @exception  IOException  if an I/O error occurs.
     */
    public void write(int b) throws IOException {
        if(position==size) {
            flush();
        }
        unsafe.putByte(address+position++,(byte)b);
    }
    public void write(byte b[], int off, int len) throws IOException {
        if(len > size-position) {
            flush();
        }
        if(len>=size) {
            channel.write(ByteBuffer.wrap(b,off,len));
        } else {
            if(len<8) {
                for(int i=0;i<len;i++) {
                    unsafe.putByte(address+position++,b[off++]);
                }
            } else {
                copyMemory(b, ARRAY_BYTE_BASE_OFFSET + off, null, address + position, len);
                position+=len;
            }
        }
    }
    public void flush() throws IOException {
        flushBuffer();
    }
    private static void copyMemory(Object src, long srcAddress, Object dest, long destAddress, int length)
    {
        // The Unsafe Javadoc specifies that the transfer size is 8 iff length % 8 == 0
        // so ensure that we copy big chunks whenever possible, even at the expense of two separate copy operations
        int bytesToCopy = length - (length % 8);
        unsafe.copyMemory(src, srcAddress, dest, destAddress, bytesToCopy);
        unsafe.copyMemory(src, srcAddress + bytesToCopy, dest, destAddress + bytesToCopy, length - bytesToCopy);
    }
}

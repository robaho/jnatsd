package com.robaho.jnatsd;

import org.junit.Test;

import java.io.IOException;

import static junit.framework.TestCase.assertEquals;

public class ServerTest {
    @Test
    public void testLifeCycle() throws InterruptedException, IOException {
        Server server = new Server(4222);
        server.start();

        Thread.sleep(1000);
        int tcount = Thread.activeCount();

        server.stop();
        Thread.sleep(1000);

        assertEquals(tcount,Thread.activeCount());

    }
}

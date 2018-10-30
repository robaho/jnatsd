package com.robaho.jnatsd;

import com.robaho.jnatsd.util.CharSeq;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;

public class CharSeqTest {
    @Test
    public void testCharSeq() {
        CharSeq c1 = new CharSeq("hello");
        CharSeq c2 = new CharSeq("hello");
        CharSeq c3 = new CharSeq("bye");

        assertEquals(c1,c1);
        assertEquals(c1,c2);
        assertFalse(c2.equals(c3));

        CharSeq[] a = new CharSeq[2];

        CharSeq seq = new CharSeq("This is");
        seq.split(a);
        assertEquals(a[0],new CharSeq("This"));
        assertEquals(a[1],new CharSeq("is"));
    }
}

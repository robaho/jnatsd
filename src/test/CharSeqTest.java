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
    }

    @Test
    public void testSplit() {
        CharSeq c1 = new CharSeq("this is a string");
        CharSeq[] a = new CharSeq[4];
        c1.split(a);

        assertEquals(new CharSeq("this"),a[0]);
        assertEquals(new CharSeq("is"),a[1]);
        assertEquals(new CharSeq("a"),a[2]);
        assertEquals(new CharSeq("string"),a[3]);

        c1 = new CharSeq("this \"is a\" string");
        c1.split(a);

        assertEquals(new CharSeq("this"),a[0]);
        assertEquals(new CharSeq("\"is a\""),a[1]);
        assertEquals(new CharSeq("string"),a[2]);
    }

}

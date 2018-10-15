package com.robaho.jnatsd;

import org.junit.Test;

import java.util.Arrays;

import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.assertFalse;

public class SubscriptionTest {
    @Test
    public void testSubscription() {
        Subscription s = new Subscription(null,1,"*","");
        assertTrue(s.matches(new Subscription(null,0,"any","")));
        assertFalse(s.matches(new Subscription(null,0,"any.any","")));

        s = new Subscription(null,1,"some.*","");
        assertFalse(s.matches(new Subscription(null,0,"any","")));
        assertTrue(s.matches(new Subscription(null,0,"some.any","")));

        s = new Subscription(null,1,"some.any.*","");
        assertFalse(s.matches(new Subscription(null,0,"any","")));
        assertFalse(s.matches(new Subscription(null,0,"some.any","")));
        assertTrue(s.matches(new Subscription(null,0,"some.any.any","")));

        s = new Subscription(null,1,"_INBOX.PNYNUPZY6TY0IBFARYXAJU.*","");
        assertTrue(s.matches(new Subscription(null,0,"_INBOX.PNYNUPZY6TY0IBFARYXAJU.PNYNUPZY6TY0IBFARYXAMB","")));

        s = new Subscription(null,1,"some.>","");
        assertFalse(s.matches(new Subscription(null,0,"any","")));
        assertTrue(s.matches(new Subscription(null,0,"some.any","")));
        assertTrue(s.matches(new Subscription(null,0,"some.any.any","")));
    }

    @Test
    public void testSorting() {
        Subscription[] subs = new Subscription[8];
        subs[0] = new Subscription(null,1,"main","");
        subs[1] = new Subscription(null,1,"second","");
        subs[2] = new Subscription(null,1,">","");
        subs[3] = new Subscription(null,1,"*","");
        subs[4] = new Subscription(null,1,"main.*","");
        subs[5] = new Subscription(null,2,"main.*","");
        subs[6] = new Subscription(null,1,"main.>","");
        subs[7] = new Subscription(null,1,"main.*.third","");

        Arrays.sort(subs);

        for(Subscription s : subs){
            System.out.println(s.subject);
        }
    }
}

package com.kakaobank.pretest.framework.record;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class PairRecordTest {
    private static final Logger logger = LoggerFactory.getLogger(PairRecordTest.class.getName());

    private final Pair<String,String> testPairRecord1 = new PairRecord<>("key1", "value1");
    private final Pair<String,String> testPairRecord2 = new PairRecord<>("key2", "value2");
    private final Pair<String,String> testPairRecord3 = new PairRecord<>("key1", "value1");

    @Test
    public void testPairRecordGetKeyAndGetValue() {
        assertEquals(testPairRecord1.getKey(), "key1");
        assertNotEquals(testPairRecord2.getKey(), "key1");
        assertEquals(testPairRecord1.getValue(), "value1");
        assertNotEquals(testPairRecord2.getValue(), "value1");
    }

    @Test
    public void testPairRecordMakeString() {
        assertEquals(testPairRecord1.mkString(), "key1,value1");
        assertEquals(testPairRecord1.mkString("\t"), "key1\tvalue1");
        assertNotEquals(testPairRecord2.mkString(), "key1,value1");
        assertNotEquals(testPairRecord2.mkString("\t"), "key2,value2");
    }

    @Test
    public void testPairRecordEqual() {
        assertEquals(testPairRecord1, testPairRecord1);
        assertNotEquals(testPairRecord1, testPairRecord2);
        assertEquals(testPairRecord1, testPairRecord3);
        assertEquals(testPairRecord1.equals(testPairRecord3), true);
        assertEquals(testPairRecord1.equals(testPairRecord2), false);
    }
}
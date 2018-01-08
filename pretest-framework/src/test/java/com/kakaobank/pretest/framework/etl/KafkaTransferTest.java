package com.kakaobank.pretest.framework.etl;

import com.kakaobank.pretest.framework.TestHelper;
import com.kakaobank.pretest.framework.record.Pair;
import com.kakaobank.pretest.framework.source.KafkaSource;
import com.kakaobank.pretest.framework.source.KafkaSourceTest;
import com.kakaobank.pretest.framework.util.KafkaFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.*;

public class KafkaTransferTest {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSourceTest.class.getName());

    private final TestHelper testHelper = TestHelper.getInstance();

    final String fromTopic = "framework-unit-test-from-topic1";
    final String toTopic = "framework-unit-test-to-topic1";

    private KafkaSource<String, String> fromKafkaSource;
    private KafkaSource<String, String> toKafkaSource;
    private KafkaTransfer kafkaFromToMover;
    private KafkaTransfer kafkaToFromMover;

    private final int testRecordCount = 100;
    private final int testLimitTimeSec = 3;
    private final int testIntervalTimeSec = 1;
    private final int testMaxThreadCount = 10;
    private final int testRepeatCount = 3;
    private final int testConcurLevel = 3;
    private final List testSampleRecords = testHelper.createSampleStringPairRecord(testRecordCount);

    @Before
    public void setUp() {
        final Properties kafkaClientProps = testHelper.readProps("conf/test/test_kafka_client.properties");
//        final Properties kafkaProducerProps = testHelper.readProps("conf/test/test_kafka_producer.properties");
//        final Properties kafkaConsumerProps = testHelper.readProps("conf/test/test_kafka_consumer.properties");

        fromKafkaSource = new KafkaSource<>(fromTopic, new KafkaFactory<>(kafkaClientProps));
        toKafkaSource = new KafkaSource<>(toTopic, new KafkaFactory<>(kafkaClientProps));
        kafkaFromToMover = new KafkaTransfer(fromKafkaSource, toKafkaSource);
        kafkaToFromMover = new KafkaTransfer(toKafkaSource, fromKafkaSource);

        while(true) {
            List readRecords = fromKafkaSource.read(testLimitTimeSec);
            if(readRecords.size() <= 0) break;
        }

        while(true) {
            List readRecords = toKafkaSource.read(testLimitTimeSec);
            if(readRecords.size() <= 0) break;
        }
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testFromToSourceWriteAndRead() {
        assertTrue(fromKafkaSource.asyncWrite(testSampleRecords));
        assertTrue(toKafkaSource.asyncWrite(testSampleRecords));

        List<Pair<String, String>> fromKafkaSourceReadRecords = fromKafkaSource.read(testLimitTimeSec);
        List<Pair<String, String>> toKafkaSourceReadRecords = toKafkaSource.read(testLimitTimeSec);

        assertEquals(fromKafkaSourceReadRecords.size() + toKafkaSourceReadRecords.size(), testRecordCount * 2);
    }

    @Test
    public void testMoveAndParalleMoveTest() {
        assertTrue(fromKafkaSource.asyncWrite(testSampleRecords));
        kafkaFromToMover.move(testLimitTimeSec, testIntervalTimeSec);
        assertEquals(toKafkaSource.read(testLimitTimeSec).size(), testRecordCount);

        assertTrue(toKafkaSource.asyncWrite(testSampleRecords));
        kafkaToFromMover.move(testLimitTimeSec, testIntervalTimeSec, testConcurLevel, testMaxThreadCount);
        assertEquals(fromKafkaSource.read(testLimitTimeSec).size(), testRecordCount);

        assertTrue(fromKafkaSource.asyncWrite(testSampleRecords));
        kafkaFromToMover.parallelMove(testLimitTimeSec, testIntervalTimeSec, testConcurLevel, testMaxThreadCount);
        assertEquals(toKafkaSource.read(testLimitTimeSec).size(), testRecordCount);

    }

    @Test
    public void testDistinctMoveAndParallelDistinctMoveTest() {
        for(int i = 0; i < testRepeatCount; i++) {
            assertTrue(fromKafkaSource.asyncWrite(testSampleRecords));
        }

        kafkaFromToMover.distinctMove(testLimitTimeSec, testIntervalTimeSec);
        assertEquals(toKafkaSource.read(testLimitTimeSec).size(), testRecordCount);

        for(int i = 0; i < testRepeatCount; i++) {
            assertTrue(toKafkaSource.asyncWrite(testSampleRecords));
        }

        kafkaToFromMover.distinctMove(testLimitTimeSec, testIntervalTimeSec, testConcurLevel, testMaxThreadCount);
        assertEquals(fromKafkaSource.read(testLimitTimeSec).size(), testRecordCount);

        for(int i = 0; i < testRepeatCount; i++) {
            assertTrue(fromKafkaSource.asyncWrite(testSampleRecords));
        }

        kafkaFromToMover.parallelDistinctMove(testLimitTimeSec, testIntervalTimeSec, testConcurLevel, testMaxThreadCount);
        assertEquals(toKafkaSource.read(testLimitTimeSec).size(), testRecordCount);
    }
}
package com.kakaobank.pretest.framework.source;

import com.kakaobank.pretest.framework.TestHelper;
import com.kakaobank.pretest.framework.record.Pair;
import com.kakaobank.pretest.framework.util.KafkaFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

import static org.junit.Assert.*;

public class KafkaSourceTest {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSourceTest.class.getName());

    // 테스트용 사전 설정
    private final String topic = "framework-unit-test-topic1";
    private final TestHelper testHelper = TestHelper.getInstance();

    // 테스트 레코드 사이즈
    private final int testRecordCount = 100;
    // 테스트 read tiem out
    private final int testLimitTimeSec = 3;
    // 병렬 테스트를 위한 pool size
    private final int testMaxThreadCount = 10;
    // 반복 테스트를 위한 반복 횟수
    private final int testRepeatCount = 3;
    // 병렬 테스트를 위한 동시 실행 레벨
    private final int testConcurLevel = 3;
    // 미리 생성 된 테스트 셋
    private final List testSampleRecords = testHelper.createSampleStringPairRecord(testRecordCount);

    // 테스트용 kafka source
    private KafkaSource<String, String> kafkaSource;

    @Before
    public void setUp() throws Exception {
        final Properties kafkaClientProps = testHelper.readProps("conf/test/test_kafka_client.properties");

        kafkaSource = new KafkaSource<>(topic, new KafkaFactory<>(kafkaClientProps));

        // 토픽에 레코드가 남아 있다면 모두 소진
        while(true) {
            List readRecords = kafkaSource.read(testLimitTimeSec);
            if(readRecords.size() <= 0) break;
        }
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testSingleWriteAndSingleRead() {
        assertTrue(kafkaSource.write(testSampleRecords.iterator()));

        List<Pair<String, String>> readRecords = kafkaSource.read(testLimitTimeSec);
        int count = readRecords.size();

        assertEquals(count, testRecordCount);
    }

    @Test
    public void testAsyncWriteAndSingleRead() {
        assertTrue(kafkaSource.asyncWrite(testSampleRecords));

        List<Pair<String, String>> readRecords = kafkaSource.read(testLimitTimeSec);
        int count = readRecords.size();

        assertEquals(count, testRecordCount);
    }

    @Test
    public void testGetTopic() {
        assertEquals(kafkaSource.getTopic(), topic);
    }

    @Test
    public void testParallelAsyncWriteAndSingleRead() {
        ExecutorService pool = Executors.newFixedThreadPool(testMaxThreadCount);
        ArrayList<Future> futureList = new ArrayList();

        for(int i = 0; i < testConcurLevel; i++) {
            futureList.add(pool.submit((Callable) () -> kafkaSource.asyncWrite(testSampleRecords)));
        }

        while(true) {
            if(testHelper.allFuterIsDone(futureList))
                break;
        }

        int totalCount = 0;
        while(true) {
            int count = kafkaSource.read(testLimitTimeSec).size();
            totalCount = totalCount + count;
            if(count <= 0) {
                break;
            }
        }

        assertEquals(totalCount, testRecordCount * testConcurLevel);
        pool.shutdown();
    }

    @Test
    public void testParallelWriteAndSingleRead() {
        kafkaSource.write(testHelper.createSampleStringPairRecord(testRecordCount * testConcurLevel), testConcurLevel, testMaxThreadCount);
        assertEquals(kafkaSource.read(testLimitTimeSec).size(),testRecordCount * testConcurLevel);
    }


    @Test
    public void testAsyncWriteParallelReadByManual() throws ExecutionException, InterruptedException {
        kafkaSource.asyncWrite(testHelper.createSampleStringPairRecord(testRecordCount * testRepeatCount));

        ExecutorService pool = Executors.newFixedThreadPool(testMaxThreadCount);
        ArrayList<Future> futureList = new ArrayList();

        for(int i = 0; i < testRepeatCount; i++) {
            futureList.add(pool.submit((Callable) () -> kafkaSource.read(testLimitTimeSec)));
        }

        while(true) {
            if(testHelper.allFuterIsDone(futureList))
                break;
        }

        int count = 0;
        for(Future<List> future : futureList) {
            count = count + future.get().size();
        }

        assertEquals(count, testRecordCount * testRepeatCount);
        pool.shutdown();
    }

    @Test
    public void testAsyncWriteParallelReadByMethod() {
        kafkaSource.asyncWrite(testHelper.createSampleStringPairRecord(testRecordCount * testRepeatCount));
        assertEquals(kafkaSource.read(testLimitTimeSec, 3, testConcurLevel, testMaxThreadCount).size(),
                testRecordCount * testRepeatCount);
    }

    @Test
    public void testParallelWriteAndRead() throws ExecutionException, InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(testMaxThreadCount);

        ArrayList<Future> writeList = new ArrayList();
        ArrayList<Future> readList = new ArrayList();

        for(int i = 0; i < testRepeatCount; i++) {
            writeList.add(pool.submit((Callable) () -> kafkaSource.asyncWrite(testSampleRecords)));
            readList.add(pool.submit((Callable) () -> kafkaSource.read(testLimitTimeSec)));
        }

        while(true) {
            if(testHelper.allFuterIsDone(writeList) && testHelper.allFuterIsDone(readList))
                break;
        }

        int count = 0;
        for(Future<List> future : readList) {
            count = count + future.get().size();
        }

        assertEquals(count, testRecordCount * testRepeatCount);
        pool.shutdown();
    }
}
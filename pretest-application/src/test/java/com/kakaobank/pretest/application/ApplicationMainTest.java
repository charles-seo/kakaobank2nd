package com.kakaobank.pretest.application;

import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import com.kakaobank.pretest.application.dao.EventDAO;
import com.kakaobank.pretest.framework.source.KafkaSource;
import com.kakaobank.pretest.framework.util.KafkaFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

/**
 * 어플리케이션 메인 테스트 케이스
 *
 * 어플리케이션 실행 시간 동안
 *
 * 하나의 스레드가 시나리오에 따라 100개의 레코드 셋을 생성하여 from topic에 저장
 * from topic에서 pipe line을 거쳐 mysql에 저장된 내용을 확인
 *
 * 테스트 시간 절약을 위해 각 pipe 단계별 시간은 30 60 120 180으로 설정
 *
 * 테스트케이스별 application 설정 파일로 진행
 *
 * testcase1 = conf/test/testcase1-application.properties
 * testcase2 = conf/test/testcase1-application.properties
 *
 */
public class ApplicationMainTest {
    private TestHelper testHelper;

    private DB db;

    private KafkaSource<String, String> fromKafkaSource;
    private KafkaSource<String, String> toKafkaSource;

    private EventDAO eventDAO;

    private List testSampleRecords;

    private int testRecordCount = 100;

    @Before
    public void setUp() throws Exception {
        // 테스트 핼프 객체 생성
        testHelper = TestHelper.getInstance();
        testSampleRecords = TestHelper.getInstance().createSampleKafkaRecord(testRecordCount);

        // kafka test Source 생성
        fromKafkaSource = new KafkaSource<>("test-application-from-topic", new KafkaFactory<>(testHelper.readProps("conf/test/test_from_kafka_client.properties")));
        toKafkaSource = new KafkaSource<>("test-application-to-topic", new KafkaFactory<>(testHelper.readProps("conf/test/test_to_kafka_client.properties")));

        // event record mysql dao 생성
        eventDAO = new EventDAO(testHelper.readProps("conf/test/test-mysql.properties"));

        // mysql 폴더 정리
        if(Files.exists(Paths.get("tmp/tmp_mysql_dir")))  testHelper.deleteAllFiles("tmp/tmp_mysql_dir");

        // mysql 테스트 서버 실행
        db = DB.newEmbeddedDB(DBConfigurationBuilder.newBuilder()
                .setPort(3306)
                .setBaseDir("tmp/tmp_mysql_dir")
                .build()
        );

        db.start();
        db.createDB("kakaobank-pretest");
        eventDAO.createTable();

        // 토픽에 레코드가 남아 있다면 모두 소진
        while(true) {
            List readRecords = fromKafkaSource.read(1);
            if(readRecords.size() <= 0) break;
        }
        while(true) {
            List readRecords = toKafkaSource.read(1);
            if(readRecords.size() <= 0) break;
        }
    }

    @After
    public void tearDown() throws Exception {
        // 테스트 mysql 중지
        if( db != null) db.stop();

        // 테스트 mysql 폴더 정리
        testHelper.deleteAllFiles("tmp/tmp_mysql_dir");
    }

    /**
     * 케이스 1
     *
     * 카프카의 들어 오는 데이터는 다음과 같이 가정
     * key : event_id (tab) event_timestamp
     * vale : event_id (tab) event_timestamp (tabl) service_code (tab) event_context
     *
     * pipe step은 10초 -> 30초 -> 60초 순으로 진행
     * 어플리케이션은 60초동안 실행
     *
     * from 소스에 동일한 100건의 레코드 셋을 1초 간격으로 세번 생성
     *
     * 최종 목적지에는 중복이 제거된 레코드셋 1건이 저장되어야 성공
     *
     * conf/test/testcase1-application.properties
     *
     */
    @Test
    public void testCase1() throws SQLException, ClassNotFoundException {
        final int sampleRecordCount = 100;
        final ExecutorService pool = Executors.newFixedThreadPool(2);

        final List sampleList = TestHelper.getInstance().createSampleKafkaRecord(sampleRecordCount);

        final String[] args = {"conf/test/testcase1-application.properties"};

        fromKafkaSource.asyncWrite(sampleList);

        pool.submit(() -> {
          for(int i = 0; i < 3; i++) {
              fromKafkaSource.asyncWrite(sampleList);
              try {
                  Thread.sleep(1000L);
              } catch (InterruptedException e) {
                  e.printStackTrace();
              }
          }
        });

        AppMain.main(args);
        assertEquals(0, toKafkaSource.read(1).size());
        assertEquals(100, eventDAO.getRecordCount());
    }

    /**
     * 케이스 2
     *
     * 카프카의 들어 오는 데이터는 다음과 같이 가정
     * key : event_id (tab) event_timestamp
     * vale : event_id (tab) event_timestamp (tabl) service_code (tab) event_context
     *
     * pipe step은 10초 -> 30초 -> 60초 순으로 진행
     * 어플리케이션은 120초동안 실행
     *
     * from 소스에 동일한 100건의 레코드 셋을 1초 간격으로 세번 생성
     * 그리고 1분 후에 100건의 레코드셋을 1초 간격으로 세번 생성
     *
     * 최종 목적지에는 중복이 제거된 레코드셋 2건이 저장되어야 성공
     *
     * conf/test/testcase2-application.properties
     *
     */
    @Test
    public void testCase2() throws SQLException, ClassNotFoundException {
        final ExecutorService pool = Executors.newFixedThreadPool(2);

        final String[] args = {"conf/test/testcase2-application.properties"};


        pool.submit(() -> {
            // 테스트 데이터 저장
            for(int i = 0; i < 3; i++) {
                fromKafkaSource.asyncWrite(testSampleRecords);
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            // 1분 대기
            try {
                Thread.sleep(60000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // 테스트 데이터 저장
            for(int i = 0; i < 3; i++) {
                fromKafkaSource.asyncWrite(testSampleRecords);
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        AppMain.main(args);
        assertEquals(0, toKafkaSource.read(3).size());
        assertEquals(200, eventDAO.getRecordCount());
    }
}
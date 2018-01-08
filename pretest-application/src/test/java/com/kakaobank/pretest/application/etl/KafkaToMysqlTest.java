package com.kakaobank.pretest.application.etl;

import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import com.kakaobank.pretest.application.TestHelper;
import com.kakaobank.pretest.application.dao.EventDAO;
import com.kakaobank.pretest.framework.source.KafkaSource;
import com.kakaobank.pretest.framework.util.KafkaFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class KafkaToMysqlTest {
    private TestHelper testHelper;

    private DB db;

    private KafkaSource<String, String> fromKafkaSource;
    private KafkaSource<String, String> toKafkaSource;
    private KafkaToMysql kafkaToMysql;

    private EventDAO eventDAO;

    private List testSampleRecords;

    private int testRecordCount = 100;

    @Before
    public void setUp() throws Exception {
        // 테스트 핼프 객체 생성
        testHelper = TestHelper.getInstance();
        testSampleRecords = TestHelper.getInstance().createSampleKafkaRecord(testRecordCount);

        // kafka test Source 생성
        fromKafkaSource = new KafkaSource<>("test-application-from-topic",
                new KafkaFactory<>(testHelper.readProps("conf/test/test_from_kafka_client.properties")));
        toKafkaSource = new KafkaSource<>("test-application-to-topic",
                new KafkaFactory<>(testHelper.readProps("conf/test/test_to_kafka_client.properties")));

        // kafka to mysql 생성
        kafkaToMysql = new KafkaToMysql("test-application-from-topic",
                testHelper.readProps("conf/test/test_from_kafka_client.properties"),
                testHelper.readProps("conf/test/test-mysql.properties"));

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

    @Test
    public void testKafkaToMysqlMove() throws Exception {
        assertTrue(fromKafkaSource.asyncWrite(testSampleRecords));
        kafkaToMysql.move(3, 3);
        assertEquals(testRecordCount, eventDAO.getRecordCount());
    }
}

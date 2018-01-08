package com.kakaobank.pretest.application.record;

import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import com.kakaobank.pretest.application.TestHelper;
import com.kakaobank.pretest.application.dao.EventDAO;
import com.kakaobank.pretest.application.etl.KafkaToMysql;
import com.kakaobank.pretest.framework.record.PairRecord;
import com.kakaobank.pretest.framework.source.KafkaSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Event Record 테스트
 */
public class EventRecordAndEventDAOTest {
    private TestHelper testHelper;

    private final int testRecordCount = 100;

    private DBConfigurationBuilder configBuilder;
    private DB db;

    private Properties mysqlProps;
    private EventDAO eventDAO;

    private List<PairRecord<String, String>> testSampleRecords;


    @Before
    public void setUp() throws Exception {
        // mysql 폴더 정리
        if(Files.exists(Paths.get("tmp/tmp_mysql_dir")))  testHelper.deleteAllFiles("tmp/tmp_mysql_dir");

        testHelper = TestHelper.getInstance();
        testSampleRecords = testHelper.createSampleKafkaRecord(testRecordCount);

        configBuilder = DBConfigurationBuilder.newBuilder()
                .setPort(3306)
                .setBaseDir("tmp/tmp_mysql_dir")
                .setDataDir("tmp/tmp_mysql_dir/data_dir")
                .setLibDir("tmp/tmp_mysql_dir/lib");

        db = DB.newEmbeddedDB(configBuilder.build());

        db.start();
        db.createDB("kakaobank-pretest");


        mysqlProps = testHelper.readProps("conf/test/test-mysql.properties");
        eventDAO = new EventDAO(mysqlProps);
        eventDAO.createTable();
    }

    @After
    public void tearDown() throws Exception {
        if( db != null) db.stop();
        testHelper.deleteAllFiles("tmp/tmp_mysql_dir");
    }

    @Test
    public void testAddAndGetAndCount() throws Exception {
        EventRecord record1 = new EventRecord((long) 1,
                "eventTimeStamp" + 1,
                "serviceCode" + 1,
                "eventContext" + 1);
        EventRecord record2 = new EventRecord((long) 2,
                "eventTimeStamp" + 2,
                "serviceCode" + 2,
                "eventContext" + 2);
        assertTrue(record1.equals(record1));
        assertFalse(record1.equals(record2));


        eventDAO.add(record1);
        List<EventRecord> readRecords = (eventDAO.get((long) 1, "eventTimeStamp" + 1));
        EventRecord record = readRecords.get(0);
        assertTrue(record.equals(record1));

        eventDAO.add(record1);
        assertEquals(eventDAO.get((long) 1, "eventTimeStamp" + 1).size(), 2);

        eventDAO.add(record2);
        assertEquals(eventDAO.get((long) 1, "eventTimeStamp" + 1).size(), 2);
        assertEquals(eventDAO.get((long) 2, "eventTimeStamp" + 2).size(), 1);

        assertEquals(eventDAO.getRecordCount(), 3);
    }

    @Test
    public void testAddList() throws SQLException, ClassNotFoundException {
        eventDAO.add(testSampleRecords.stream().map(EventRecord::new).collect(Collectors.toList()));
        assertEquals(testRecordCount, eventDAO.getRecordCount());
    }
}
package com.kakaobank.pretest.application.dao;

import com.kakaobank.pretest.application.record.EventRecord;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * Event 테이블 CRUD 객체
 *
 */
public class EventDAO {
    private static final Logger logger = Logger.getLogger(EventDAO.class.getName());

    final Properties mysqlProps;
    final String insertQuery = "insert into events(event_id, event_timestamp, service_code, event_context) values(?,?,?,?)";
    final String selectQuery = "select * from events where event_id = ? and event_timestamp = ?";
    final String countQuery = "select count(event_id) from events";

    public EventDAO(Properties mysqlProps) {
        this.mysqlProps = mysqlProps;
    }

    /**
     * Insert 메소드
     *
     * @param record Intert target record
     * @return insert 성공시 true 아니면 flase
     */
    public boolean add(EventRecord record) {
        logger.info("insert single record : " + record);
        int resultCount = 0;

        try ( Connection c = this.createConnection();
              PreparedStatement ps = c.prepareStatement(insertQuery)
        ) {
            ps.setLong(1, record.getEventID());
            ps.setString(2, record.getEventTimestamp());
            ps.setString(3, record.getServiceCode());
            ps.setString(4, record.getEventContext());

            resultCount = ps.executeUpdate();
        } catch (SQLException | ClassNotFoundException e) {
            logger.error("event message create connection faile. ", e);
            e.printStackTrace();
        }

        logger.info("insert record complete : " + resultCount);
        return resultCount == 1;
    }

    /**
     * 복수개의 Insert 메소드
     * 오토 커밋을 off 한 후 batch처리
     *
     * @param records Insert 목록
     * @return insert 성공시 true 아니면 flase
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public boolean add(List<EventRecord> records) throws SQLException, ClassNotFoundException {
        final int recordCount = records.size();
        logger.info("insert records start. record count " + recordCount);

        if(recordCount == 0) {
            logger.info("insert records complete. : record count 0");
            return true;
        }

        Connection c = this.createConnection();
        PreparedStatement ps = c.prepareStatement(insertQuery);

        c.setAutoCommit(false);

        for(EventRecord record : records) {
            ps.setLong(1, record.getEventID());
            ps.setString(2, record.getEventTimestamp());
            ps.setString(3, record.getServiceCode());
            ps.setString(4, record.getEventContext());
            ps.addBatch();
        }

        int[] resultSet = ps.executeBatch();
        c.commit();

        logger.info("insert records complete. : record count " + resultSet.length);
        return resultSet.length == recordCount;
    }

    /**
     * Select 메소드
     *
     * @param event_id target record column value
     * @param event_timestamp target record column value
     * @return EventRecord 객체 리스트
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public List<EventRecord> get(long event_id, String event_timestamp) throws ClassNotFoundException, SQLException {
        List<EventRecord> readRecords = new LinkedList<>();

        Connection c = this.createConnection();
        PreparedStatement ps = c.prepareStatement(selectQuery);

        ps.setLong(1, event_id);
        ps.setString(2, event_timestamp);

        ResultSet rs = ps.executeQuery();

        while(rs.isLast() == false) {
            rs.next();
            readRecords.add(new EventRecord(rs.getLong("event_id"),
                    rs.getString("event_timestamp"),
                    rs.getString("service_code"),
                    rs.getString("event_context")));
        }

        return readRecords;
    }

    /**
     * 총 로우 수를 구하는 메소드
     *
     * @return Event테이블의 전체 레코드 수
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public int getRecordCount() throws SQLException, ClassNotFoundException {
        Connection c = this.createConnection();
        PreparedStatement ps = c.prepareStatement(countQuery);
        ResultSet resultSet = ps.executeQuery();

        resultSet.next();
        return resultSet.getInt(1);
    }

    public void createTable() throws SQLException, ClassNotFoundException {
        Connection c = this.createConnection();
        String createTableQuery = "create table events " +
                "(event_id bigint not null, " +
                " event_timestamp varchar(255) not null, " +
                " service_code varchar(255), " +
                " event_context varchar(255));";

        PreparedStatement ps = c.prepareStatement(createTableQuery);
        ps.executeUpdate();
    }

    /**
     * mysql connection 생성 메소드
     *
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    private Connection createConnection() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        return DriverManager.getConnection(
                mysqlProps.getProperty("url") + "/" + mysqlProps.getProperty("db"),
                mysqlProps.getProperty("user"),
                mysqlProps.getProperty("password"));
    }

}

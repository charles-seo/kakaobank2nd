package com.kakaobank.pretest.application.etl;

import com.kakaobank.pretest.application.dao.EventDAO;
import com.kakaobank.pretest.application.record.EventRecord;
import com.kakaobank.pretest.framework.record.PairRecord;
import com.kakaobank.pretest.framework.source.KafkaSource;
import com.kakaobank.pretest.framework.util.KafkaFactory;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * 카프카 소스에서 mysql로 이동하는 클래스
 *
 */
public class KafkaToMysql {
    private static final Logger logger = Logger.getLogger(KafkaToMysql.class.getName());

    final KafkaSource fromKafkaSource;
    final EventDAO toMysql;

    public KafkaToMysql(String fromKafkaTopic, Properties fromKafkaProps,Properties toMysqlProps) {
        fromKafkaSource = new KafkaSource(fromKafkaTopic, new KafkaFactory<String, String>(fromKafkaProps));
        toMysql = new EventDAO(toMysqlProps);
    }

    /**
     * 클래스에 주어진 props 정보를 가지고 출발지 Kafka 서버에서 도착지 Mysql로 데이터 전송
     *
     * @param applicationTImeLimit 전송 제한 시간
     * @param limitTimeSec Kafka 소스를 읽는 제한시간
     * @return 전송 중 오류가 있다면 false 아니라면 true
     */
    public boolean move(long applicationTImeLimit, int limitTimeSec) {
        boolean result = true;
        do {
            try {
                final List<PairRecord> readRecords = fromKafkaSource.read(limitTimeSec);
                logger.info("move kafka to mysql by interval time : " + limitTimeSec);

                if(toMysql.add(readRecords.stream().map(EventRecord::new).collect(Collectors.toList())) == false) {
                    result = false;
                }

                logger.info("move record : " + readRecords.size());
            } catch (SQLException | ClassNotFoundException e) {
                e.printStackTrace();
                logger.error("mysql insert fail.", e);
            }
        } while(System.currentTimeMillis() < applicationTImeLimit);

        if(result) {
            logger.info("move from kafka to mysql complete");
        } else {
            logger.info("move from kafka to mysql complete with error : ");
        }
        return result;
    }
}

package com.kakaobank.pretest.framework.etl;

import com.kakaobank.pretest.framework.record.Pair;
import com.kakaobank.pretest.framework.record.PairRecord;
import com.kakaobank.pretest.framework.source.KafkaSource;
import org.apache.log4j.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 카프카 데이터 ETL 클래스
 * 카프카의 From 토픽에서 To 토픽으로 데이터를 이동
 * 또는 중복을 제거하고 이동
 */
public class KafkaTransfer implements Transfer {
    private static final Logger logger = Logger.getLogger(KafkaTransfer.class.getName());

    final KafkaSource<String, String> fromKafkaSource;
    final KafkaSource<String, String> toKafkaSource;
    final String fromTopic;
    final String toTopic;

    public KafkaTransfer(KafkaSource<String, String> fromKafkaSource, KafkaSource<String, String> toKafkaSource) {
        this.toKafkaSource = toKafkaSource;
        this.fromKafkaSource = fromKafkaSource;
        this.fromTopic = fromKafkaSource.getTopic();
        this.toTopic = toKafkaSource.getTopic();

    }

    /**
     * From 토픽에서 읽기 제한 시간 만큼 데이터를 읽어 To 토픽으로 데이터를 저장
     *
     * @param limitTimeSec 읽기 제한 시간
     * @param intervalTimeSec 처리 사이클 제한 시간
     * @return 데이터 쓰기 중 한번이라도 오류가 있다면 False 없다면 True
     */
    @Override
    public boolean move(int limitTimeSec, int intervalTimeSec) {
        logger.info("kafka move : " + fromTopic + " -> " + toTopic);
        boolean result = true;

        final long limitTImeMillis = System.currentTimeMillis() + limitTimeSec * 1000;
        while(true) {
            if(toKafkaSource.asyncWrite(fromKafkaSource.read(intervalTimeSec)) == false) result = false;
            if(System.currentTimeMillis() >= limitTImeMillis) {
                break;
            }
        }
        return result;
    }

    /**
     * move 의 읽기 과정을 병렬로 처리
     * From 토픽에서 읽기 제한 시간 만큼 병렬로 데이터를 읽어 To 토픽으로 데이터를 저장
     * 
     * @param limitTimeSec 읽기 제한 시간
     * @param intervalTimeSec 처리 사이클 제한 시간
     * @param concurLevel 동시 실행 레벨
     * @param maxExecutor thread pool size
     * @return 데이터 쓰기 중 한번이라도 오류가 있다면 false 없다면 true
     */
    public boolean move(int limitTimeSec, int intervalTimeSec, int concurLevel, int maxExecutor) {
        logger.info("kafka move with parallel read : " + fromTopic + " -> " + toTopic);
        boolean result = true;

        final long limitTImeMillis = System.currentTimeMillis() + limitTimeSec * 1000;
        while(true) {
            if(toKafkaSource.asyncWrite(fromKafkaSource.read(intervalTimeSec, intervalTimeSec, concurLevel, maxExecutor)) == false) result = false;

            if(System.currentTimeMillis() >= limitTImeMillis) {
                break;
            }
        }
        return result;
    }

    /**
     * move 의 읽기 쓰기 과정을 병렬로 처리
     * From 토픽에서 읽기 제한 시간 만큼 병렬로 데이터를 읽어 To 토픽으로 병렬로 데이터를 저장
     * 
     * @param limitTimeSec 읽기 제한 시간
     * @param intervalTimeSec 처리 사이클 제한 시간
     * @param concurLevel 동시 실행 레벨
     * @param maxExecutor thread pool size
     * @return 데이터 쓰기 중 한번이라도 오류가 있다면 false 없다면 true
     */
    public boolean parallelMove(int limitTimeSec, int intervalTimeSec, int concurLevel, int maxExecutor) {
        logger.info("kafka move with parallel read : " + fromTopic + " -> " + toTopic);
        boolean result = true;

        final long limitTImeMillis = System.currentTimeMillis() + limitTimeSec * 1000;
        while(true) {
            if(toKafkaSource.write(fromKafkaSource.read(intervalTimeSec, intervalTimeSec, concurLevel, maxExecutor), concurLevel, maxExecutor) == false) result = false;
            if(System.currentTimeMillis() >= limitTImeMillis) {
                break;
            }
        }
        return result;
    }

    /**
     * From 토픽에서 읽기 제한 시간 만큼 데이터를 읽어 중복을 제거한 후 To 토픽으로 데이터 저장
     *
     * @param limitTimeSec 읽기 제한 시간
     * @param intervalTimeSec 처리 사이클 제한 시간                     
     * @return 데이터 쓰기중 오류가 있다면 False 없다면 True
     */
    @Override
    public boolean distinctMove(int limitTimeSec, int intervalTimeSec) {
        logger.info("kafka distinct move : " + fromTopic + " -> " + toTopic);
        List resultSet = new LinkedList();

        final long limitTImeMillis = System.currentTimeMillis() + limitTimeSec * 1000;
        while(true) {
            resultSet.addAll(fromKafkaSource.read(intervalTimeSec)
                    .stream()
                    .map((Function<Pair, String>) Pair::mkString)
                    .distinct()
                    .map((String string) -> {
                        final String[] splitString = string.split(",");
                        return new PairRecord<>(splitString[0], splitString[1]);
                    })
                    .collect(Collectors.toList()));

            if(System.currentTimeMillis() >= limitTImeMillis) {
                break;
            }
        }

        logger.debug("kafka distinct move count : " + resultSet.size());

        return toKafkaSource.asyncWrite(resultSet);
    }

    /**
     * distincMove의 읽기과정을 병렬로 처리
     * From 토픽에서 읽기 제한 시간만큼 병렬로 데이터를 읽어 To 토픽으로 저장
     *
     * @param limitTimeSec 읽기 제한 시간
     * @param intervalTimeSec 처리 사이클 제한 시간
     * @param concurLevel 동시 실행 레벨
     * @param maxExecutor thread pool size
     * @return To 토픽에 쓰기 오류가 한번이라도 있다면 False 없다면 True
     */
    public boolean distinctMove(int limitTimeSec, int intervalTimeSec, int concurLevel, int maxExecutor) {
        logger.info("kafka distinct move with parallel read : " + fromTopic + " -> " + toTopic);
        final long limitTImeMillis = System.currentTimeMillis() + limitTimeSec * 1000;
        List resultSet = new LinkedList();

        resultSet = this.getDistincSetFromParallelRead(limitTimeSec, intervalTimeSec, concurLevel, maxExecutor);

        logger.debug("kafka distinct move count : " + resultSet.size());
        return toKafkaSource.asyncWrite(resultSet);
    }

    /**
     * distincMove의 읽기 쓰기 과정을 병렬로 처리
     * From 토픽에서 읽기 제한 시간만큼 병렬로 데이터를 읽어 중복을 제거한 후 To 토픽으로 병렬로 저장
     *
     * @param limitTimeSec 읽기 제한 시간
     * @param intervalTimeSec 처리 사이클 제한 시간
     * @param concurLevel 동시 실행 레벨
     * @param maxExecutor 최대 thread pool size
     * @return To 토픽에 쓰기 오류가 한번이라도 있다면 Fasle 없다면 True
     */
    public boolean parallelDistinctMove(int limitTimeSec, int intervalTimeSec, int concurLevel, int maxExecutor) {
        logger.info("kafka distinct move with parallel read : " + fromTopic + " -> " + toTopic);
        final long limitTImeMillis = System.currentTimeMillis() + limitTimeSec * 1000;
        final List<Pair<String, String>> resultSet;
        resultSet = this.getDistincSetFromParallelRead(limitTimeSec, intervalTimeSec, concurLevel, maxExecutor);

        logger.debug("kafka distinct move count : " + resultSet.size());
        return toKafkaSource.write(resultSet, concurLevel,maxExecutor);
    }

    /**
     * 카프카에서 데이터를 병렬로 읽어 중복을 제거한 결과를 리턴
     *
     * @param limitTimeSec 읽기 제한 시간
     * @param intervalTimeSec 처리 사이클 제한 시간
     * @param concurLevel 동시 실행 레벨
     * @param maxExecutor 최대 thread pool size
     * @return 중복이 제거된 pair 레코드 셋
     */
    private List getDistincSetFromParallelRead(int limitTimeSec, int intervalTimeSec, int concurLevel, int maxExecutor) {
        final long limitTImeMillis = System.currentTimeMillis() + limitTimeSec * 1000;
        final List<String> tempSet = new LinkedList();

        while(true) {
            tempSet.addAll(fromKafkaSource.read(intervalTimeSec, 1, concurLevel, maxExecutor)
                    .stream()
                    .map((Function<Pair, String>) Pair::mkString)
                    .distinct()
                    .collect(Collectors.toList()));

            if(System.currentTimeMillis() >= limitTImeMillis) {
                break;
            }
        }

        return tempSet.stream()
                .distinct()
                .map((String string) -> {
                    final String[] splitString = string.split(",");
                    return new PairRecord<>(splitString[0], splitString[1]);
                }).collect(Collectors.toList());
    }

}

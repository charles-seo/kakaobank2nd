package com.kakaobank.pretest.framework.source;

import com.kakaobank.pretest.framework.record.PairRecord;
import com.kakaobank.pretest.framework.util.KafkaFactory;
import org.apache.kafka.clients.consumer.*;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.kakaobank.pretest.framework.record.Pair;

/**
 * kafka source 클래스
 *
 * kafka consumer를 이용해 읽기를 구현
 * kafka producer를 이용해 쓰기르 구현
 *
 * consumer는 thread-safe하지 않기 때문에 consumer 객체를 스레드로 부터 독립시켜 필요할 때마다 생성하고
 * 필요한 레코드를 가져가 오프셋을 커밋하기 전까지는 consumer를 블락킹
 *
 * @param <K> kafka record key type
 * @param <V> kafka record value type
 */
public class KafkaSource<K, V> implements Source<Pair<K, V>> {
    private static final Logger logger = Logger.getLogger(KafkaSource.class.getName());

    private final String topic;
    private final KafkaFactory<K, V> kafkaFactory;

    public KafkaSource(String topic, KafkaFactory<K, V> kafkaFactory) {
        this.topic = topic;
        this.kafkaFactory = kafkaFactory;
    }

    /**
     * 카프카 데이터 읽기
     *
     * 주어진 시간동안 카프카의 토픽에서 데이터를 풀링
     * 풀링 타임 아웃을 1초로 잡아 최대 1초 설정된 최대 값의 데이터만 가져오는 동안만 스레드를 격리시키고
     * 이후 데이터를 처리함
     *
     * @param limitTimeSec 읽기 제한 시간
     * @return 읽어들인 카프카 레코드 리스트
     */
    @Override
    public List<Pair<K, V>> read(final int limitTimeSec) {
        final List<Pair<K, V>> resultSet = new LinkedList<>();
        final long limitTImeMillis = System.currentTimeMillis() + limitTimeSec * 1000;

        logger.debug("create kafka consumer. topic : " + this.topic);
        final KafkaConsumer<K, V> consumer = kafkaFactory.createConsumer(String.valueOf(Thread.currentThread().getId()));
        consumer.subscribe(Collections.singletonList(topic));

        // 제한 시간 까지 데이터를 풀링하여 결과 리스트에 합침
        while(true) {
            final ConsumerRecords<K, V> consumerRecords;

            // 풀링이 완료된 후 offset을 커밋할때까지 블락킹
            synchronized (consumer) {
                consumerRecords = consumer.poll(1000);
                consumer.commitSync();
                logger.debug("consume kafka " + topic + " record : " + consumerRecords.count());
            }

            // 풀링한 데이터가 있다면 결과 리스트에 데이터를 합침
            if (consumerRecords.isEmpty()) {
                logger.info("topic : " + topic + " read record from kafka source complete : kafka source is empty");
            } else {
                for(ConsumerRecord<K, V> consumerRecord : consumerRecords) {
                    resultSet.add(new PairRecord<>(consumerRecord.key(), consumerRecord.value()));
                    logger.debug(new StringBuilder().append("read kafka record :")
                            .append(" key = ").append(consumerRecord.key())
                            .append(" value = ").append(consumerRecord.value())
                            .append(" partition = ").append(consumerRecord.partition())
                            .append(" offset = ").append(consumerRecord.offset()));
                }
                logger.info("topic : " + topic + " read record from kafka source complete : kafka source is empty");
            }
            if(System.currentTimeMillis() >= limitTImeMillis) {
                break;
            }
        }
        consumer.close();
        logger.debug("close kafka consumer. topic : " + this.topic);
        logger.info("topic : " + topic + " parallel read complete");
        return resultSet;
    }

    /**
     * 병렬 카프카 읽기
     *
     * 주어진 시간동안 카프카의 토픽에서 데이터를 병렬로 풀링
     *
     * @param limitTimeSec 읽기 제한 시간
     * @param concurLevel 동시실행 레벨
     * @param maxExecutor 최대 thread pool size
     * @return 읽어 들인 카프카 레코드 리스트
     */
    public List<Pair<K, V>> read(final int limitTimeSec, final int intervalTimeSec, final int concurLevel, final int maxExecutor) {
        // 병렬 실행을 위한 thread pool 생성
        logger.debug("create executor pool");
        final ExecutorService pool = Executors.newFixedThreadPool(maxExecutor);
        final List<Future<List<Pair<K, V>>>> futureList = new ArrayList<>(concurLevel);
        final List<Pair<K, V>> resultSet = new LinkedList<>();

        final long limitTImeMillis = System.currentTimeMillis() + limitTimeSec * 1000;
        // 제한 시간 까지 데이터를 풀링하여 결과 리스트에 합침
        while(true) {
            // read 명령을 thread pool에 병렬 레벨 만큼 submit
            try {
                for(int i = 0; i < concurLevel; i++) {
                    futureList.add(pool.submit((Callable) () -> this.read(intervalTimeSec)));
                }

                // 결과 값을 받아 전부 결과 셋에 합침
                for (Future<List<Pair<K, V>>> future : futureList) {
                    final List<Pair<K, V>> readRecords = future.get();
                    if(readRecords.size() > 0) {
                        resultSet.addAll(readRecords);
                    }
                }
            } catch (InterruptedException | CancellationException | ExecutionException e) {
                logger.error("topic : " + topic + " parallel read exception fail", e);
                pool.shutdown();
                e.printStackTrace();
            }

            if(System.currentTimeMillis() >= limitTImeMillis) {
                break;
            }
        }

        if(pool.isShutdown() == false) pool.shutdown();

        logger.info("topic : " + topic + " parallel read complete");
        return resultSet;
    }

    /**
     * 하나의 레코드를 카프카에 쓰기
     *
     * PairRecord 객체를 통해 K-V 카프카 레코드에 저장
     * K-V의 타입에 맞게 ser-deser 객체를 propeties에 설정해 줘야함
     *
     * @param pairRecord K-V레코드를 표현하기 위한 객체
     * @return 쓰기가 성공하면 True 실패하면 False
     */
    @Override
    public boolean write(final Pair<K, V> pairRecord) {
        logger.debug("create kafka producer. topic : " + this.topic);
        final KafkaProducer<K, V> kafkaProducer = kafkaFactory.createProducer(String.valueOf(Thread.currentThread().getId()));

        final StringBuilder logString = new StringBuilder().append("write kafka record :")
                .append(" key = ").append(pairRecord.getKey())
                .append(" value = ").append(pairRecord.getValue());

        try {
            final RecordMetadata metadata = kafkaProducer
                    .send(new ProducerRecord<>(this.topic, pairRecord.getKey(), pairRecord.getValue()))
                    .get();

            logString.append(", success :")
                    .append(" partition = ").append(metadata.partition())
                    .append(" offset = ").append(metadata.offset());

            logger.debug(logString);
        } catch (InterruptedException e) {
            logger.error(logString.append(", fail : interruptedException"));
            e.printStackTrace();
            logger.info("topic : " + topic + " write record to kafka source complete with error");

            return false;
        } catch (ExecutionException e) {
            logger.error(logString.append(", fail : executionException"));
            e.printStackTrace();
            logger.info("topic : " + topic + " write record to kafka source complete with error");

            return false;
        } finally {
            kafkaProducer.flush();
            kafkaProducer.close();
            logger.debug("close kafka producer. topic : " + this.topic);
        }

        logger.info("topic : " + topic + " write record complete.");
        return true;
    }

    /**
     * 동기식 리스트 카프카 쓰기
     *
     * PairRecord 리스트를 받아 카프카에 쓰기
     * @param pairRecords
     * @return 쓰기에 한번이라도 오류가 있었다면 False 없다면 True
     */
    @Override
    public boolean write(final Iterator<Pair<K, V>> pairRecords) {
        logger.debug("create kafka producer. topic : " + this.topic);
        final KafkaProducer<K, V> kafkaProducer = kafkaFactory.createProducer(String.valueOf(Thread.currentThread().getId()));

        boolean result = true;
        long writeRecordCount = 0;

        logger.debug("write records to kafka source start");
        try {
            // 프로듀서를 한번 생성하여 모든 리스트를 동기식으로 카프카에 저장
            while(pairRecords.hasNext()) {
                final Pair<K, V> pairRecord = pairRecords.next();
                final StringBuilder logString = new StringBuilder().append("write kafka record :")
                        .append(" key = ").append(pairRecord.getKey())
                        .append(" value = ").append(pairRecord.getValue());

                try {
                    final RecordMetadata metadata = kafkaProducer
                            .send(new ProducerRecord<>(this.topic, pairRecord.getKey(), pairRecord.getValue()))
                            .get();

                    logString.append(", success :")
                            .append(" partition = ").append(metadata.partition())
                            .append(" offset = ").append(metadata.offset());

                    logger.debug(logString);
                    writeRecordCount++;
                } catch (InterruptedException e) {
                    logger.error(logString.append(", fail : interruptedException"), e);
                    e.printStackTrace();
                    result = false;
                } catch (ExecutionException e) {
                    logger.error(logString.append(", fail : executionException"), e);
                    e.printStackTrace();
                    result = false;
                }
            }
        } finally {
            kafkaProducer.flush();
            kafkaProducer.close();
            logger.debug("close kafka producer. topic : " + this.topic);
        }

        if(result) {
            logger.info("topic : " + topic + " write records to kafka source complete : " + writeRecordCount);
        } else {
            logger.error("topic : " + topic + " write records to kafka source complete with error : " + writeRecordCount);
        }

        return result;
    }

    /**
     * 비동기식 카프카 리스트 쓰기 메소드
     *
     * Pair Record 리스트를 받아 카프카 저장 결과를 기다리지 않고 어싱크하게 데이터를 저장하고 모두 저장이 완료되길 기다림
     * @param pairRecords
     * @return 쓰기에 한번이라도 오류가 있었다면 False 아니면 True
     */
    public boolean asyncWrite(final List<Pair<K, V>> pairRecords) {
        logger.debug("create kafka producer. topic : " + this.topic);
        final KafkaProducer<K, V> kafkaProducer = kafkaFactory.createProducer(String.valueOf(Thread.currentThread().getId()));
        final int totalRecords = pairRecords.size();

        // 데이터 처리 결과를 저장하기 위한 thread-safe 변수 생성
        final AtomicInteger writeRecordCount = new AtomicInteger(0);
        final CountDownLatch countDownLatch = new CountDownLatch(totalRecords);

        logger.debug("topic : " + topic + " async write records to kafka source start : " + pairRecords.size());
        try {
            for (Pair<K, V> pairRecord : pairRecords) {
                final StringBuilder logString = new StringBuilder().append("async write kafka record :")
                        .append(" key = ").append(pairRecord.getKey())
                        .append(" value = ").append(pairRecord.getValue());

                // 메세지를 쓰고 결과를 비동기 처리
                kafkaProducer
                        .send(new ProducerRecord<K, V>(this.topic, pairRecord.getKey(), pairRecord.getValue()),
                                (metadata, exception) -> {
                                    if (metadata == null) {
                                        logger.error(logString.append(", fail : interruptedException"), exception);
                                        exception.printStackTrace();
                                    } else {
                                        logString.append(", success :")
                                                .append(" partition = ").append(metadata.partition())
                                                .append(" offset = ").append(metadata.offset());
                                        logger.debug(logString);

                                        writeRecordCount.incrementAndGet();
                                    }
                                    countDownLatch.countDown();
                                });

            }
        } finally {
            kafkaProducer.flush();
            kafkaProducer.close();
            logger.debug("close kafka producer. topic : " + this.topic);
        }

        if(totalRecords == writeRecordCount.intValue()) {
            logger.info("topic : " + topic + " async write records to kafka source complete : " +  writeRecordCount.get() + "/" + totalRecords);
            return true;
        } else {
            logger.error("topic : " + topic + " async write records to kafka source complete with error : " +  writeRecordCount.get() + "/" + totalRecords);
            return false;
        }
    }

    /**
     * 병렬 비동기식 카프카 레코드 쓰기
     *
     * 비동기식 레코드 쓰기를 주어진 레코드를 병렬 레벨만금 잘라 병렬로 실행
     *
     * @param pairRecords 저장할 레코드 셋
     * @param concurLevel 동시 실행 레벨
     * @param maxExecutor thread pool size
     * @return 쓰기에 한번이라도 오류가 있었다면 Fasel 아니면 True
     */
    public boolean write(final List<Pair<K, V>> pairRecords, final int concurLevel, final int maxExecutor) {
        logger.debug("create executor pool");
        ExecutorService pool = Executors.newFixedThreadPool(maxExecutor);
        ArrayList<Future<Boolean>> futureList = new ArrayList<>();

        boolean result = true;

        final int maxIndex = pairRecords.size();
        final int partition = maxIndex / concurLevel;
        logger.debug("split list amount : " + partition);

        int startIndex = 0;
        int nextIndex = startIndex + partition;

        while (nextIndex <= maxIndex) {
            final int start = startIndex;
            final int end = nextIndex;
            // 나눠 저장할 사이즈 (Partition)만큼 리스트를 나눠 병렬로 처리
            logger.debug("split list " + start + ", " + end);
            futureList.add(pool.submit((Callable) () -> Boolean.valueOf(asyncWrite(pairRecords.subList(start, end)))));

            startIndex = nextIndex;
            if (nextIndex == maxIndex) {
                break;
            } else if (nextIndex + partition >= maxIndex) {
                nextIndex = maxIndex;
            } else
                nextIndex = nextIndex + partition;
        }

        for(Future<Boolean> future : futureList) {
            try {
                result = future.get().booleanValue();
            } catch (InterruptedException | ExecutionException e) {
                logger.error("topic : " + topic + " parallel write exception fail", e);
                e.printStackTrace();
            } finally {
                pool.shutdown();
                logger.debug("close executor pool");
            }
        }

        if(pool.isShutdown() == false) {
            pool.shutdown();
        }
        if(result) {
            logger.info("topic : " + topic + " parallel write complete.");
        } else {
            logger.info("topic : " + topic + " parallel write complete with write error.");
        }

        return result;
    }

    public String getTopic() {
        return topic;
    }

    public Properties getKafkaConsumerProps() {
        return this.kafkaFactory.getKafkaConsumerProps();
    }

    public Properties getKafkaProducerProps() {
        return this.kafkaFactory.getKafkaProducerProps();
    }
}

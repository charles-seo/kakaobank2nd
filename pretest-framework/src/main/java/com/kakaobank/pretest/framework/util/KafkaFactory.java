package com.kakaobank.pretest.framework.util;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * 카프카 Consumer와 Producer를 생성하기 위한 객체
 * @param <K> Key Type
 * @param <V> Value Type
 */
public class KafkaFactory<K, V> {
    private static final Logger logger = Logger.getLogger(KafkaFactory.class.getName());

//    private final Properties producerProps;
//    private final Properties consumerProps;
    private final Properties kafkaClientProps;
//    public KafkaFactory(Properties producerProp, Properties consumerProp ) {
//        this.producerProps = producerProp;
//        this.consumerProps = consumerProp;
//    }

    public KafkaFactory(Properties kafkaClientProps) {
        this.kafkaClientProps = kafkaClientProps;
//        this.consumerProps = consumerProp;
    }

    public KafkaProducer<K, V> createProducer() {
        return new KafkaProducer<>(kafkaClientProps);
    }

    /**
     * 병렬 처리를 위해 기본 client.id에 Thread ID를 포스트픽스로 붙임
     *
     * @param clientIdPostFix thread id
     * @return cliend.id에 thread id가 포스트픽스로 붙은 KafkaProducer
     */
    public KafkaProducer<K, V> createProducer(String clientIdPostFix) {
        String oldClientId = kafkaClientProps.getProperty("client.id");

        kafkaClientProps.put("client.id", oldClientId + clientIdPostFix);
        final KafkaProducer kafkaProducer = new KafkaProducer<>(kafkaClientProps);
        kafkaClientProps.put("client.id", oldClientId);

        return kafkaProducer;
    }

    public KafkaConsumer<K, V> createConsumer() {
        kafkaClientProps.getProperty("client.id");
        return new KafkaConsumer<>(kafkaClientProps);
    }

    /**
     * 병렬 처리를 위해 기본 client.id에 Thread ID를 포스트픽스로 붙임
     *
     * @param clientIdPostFix thread id
     * @return cliend.id에 thread id가 포스트픽스로 붙은 KafkaConsumer
     */
    public KafkaConsumer<K, V> createConsumer(String clientIdPostFix) {
        String oldClientId = kafkaClientProps.getProperty("client.id");

        kafkaClientProps.put("client.id", oldClientId + clientIdPostFix);
        final KafkaConsumer KafkaConsumer = new KafkaConsumer<>(kafkaClientProps);
        kafkaClientProps.put("client.id", oldClientId);

        return KafkaConsumer;
    }

    public Properties getKafkaConsumerProps() {
        return this.kafkaClientProps;
    }

    public Properties getKafkaProducerProps() {
        return this.kafkaClientProps;
    }
}

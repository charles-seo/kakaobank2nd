package com.kakaobank.pretest.application.etl;

import com.kakaobank.pretest.framework.etl.KafkaTransfer;
import com.kakaobank.pretest.framework.source.KafkaSource;
import com.kakaobank.pretest.framework.util.KafkaFactory;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * Pipe 잡을 수행할 Worker객체
 * 주어진 Props값과 Topic으로 Kafka 간의 데이터를 이동시키는 클래스
 *
 * test framework의 kafkaTransfer를 사용
 *
 */
public class KafkaTransferWorker {
    private static final Logger logger = Logger.getLogger(KafkaTransferWorker.class.getName());

    private final Properties fromKafkaClientProps;
    private final Properties toKafkaClientProps;

    public KafkaTransferWorker(Properties fromKafkaClientProps, Properties toKafkaClientProps) {
        this.fromKafkaClientProps = fromKafkaClientProps;
        this.toKafkaClientProps = toKafkaClientProps;
    }

    public KafkaTransfer work(final String fromTopic, final String toTopic) {
        return this.getKafkaTransfer(fromTopic, toTopic);
    }

    private KafkaTransfer getKafkaTransfer(String fromTopic, String toTopic) {
        final KafkaTransfer kafkaTransfer = new KafkaTransfer(new KafkaSource<>(fromTopic, new KafkaFactory<>(fromKafkaClientProps)),
                new KafkaSource<>(toTopic, new KafkaFactory<>(toKafkaClientProps)));

        return kafkaTransfer;
    }
}

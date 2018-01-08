package com.kakaobank.pretest.application;

import com.kakaobank.pretest.application.record.EventRecord;
import com.kakaobank.pretest.framework.record.PairRecord;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Application 테스트 Help 객체
 */
public class TestHelper {
    private static final Logger logger = Logger.getLogger(TestHelper.class.getName());
    private static final TestHelper instance = new TestHelper();

    public static TestHelper getInstance() {
        return instance;
    }

    private TestHelper() {
    }

    public List<PairRecord<String, String>> createSampleKafkaRecord(int recordCount) {
        List<PairRecord<String, String>> samplePairRecord = new LinkedList<>();

        for (int i = 0; i < recordCount; i++) {
            samplePairRecord.add(new PairRecord<String, String>(
                    new StringBuilder()
                        .append(i).append("\t")
                        .append("eventTimeStamp").append(i)
                        .toString(),
                    new StringBuilder()
                        .append(i).append("\t")
                        .append("eventTimeStamp").append(i).append("\t")
                        .append("serviceCode").append(i).append("\t")
                        .append("eventContext").append(i).append("\t")
                        .toString()));

        }

        return samplePairRecord;
    }

    public Properties readProps(String propFile) {
        Properties prop = new Properties();
        InputStream input = null;

        try {
            input = new FileInputStream(propFile);
            prop.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return prop;
    }

    public boolean allFuterIsDone(List<Future> futureList) {
        boolean result = true;
        for(Future future : futureList) {
            if(future.isDone() == false) {
                result = false;
            }
        }
        return result;
    }

    public synchronized void deleteAllFiles(String path) {
        try {
            Files.walk(Paths.get(path))
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
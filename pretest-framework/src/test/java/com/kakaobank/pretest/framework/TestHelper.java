package com.kakaobank.pretest.framework;

import com.kakaobank.pretest.framework.record.Pair;
import com.kakaobank.pretest.framework.record.PairRecord;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

public class TestHelper {
    private static final Logger logger = Logger.getLogger(TestHelper.class.getName());

    private static final TestHelper instance = new TestHelper();

    public static TestHelper getInstance() {
        return instance;
    }

    private TestHelper() {
    }

    public List<Pair<String, String>> createSampleStringPairRecord(int recordCount) {
        List<Pair<String, String>> samplePairRecord = new LinkedList<>();

        for (int i = 0; i < recordCount; i++) {
            samplePairRecord.add(new PairRecord<>("key" + i, "value" + i));
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
}
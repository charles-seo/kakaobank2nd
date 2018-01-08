package com.kakaobank.pretest.application.record;

import com.kakaobank.pretest.framework.record.Pair;
import com.kakaobank.pretest.framework.record.PairRecord;

/**
 * event record를 처리하기 위한 데이터 객체
 * pair 인터페이스를 상속 받아 kafkaSource와 연결
 *
 */
public class EventRecord implements Pair<String, String> {
    private final long eventID;
    private final String eventTimestamp;
    private final String serviceCode;
    private final String eventContext;

    public long getEventID() {
        return eventID;
    }

    public String getEventTimestamp() {
        return eventTimestamp;
    }

    public String getServiceCode() {
        return serviceCode;
    }

    public String getEventContext() {
        return eventContext;
    }

    public EventRecord(long eventID, String eventTimestamp, String serviceCode, String eventContext) {
        this.eventID = eventID;
        this.eventTimestamp = eventTimestamp;

        if(serviceCode.equals("") || serviceCode == null) {
            serviceCode = "null";
            this.serviceCode = serviceCode;
        } else {
            this.serviceCode = serviceCode;
        }
        if(eventContext.equals("") || eventContext == null) {
            this.eventContext = "null";
        } else {
            this.eventContext = eventContext;
        }

    }

    public EventRecord(Pair<String, String> pairRecord) {
        final String[] valueSplit = pairRecord.getValue().split("\t");

        if(valueSplit.length == 4) {
            this.eventID = Long.valueOf(valueSplit[0]);
            this.eventTimestamp = valueSplit[1];
            this.serviceCode = valueSplit[2];
            this.eventContext = valueSplit[3];
        } else if(valueSplit.length == 3) {
            this.eventID = Long.valueOf(valueSplit[0]);
            this.eventTimestamp = valueSplit[1];
            this.serviceCode = valueSplit[2];
            this.eventContext = null;
        } else {
            this.eventID = Long.valueOf(valueSplit[0]);
            this.eventTimestamp = valueSplit[1];
            this.serviceCode = null;
            this.eventContext = null;
        }
    }

    @Override
    public String getKey() {
        return new StringBuilder()
                .append(eventID).append("\t")
                .append(eventTimestamp).toString();
    }

    @Override
    public String getValue() {
        return new StringBuilder()
                .append(eventID).append("\t")
                .append(eventTimestamp).append("\t")
                .append(serviceCode).append("\t")
                .append(eventContext).toString();

    }

    @Override
    public String mkString() {
        return mkString(",");
    }

    @Override
    public String mkString(String s) {
        return new StringBuilder()
                .append(eventID).append("\t")
                .append(eventTimestamp).append(s)
                .append(eventID).append("\t")
                .append(eventTimestamp).append("\t")
                .append(serviceCode).append("\t")
                .append(eventContext).toString();
    }

    @Override
    public boolean equals(Pair pair) {
        if(pair.getKey().equals(this.getKey()) == false) {
            return false;
        } else if(pair.getValue().equals(this.getValue()) == false)
            return false;
        return true;
    }

    public String toString() {
        return this.mkString();
    }

    public PairRecord<String, String> createPairRecord() {
        return new PairRecord<>(this.getKey(), this.getValue());
    }

}

package com.kakaobank.pretest.framework.etl;

/**
 * ETL관련 인터페이스
 * 주어진 조건 을 만족하기 위해 필수로 구현해야 하는 메소드를 강제 함
 */
public interface Transfer {
    boolean move(int limitTimeSec, int intervalTimeSec);
    boolean distinctMove(int limitTimeSec, int intervalTimeSec);
}

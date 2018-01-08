package com.kakaobank.pretest.framework.source;

import java.util.Iterator;

/**
 * 데이터 소스에 사용할 공동 인터페이스
 * 읽기는 소비 패턴으로 정해진 시간동안 데이터를 소비
 * 쓰기는 주어진 레코드를 저장
 *
 * @param <T> 소스 타입
 */
public interface Source<T> {
    Iterable<T> read(int limitTimeSec);
    boolean write(T record);
    boolean write(Iterator<T> records);
}

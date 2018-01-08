package com.kakaobank.pretest.framework.record;

/**
 * key-value pair 데이터를 사용하기 위한 인터페이스
 * @param <K> Key 타입
 * @param <V> Value 타입
 */
public interface Pair<K, V> {
    K getKey();
    V getValue();
    String mkString();
    String mkString(final String delimiter);
    boolean equals(final Pair obj);
    boolean equals(final Object object);
    String toString();
}

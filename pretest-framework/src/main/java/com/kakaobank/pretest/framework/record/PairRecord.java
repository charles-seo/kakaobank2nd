package com.kakaobank.pretest.framework.record;

/**
 * 단순 key-value 데이터를 저장하는데 사용하기 위한 객체
 * final 속성을 사용하여 Read-Only 레코드로 사용
 *
 * @param <K> Key 타입
 * @param <V> Value 타입
 */
public class PairRecord<K, V> implements Pair<K, V> {
    private final K key;
    private final V value;

    public PairRecord(K key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public K getKey() {
        return this.key;
    }

    @Override
    public V getValue() {
        return this.value;
    }

    /**
     * 기본 구분자는 ','를 사용하여 스트링 문자열 생성
     * @return csv포맷의 문자열
     */
    @Override
    public String mkString() {
        return this.key.toString() + "," + this.value.toString();
    }

    /**
     * 구분자를 전달 받아 스트링 문자열 생성
     * @param delimiter Key Value 구분자
     * @return delimiter로 구분된 문자열
     */
    @Override
    public String mkString(final String delimiter) {
        return this.key.toString() + delimiter + this.value.toString();
    }

    /**
     * 값 비교 동등 메소드
     *
     * @param pairRecord 비교할 PairRecord
     * @return Key Value 값이 모두 같다면 True 다르면 False
     */
    @Override
    public boolean equals(final Pair pairRecord) {
        if(pairRecord == null) {
            return false;
        } else {
            return (this.key.equals(pairRecord.getKey()) && this.value == pairRecord.getValue());
        }
    }

    /**
     * 묵시적 비교 연산을 위한 메소드 오버라이딩
     */

    @Override
    public boolean equals(final Object object) {
        if (object == null) {
            return false;
        }
        final Pair pairRecord = (Pair) object;

        return (this.key.equals(pairRecord.getKey()) && this.value == pairRecord.getValue());
    }
    /**
     * 묵시적 형변환을 위한 메소드 오버라이딩
     *
     * @return
     */

    @Override
    public String toString() {
        return this.mkString();
    }
}
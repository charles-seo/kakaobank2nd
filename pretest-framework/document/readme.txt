[디렉토리]

conf
    : log4j.properties : 로깅용 lof4j properties
conf/test
    : test_kafka_consumer.properties : unit test용 카프카 consumer properties
    : test_kafka_producer.properties : unit test용 카프카 producer properties
document
    : 프로젝트 관련 문서
log
    : log4j properties에 테스트를 위한 로그를 각 패키지별로 하위에 생성하도록 설정
src
    : 소스 코드

[패키지]

사전 과제1을 위한 패키지
com.kakaobank.preTest.framework 이하

etl : etl 기능을 가진 클래스 패키지
    Transfer
        : etl 인터페이스
    KafkaTransfer
        : kafka 클러스터/토픽간의 데이터 이동을 위한 객체
        : move : 단순 카프카 메세지를 이동
        : distinctMove : 중복을 제거하여 이동

record : 데이터 소스에서 사용하는 레코드 패키지
    Pair
        : record 중 k-value pair 형식의 레코드를 위한 인터페이스
        : 제네릭 타입을 통해 재사용성을 높임
    PairRecord
        : K-V 구조의 제네릭 레코드 클래스
    SingleSimpleTransfer

source : 데이터 소스 패키지
    Source
        : 데이터 소스를 위한 인터페이스
    KafkaSource
        : kafka 데이터 소스 객체
        : PairRecord를 주입 받아 사용
        : 제네릭 타입을 통해 재사용성을 높임 (properties 설정 통일 필요)

util : 유틸리티 클래스 패키지
    KafkaFactory
        : 카프카 consumer와 producer 객체 생성을 위해 분리한 카프카 팩토리클래스
[디렉토리]

conf
    : log4j.properties : 로깅용 lof4j properties
    : application.properties : 어플리케이션 실행 설정 파일
conf/test
    : from_kafka_client.properties : 카프카 client 설정파일
    : to_kafka_client.properties : 카프카 client 설정파일
document
    : 프로젝트 관련 문서
log
    : log4j properties에 테스트를 위한 로그를 각 패키지별로 하위에 생성하도록 설정
src
    : 소스 코드

[패키지]

사전 과제2을 위한 패키지
com.kakaobank.pretest.application 이하

etl : etl 기능을 가진 클래스 패키지
    KafkaToMysql
        : Kafka 에서 Mysql 로 데이터를 옮기기 위한 클래스
    KafkaTransferWorker
        : 카프카간에 데이터를 이동하는데 사용하는 클래스

record : 데이터 소스에서 사용하는 레코드 패키지
    EventRecord
        : 주어진 카프카데이터를 처리하기 위한 데이터 클래스
        : 카프카데이터를 mysql에 넣기 위한 DAO 클래스
AppMain
    :   어플리케이션 실행 메소드
    : conf 폴더 및
util : 유틸리티 클래스 패키지
    KafkaFactory
        : 카프카 consumer와 producer 객체 생성을 위해 분리한 카프카 팩토리클래스
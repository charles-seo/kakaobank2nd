[사전과제]

## 과제
1. 주어진 개발 요건을 준수하여 Origin 데이터 소스에서 데이터를 읽어 Destination 데이터 소스로 ETL 하는 Java 프레임워크를 만드시오.
2. 해당 프레임워크를 이용해 Kafka 토픽에서 데이터를 읽어 10분 Time Window 내에서 발생하는 데이터 중복을 제거하고 최종적으로 MySql 테이블에 데이터를 저장하는 Java 프로그램을 만드시오.


## 과제1

1. 데이터 소스의 2가지 기능인 읽기/쓰기 부분을 Source 인터페이스와 제네릭타입을 이용해 재사용성과 확장성을 확보 Kafka에 맞게 설계된 Kafka 데이터 소스 클래스 설계

2. 데이터 소스에 사용하는 레코드를 가장 범용적으로 사용할 수 있는 K-V Pair구조로 Pair 인터페이스를 통해 추상화 및 PairRecord 클래스 설계

3. ETL에서 주어진 이동과 중복제거 2가지의 기능을 위해 Transfer 인터페이스로 추상화 및 카프카에 맞는 클래스 설계

4. Unit Test

KafkaTransferTest : 카프카 데이터 이동 테스트
	testFromToSourceWriteAndRead 				: Kafka 데이터 소스의 읽기/쓰기 테스트
	testDistinctMoveAndParallelDistinctMoveTest Kafka 	: 데이터 소소간의 중복제거 이동 테스트 (병렬 포함)
	testMoveAndParalleMoveTest Kafka			: 데이터 소스간의 데이터 이동 테스트 (병렬 포함)

PairRecordTest : Pair 키밸류 테스트
	testPairRecordEqual 			: PairRecord 클래스 동등성 테스트
	testPairRecordMakeString		: PairRecord 클래스 스트링 테스트
	testPairRecordGetKeyAndGetValue		: PairRecord 클래스 K-V 테스트

KafkasourceTest : 카프카 데이터 소스 테스트
	tetsAsyncWriteParallelReadByManual	: 병렬 읽기를 하드 코딩으로 테스트
	testAsyncWriteParallelReadByMethod	: 소스 병렬 읽기를 개발한 메소드를 이용해 테스트
	testGetTopic				: 토픽 확인 테스트
	testParallelWriteAndSingleRead		: 병렬 쓰기 - 싱글 읽기 테스트
	testParallelAsyncWriteAndSingleRead	: 병렬 비동기 쓰기 - 싱글 읽기 테스트
	testSingleWriteAndSingleRead		: 싱글 동기 쓰기 - 싱글 읽기 테스트
	testParallelWriteAndRead		: 병렬로 동시에 읽기 쓰기 테스트
	testAsyncWriteAndSingleRead		: 비동기 쓰기 - 싱글 읽기 테스트


## 과제 2

1. kafka의 토픽을 시간단위로 파이프라인을 구성하여 각 단계별로 중복을 제거한 레코드를 단위 시간만큼 배치처리

2. 주어진 데이터를 저장하기 위한 Mysql 접근 클래스 설계 (시간 부족으로 확장성 및 재사용성 부족)

3. application props : arg로 어플리케이션 properties위치를 받아 실행 환경 설정
fromKafkaPropsFile=conf/from_kafka_client.properties	: Origin 카프카 client 설정
toKafkaPropsFile=conf/to_kafka_client.properties	: Destination 카프카 client 설정
fromTopic=application-from-topic			: Origin 카프카 토픽
toTopic=application-to-topic				: Destination 카프카 토픽
applicationLimitTime=-1					: 어플리케이션 수행시간 (-1 무한)
taskSubTimeInterval=3					: 각 작업의 서브 테스크들의 반복 주기
pipeTimeSecStep=60,180,300,600				: 파이프 라인 단계 (단위, 초) 1분/3분/5분/10분으로 데이터 처리

4. mysql props
	: 최종 destination mysql client 설정

5. Unit Test

ApplicationMainTest
	시나리오테스트1			: testCase1 conf/test/testcase1-application.properties 로 설정된 시나리오 테스트 1
	시나리오테스트2			: testCase2 conf/test/testcase2-application.properties 로 설정된 시나리오 테스트 2

KafkaToMysqlTest			: from 카프카 to Mysql 테스트
	testKafkaToMysqlMove		: 카프카 데이터 소스에서 Mysql로 저장 테스트

EventRecordAndEventDaoTest		: MYsql 테스트
	testAddList			: 주어진 데이터를 저장할 Mysql 테스트
	testAddAndGetAndCount		: 주어진 데이터를 저장할 Mysql 테스트

maven unit test 확인.

*
원활한 테스트를 위해 Kafka 테스트 서버를 올려 놓았습니다
mysql은 외부 라이브러리를 이용해 테스트중 프로레스를 띄우고 종료 되면 관련 데이터를 삭제합니다.

**
Mysql과 관련된 부분과 TIme Window를 컨트롤 하는 부분이 기능은 작동하나 정교함이 조금 부족합니다.

***
테스트 실행시 log 폴더 밑으로 패키지 별 debug 레벨로 로그를 쌓이도록 log4j prop를 설정해 두었습니다.


인내심을 가지고 기다려주셔서 감사합니다.

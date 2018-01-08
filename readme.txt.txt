[��������]

## ����
1. �־��� ���� ����� �ؼ��Ͽ� Origin ������ �ҽ����� �����͸� �о� Destination ������ �ҽ��� ETL �ϴ� Java �����ӿ�ũ�� ����ÿ�.
2. �ش� �����ӿ�ũ�� �̿��� Kafka ���ȿ��� �����͸� �о� 10�� Time Window ������ �߻��ϴ� ������ �ߺ��� �����ϰ� ���������� MySql ���̺� �����͸� �����ϴ� Java ���α׷��� ����ÿ�.


## ����1

1. ������ �ҽ��� 2���� ����� �б�/���� �κ��� Source �������̽��� ���׸�Ÿ���� �̿��� ���뼺�� Ȯ�强�� Ȯ�� Kafka�� �°� ����� Kafka ������ �ҽ� Ŭ���� ����

2. ������ �ҽ��� ����ϴ� ���ڵ带 ���� ���������� ����� �� �ִ� K-V Pair������ Pair �������̽��� ���� �߻�ȭ �� PairRecord Ŭ���� ����

3. ETL���� �־��� �̵��� �ߺ����� 2������ ����� ���� Transfer �������̽��� �߻�ȭ �� ī��ī�� �´� Ŭ���� ����

4. Unit Test

KafkaTransferTest : ī��ī ������ �̵� �׽�Ʈ
	testFromToSourceWriteAndRead 				: Kafka ������ �ҽ��� �б�/���� �׽�Ʈ
	testDistinctMoveAndParallelDistinctMoveTest Kafka 	: ������ �ҼҰ��� �ߺ����� �̵� �׽�Ʈ (���� ����)
	testMoveAndParalleMoveTest Kafka			: ������ �ҽ����� ������ �̵� �׽�Ʈ (���� ����)

PairRecordTest : Pair Ű��� �׽�Ʈ
	testPairRecordEqual 			: PairRecord Ŭ���� ��� �׽�Ʈ
	testPairRecordMakeString		: PairRecord Ŭ���� ��Ʈ�� �׽�Ʈ
	testPairRecordGetKeyAndGetValue		: PairRecord Ŭ���� K-V �׽�Ʈ

KafkasourceTest : ī��ī ������ �ҽ� �׽�Ʈ
	tetsAsyncWriteParallelReadByManual	: ���� �б⸦ �ϵ� �ڵ����� �׽�Ʈ
	testAsyncWriteParallelReadByMethod	: �ҽ� ���� �б⸦ ������ �޼ҵ带 �̿��� �׽�Ʈ
	testGetTopic				: ���� Ȯ�� �׽�Ʈ
	testParallelWriteAndSingleRead		: ���� ���� - �̱� �б� �׽�Ʈ
	testParallelAsyncWriteAndSingleRead	: ���� �񵿱� ���� - �̱� �б� �׽�Ʈ
	testSingleWriteAndSingleRead		: �̱� ���� ���� - �̱� �б� �׽�Ʈ
	testParallelWriteAndRead		: ���ķ� ���ÿ� �б� ���� �׽�Ʈ
	testAsyncWriteAndSingleRead		: �񵿱� ���� - �̱� �б� �׽�Ʈ


## ���� 2

1. kafka�� ������ �ð������� ������������ �����Ͽ� �� �ܰ躰�� �ߺ��� ������ ���ڵ带 ���� �ð���ŭ ��ġó��

2. �־��� �����͸� �����ϱ� ���� Mysql ���� Ŭ���� ���� (�ð� �������� Ȯ�强 �� ���뼺 ����)

3. application props : arg�� ���ø����̼� properties��ġ�� �޾� ���� ȯ�� ����
fromKafkaPropsFile=conf/from_kafka_client.properties	: Origin ī��ī client ����
toKafkaPropsFile=conf/to_kafka_client.properties	: Destination ī��ī client ����
fromTopic=application-from-topic			: Origin ī��ī ����
toTopic=application-to-topic				: Destination ī��ī ����
applicationLimitTime=-1					: ���ø����̼� ����ð� (-1 ����)
taskSubTimeInterval=3					: �� �۾��� ���� �׽�ũ���� �ݺ� �ֱ�
pipeTimeSecStep=60,180,300,600				: ������ ���� �ܰ� (����, ��) 1��/3��/5��/10������ ������ ó��

4. mysql props
	: ���� destination mysql client ����

5. Unit Test

ApplicationMainTest
	�ó������׽�Ʈ1			: testCase1 conf/test/testcase1-application.properties �� ������ �ó����� �׽�Ʈ 1
	�ó������׽�Ʈ2			: testCase2 conf/test/testcase2-application.properties �� ������ �ó����� �׽�Ʈ 2

KafkaToMysqlTest			: from ī��ī to Mysql �׽�Ʈ
	testKafkaToMysqlMove		: ī��ī ������ �ҽ����� Mysql�� ���� �׽�Ʈ

EventRecordAndEventDaoTest		: MYsql �׽�Ʈ
	testAddList			: �־��� �����͸� ������ Mysql �׽�Ʈ
	testAddAndGetAndCount		: �־��� �����͸� ������ Mysql �׽�Ʈ

maven unit test Ȯ��.

*
��Ȱ�� �׽�Ʈ�� ���� Kafka �׽�Ʈ ������ �÷� ���ҽ��ϴ�
mysql�� �ܺ� ���̺귯���� �̿��� �׽�Ʈ�� ���η����� ���� ���� �Ǹ� ���� �����͸� �����մϴ�.

**
Mysql�� ���õ� �κа� TIme Window�� ��Ʈ�� �ϴ� �κ��� ����� �۵��ϳ� �������� ���� �����մϴ�.

***
�׽�Ʈ ����� log ���� ������ ��Ű�� �� debug ������ �α׸� ���̵��� log4j prop�� ������ �ξ����ϴ�.


�γ����� ������ ��ٷ��ּż� �����մϴ�.

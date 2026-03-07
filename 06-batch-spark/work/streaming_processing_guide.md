# 스트리밍 프로세싱 완전 가이드

> 이 문서는 [batch_processing_guide.md](batch_processing_guide.md)의 별도 보충 문서입니다.

## 목차

1. [스트리밍 프로세싱이란?](#1-스트리밍-프로세싱이란)
2. [스트리밍에서 하는 일](#2-스트리밍에서-하는-일)
3. [CDC (Change Data Capture)](#3-cdc-change-data-capture)
4. [스트리밍 주요 도구](#4-스트리밍-주요-도구)
5. [스트리밍 핵심 개념](#5-스트리밍-핵심-개념)
6. [배치와 스트리밍의 경계 — Micro-batch](#6-배치와-스트리밍의-경계--micro-batch)
7. [실전 유즈케이스](#7-실전-유즈케이스)

---

## 1. 스트리밍 프로세싱이란?

데이터가 생성되는 **즉시**(실시간 또는 준실시간) 처리하는 방식입니다.

```
배치: 데이터가 쌓인다 → 모아서 한번에 처리 → 결과
       ═══════════════════════════════════════
       |    수집 (대기)    |   처리   | 결과 |

스트리밍: 데이터 도착 → 즉시 처리 → 즉시 결과
          ══╦══╦══╦══╦══╦══╦══╦══
            │  │  │  │  │  │  │
            ▼  ▼  ▼  ▼  ▼  ▼  ▼
          결과 결과 결과 ...
```

### 핵심 특성

| 특성 | 설명 |
|------|------|
| **무한 데이터** | 끝이 없는 데이터 스트림 (unbounded) |
| **이벤트 기반** | 데이터 도착이 처리 트리거 |
| **상태 관리** | 중간 상태를 유지해야 함 (집계, 윈도우 등) |
| **순서 보장** | 늦게 도착하는 데이터(late arrival) 처리 필요 |
| **Exactly-once** | 정확히 한 번만 처리하는 보장이 어려움 |

---

## 2. 스트리밍에서 하는 일

### 2.1 실시간 이벤트 처리

```
사용자 행동 이벤트 → 즉시 처리 → 즉시 반응

예:
  사용자 클릭 → 실시간 추천 업데이트
  결제 요청 → 실시간 사기 탐지
  센서 데이터 → 실시간 이상 알림
```

### 2.2 실시간 집계 (Windowed Aggregation)

```python
# 5분 윈도우 단위 집계
stream_df \
    .groupBy(
        F.window("event_time", "5 minutes"),
        "zone_id"
    ) \
    .agg(F.count("*").alias("trip_count"))
```

```
|------ Window 1 (00:00~00:05) ------|
|  event1  event2  event3            |  → count: 3
|------ Window 2 (00:05~00:10) ------|
|  event4  event5                    |  → count: 2
```

### 2.3 실시간 ETL / Data Pipeline

```
Source DB (MySQL)
    │
    ▼ CDC (Debezium)
    │
    ▼ Kafka
    │
    ▼ Flink / Spark Streaming
    │  - 정제, 변환, 조인
    ▼
Target (BigQuery / Elasticsearch / Redis)
```

### 2.4 이벤트 소싱 (Event Sourcing)

```
모든 상태 변경을 이벤트로 저장

Event 1: {type: "OrderCreated", order_id: 1, amount: 100}
Event 2: {type: "PaymentReceived", order_id: 1, amount: 100}
Event 3: {type: "OrderShipped", order_id: 1}

→ 현재 상태를 이벤트 리플레이로 재구성 가능
```

---

## 3. CDC (Change Data Capture)

### CDC란?

데이터베이스의 **변경사항**(INSERT, UPDATE, DELETE)을 실시간으로 캡처하여 다른 시스템으로 전파하는 기술입니다.

```
┌──────────┐     ┌───────────┐     ┌─────────┐     ┌──────────┐
│  Source   │────▶│ Debezium  │────▶│  Kafka  │────▶│  Target  │
│  MySQL    │     │ (CDC)     │     │         │     │ BigQuery │
│           │     │           │     │         │     │ Elastic  │
└──────────┘     └───────────┘     └─────────┘     └──────────┘
    │                  │
    │ binlog 읽기       │ 변경 이벤트 발행
    │ (트랜잭션 로그)     │
```

### CDC 방식 비교

| 방식 | 설명 | 장점 | 단점 |
|------|------|------|------|
| **Log-based (Binlog)** | DB 트랜잭션 로그 읽기 | 소스 DB 부하 최소, 모든 변경 캡처 | DB별 설정 필요 |
| **Trigger-based** | DB 트리거로 변경 감지 | 구현 단순 | 소스 DB 부하 큼 |
| **Timestamp-based** | `updated_at` 컬럼 비교 | 가장 단순 | DELETE 감지 불가, 정확도 낮음 |
| **Query-based polling** | 주기적 쿼리로 변경 감지 | 범용적 | 실시간 아님, 부하 |

### CDC 상세 흐름 (Log-based)

```
MySQL                  Debezium               Kafka                  Consumer
──────                 ────────               ─────                  ────────
INSERT user(1, "Kim")
  │
  ▼
binlog 기록 ──────────▶ binlog 읽기
                          │
                          ▼
                       이벤트 생성 ──────────▶ topic: db.users
                       {                       {
                         "op": "c",              key: {id: 1}
                         "after": {              value: {
                           id: 1,                  op: "c",
                           name: "Kim"             after: {id:1, name:"Kim"}
                         }                       }
                       }                       }
                                                    │
                                                    ▼
                                               Consumer가 읽어서
                                               BigQuery INSERT
UPDATE user SET name="Park" WHERE id=1
  │
  ▼
binlog ──────────────▶ {                  ──▶ {
                         "op": "u",             op: "u",
                         "before": {            before: {id:1, name:"Kim"}
                           id:1,name:"Kim"      after:  {id:1, name:"Park"}
                         },                   }
                         "after": {
                           id:1,name:"Park"
                         }
                       }

DELETE FROM user WHERE id=1
  │
  ▼
binlog ──────────────▶ {                  ──▶ {
                         "op": "d",             op: "d",
                         "before": {            before: {id:1, name:"Park"}
                           id:1,name:"Park"   }
                         }
                       }
```

### CDC 유즈케이스

| 유즈케이스 | 설명 |
|-----------|------|
| **DB 복제** | MySQL → PostgreSQL 실시간 복제 |
| **DB → Data Warehouse** | OLTP → BigQuery/Snowflake 실시간 동기화 |
| **캐시 무효화** | DB 변경 → Redis 캐시 자동 갱신 |
| **검색 인덱스 동기화** | DB 변경 → Elasticsearch 인덱스 업데이트 |
| **마이크로서비스 이벤트** | 서비스 간 데이터 변경 전파 |
| **감사 로그** | 모든 데이터 변경 이력 보관 |

### CDC 도구

| 도구 | 특징 |
|------|------|
| **Debezium** | 오픈소스, Kafka Connect 기반, 가장 널리 사용 |
| **AWS DMS** | AWS 매니지드, DB 마이그레이션/복제 |
| **Fivetran** | SaaS, 자동 CDC |
| **Airbyte** | 오픈소스, CDC 모드 지원 |
| **Maxwell** | MySQL 전용, 경량 |

---

## 4. 스트리밍 주요 도구

### 도구 비교

| 도구 | 모델 | 언어 | 지연 | 특징 |
|------|------|------|------|------|
| **Apache Kafka** | 메시지 브로커 | 모든 언어 | ms | 이벤트 저장 + 전달, 핵심 인프라 |
| **Apache Flink** | 진정한 스트리밍 | Java/Python | ms | 상태 관리 최강, 정확한 이벤트 시간 처리 |
| **Spark Structured Streaming** | Micro-batch | Python/Scala | 초~분 | 배치 API와 통합, 학습 곡선 낮음 |
| **Apache Kafka Streams** | 라이브러리 | Java | ms | 별도 클러스터 불필요 |
| **Amazon Kinesis** | 매니지드 | 모든 언어 | 초 | AWS 네이티브 |
| **Google Pub/Sub + Dataflow** | 매니지드 | Java/Python | 초 | GCP 네이티브, Apache Beam 기반 |
| **Redpanda** | Kafka 호환 브로커 | 모든 언어 | ms | C++로 작성, ZooKeeper 불필요 |

### Kafka 아키텍처 간단 설명

```
Producer ──▶ Kafka Cluster ──▶ Consumer
             ┌────────────┐
             │  Topic:     │
             │  "orders"   │
             │             │
             │ Partition 0 │ ──▶ Consumer Group A
             │ [msg1|msg2] │     (주문 서비스)
             │             │
             │ Partition 1 │ ──▶ Consumer Group B
             │ [msg3|msg4] │     (분석 서비스)
             └────────────┘

핵심 개념:
- Topic: 메시지 카테고리 (예: orders, payments)
- Partition: 병렬 처리 단위
- Offset: 각 메시지의 순번
- Consumer Group: 같은 토픽을 읽는 소비자 그룹
- Retention: 메시지 보관 기간 (기본 7일)
```

---

## 5. 스트리밍 핵심 개념

### 5.1 이벤트 시간 vs 처리 시간

```
이벤트 시간 (Event Time): 데이터가 실제 발생한 시각
처리 시간 (Processing Time): 시스템이 데이터를 받은 시각

예: 센서가 14:00:00에 데이터 생성 → 네트워크 지연 → 14:00:05에 도착

  Event Time:      14:00:00
  Processing Time: 14:00:05
  Skew:            5초
```

**왜 중요?** 대부분의 비즈니스 로직은 이벤트 시간 기준이어야 정확.

### 5.2 워터마크 (Watermark)

> "이 시점 이전의 데이터는 더 이상 오지 않을 것이다"

```python
# Spark: 10분 워터마크
stream_df \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        F.window("event_time", "5 minutes"),
        "zone_id"
    ).count()
```

```
현재 처리 중인 이벤트 시간: 14:20
워터마크: 14:20 - 10분 = 14:10

→ 14:10 이전 윈도우(14:00~14:05, 14:05~14:10)는 확정
→ 14:10 이후 이벤트는 아직 도착 가능 (윈도우 열려있음)
→ 14:09에 발생한 이벤트가 늦게 도착 → 무시됨 (너무 늦음)
```

### 5.3 Delivery Guarantees

| 보장 수준 | 설명 | 복잡도 |
|-----------|------|--------|
| **At-most-once** | 최대 1번 (유실 가능) | 낮음 |
| **At-least-once** | 최소 1번 (중복 가능) | 중간 |
| **Exactly-once** | 정확히 1번 | 높음 |

```
At-most-once:  처리 → ACK → 실패해도 안 재시도 → 유실 가능
At-least-once: ACK전 실패 → 재시도 → 중복 가능
Exactly-once:  트랜잭션 + 멱등성 → 정확히 1번
```

### 5.4 상태 관리 (State Management)

```
Stateless 처리: 각 이벤트를 독립적으로 처리
  예: 필터링, 매핑, 포맷 변환

Stateful 처리: 이전 이벤트 정보를 기억해야 함
  예: 윈도우 집계, 조인, 패턴 매칭

Flink:  RocksDB 기반 로컬 상태 + 체크포인트
Spark:  HDFS/S3에 체크포인트 저장
Kafka Streams: 내장 RocksDB 상태 저장소
```

---

## 6. 배치와 스트리밍의 경계 — Micro-batch

### Micro-batch란?

```
순수 스트리밍: 이벤트 1개 도착 → 즉시 1개 처리
Micro-batch:  이벤트 N개 모음 (수초) → 한번에 처리

┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐
│Batch│ │Batch│ │Batch│ │Batch│  ...
│ 0   │ │ 1   │ │ 2   │ │ 3   │
│2sec │ │2sec │ │2sec │ │2sec │
└─────┘ └─────┘ └─────┘ └─────┘
```

**Spark Structured Streaming이 micro-batch 방식**

```python
# trigger: 2초마다 micro-batch
query = (
    stream_df
    .writeStream
    .trigger(processingTime="2 seconds")
    .format("parquet")
    .option("path", "output/")
    .option("checkpointLocation", "checkpoint/")
    .start()
)
```

### 비교

| 항목 | 순수 스트리밍 (Flink) | Micro-batch (Spark) |
|------|---------------------|---------------------|
| 지연 | 밀리초 | 초~십 초 |
| 처리량 | 높음 | 매우 높음 |
| 상태 관리 | 네이티브 | 체크포인트 기반 |
| 구현 복잡도 | 높음 | 낮음 (배치 API 재사용) |
| 적합 | 초저지연 필수 | 수 초 지연 허용 |

---

## 7. 실전 유즈케이스

### Case 1: 실시간 사기 탐지

```
결제 이벤트 → Kafka → Flink
                        │
                        ├─ Rule 1: 1분 내 3회 이상 결제 시도 → 알림
                        ├─ Rule 2: 평소 지역과 다른 곳에서 결제 → 알림
                        └─ ML 모델 스코어링 → 0.8 이상 → 차단
                        │
                        ▼
                   Redis (실시간 상태) + Slack Alert
```

### Case 2: CDC → Data Warehouse 동기화

```
MySQL (OLTP)
  │ Debezium
  ▼
Kafka (topics: db.users, db.orders, db.products)
  │ Flink / Kafka Connect
  ▼
BigQuery (분석용)
  │ dbt
  ▼
Dashboard (Looker/Metabase)

장점: 배치 ETL과 달리 거의 실시간 반영 (수 초~수 분)
```

### Case 3: 실시간 추천 시스템

```
사용자 행동 (클릭, 조회, 구매)
  │
  ▼ Kafka
  │
  ▼ Flink
  │  ├─ 실시간 피처 계산 (최근 30분 조회 카테고리)
  │  └─ Redis에 피처 저장
  │
  ▼ 추천 API 서버
     └─ Redis에서 피처 읽기 → ML 모델 추론 → 추천 결과 반환
```

### Case 4: IoT 센서 모니터링

```
센서 1만대 → Kafka → Spark Streaming
                        │
                        ├─ 5분 이동 평균 계산
                        ├─ 임계치 초과 감지
                        └─ Elasticsearch 저장
                        │
                        ▼
                   Grafana 대시보드 + PagerDuty 알림
```

### Case 5: 로그 실시간 분석

```
App Server 1 ──┐
App Server 2 ──┼──▶ Kafka ──▶ Flink ──▶ Elasticsearch
App Server 3 ──┘                            │
                    ├─ 에러율 집계              ▼
                    ├─ 응답 시간 P99 계산    Kibana 대시보드
                    └─ 5xx 스파이크 감지 → Slack 알림
```

### Case 6: 이벤트 소싱 + CQRS

```
Command (쓰기) ──▶ Event Store (Kafka)
                        │
                        ├──▶ Read Model 1 (검색용 - Elasticsearch)
                        ├──▶ Read Model 2 (분석용 - BigQuery)
                        └──▶ Read Model 3 (캐시용 - Redis)

CQRS: Command Query Responsibility Segregation
  - 쓰기와 읽기를 분리
  - 각 읽기 모델을 용도에 맞게 최적화
```

---

## 요약: 배치 vs 스트리밍 선택 기준

```
데이터 신선도 요구?
│
├─ "즉시 반응해야 한다" (ms~초)
│   └─ 스트리밍 (Kafka + Flink)
│      예: 사기 탐지, IoT 알림, 실시간 추천
│
├─ "수 분 이내면 된다"
│   └─ Micro-batch (Spark Streaming)
│      예: 준실시간 대시보드, CDC 동기화
│
├─ "시간~일 단위면 된다"
│   └─ 배치 (dbt + Airflow)
│      예: 일일 리포트, ML 학습, 정산
│
└─ "둘 다 필요하다"
    └─ Lambda 아키텍처 or 통합 프레임워크 (Apache Beam)
```

# 배치 프로세싱 완전 가이드

## 목차

1. [배치 vs 스트리밍 — 왜 나누는가?](#1-배치-vs-스트리밍--왜-나누는가)
2. [배치 프로세싱에 쓰이는 도구](#2-배치-프로세싱에-쓰이는-도구)
3. [배치 프로세싱 핵심 방법론](#3-배치-프로세싱-핵심-방법론)
4. [파이프라인 관리 — 에러, 로깅, 모니터링](#4-파이프라인-관리--에러-로깅-모니터링)
5. [메달리온 아키텍처 — 데이터 단계별 활용](#5-메달리온-아키텍처--데이터-단계별-활용)
6. [배치 스케줄링과 유즈케이스](#6-배치-스케줄링과-유즈케이스)

> 스트리밍 프로세싱 상세 내용은 [streaming_processing_guide.md](streaming_processing_guide.md) 참조

---

## 1. 배치 vs 스트리밍 — 왜 나누는가?

### 근본적 차이

| 항목 | 배치(Batch) | 스트리밍(Streaming) |
|------|-------------|---------------------|
| **데이터 범위** | 유한한 데이터셋 (bounded) | 무한한 데이터 흐름 (unbounded) |
| **처리 시점** | 일정 주기로 모아서 처리 | 데이터 도착 즉시 처리 |
| **지연 시간** | 분~시간~일 | 밀리초~초~분 |
| **복잡도** | 상대적으로 단순 | 상태 관리, 순서 보장 등 복잡 |
| **비용** | 리소스를 주기적으로만 사용 | 항상 실행 → 비용 높음 |
| **재처리** | 쉬움 (같은 데이터 다시 처리) | 어려움 (오프셋 관리 필요) |

### 왜 나누는가?

```
"모든 것을 실시간으로 처리하면 되지 않나?"
→ 아니다. 대부분의 비즈니스 요구는 배치로 충분하고, 비용도 훨씬 저렴하다.
```

**배치가 적합한 경우:**
- 일 단위 매출 리포트, 주간 KPI 대시보드
- ML 모델 학습용 피처 생성
- 대용량 데이터 마이그레이션/변환
- 규정 준수를 위한 일 단위 감사 로그 집계

**스트리밍이 필요한 경우:**
- 실시간 이상 탐지 (결제 사기 감지)
- IoT 센서 데이터 실시간 모니터링
- 실시간 추천 시스템
- CDC(Change Data Capture) 기반 데이터 동기화

### 현실에서의 조합: Lambda / Kappa 아키텍처

```
┌────────────────────────────────────────────┐
│           Lambda Architecture               │
│                                            │
│  Source ──┬── Batch Layer ──┐              │
│           │   (일 단위)      ├── Serving   │
│           └── Speed Layer ──┘   Layer      │
│               (실시간)                      │
└────────────────────────────────────────────┘

┌────────────────────────────────────────────┐
│           Kappa Architecture               │
│                                            │
│  Source ── Stream Layer ── Serving Layer   │
│            (모든 것을 스트림으로)             │
└────────────────────────────────────────────┘
```

- **Lambda**: 배치 + 스트리밍 병행. 정확성(배치) + 속도(스트리밍) 확보. 단점: 로직 이중 관리
- **Kappa**: 스트리밍만으로 통합. 단순하지만 재처리 비용 높음

---

## 2. 배치 프로세싱에 쓰이는 도구

### 도구 분류 맵

```
┌─────────────────────────────────────────────────────┐
│                  배치 프로세싱 도구                     │
│                                                     │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────┐ │
│  │ 변환(Transform)│  │ 이동(Extract/ │  │ 분산 처리   │ │
│  │             │  │  Load)       │  │ 엔진       │ │
│  │ • dbt       │  │ • dlt        │  │ • Spark    │ │
│  │ • SQLMesh   │  │ • Airbyte    │  │ • Flink    │ │
│  │             │  │ • Fivetran   │  │ • Trino    │ │
│  │             │  │ • Meltano    │  │            │ │
│  └─────────────┘  └──────────────┘  └────────────┘ │
│                                                     │
│  ┌─────────────────────────────────────────────┐    │
│  │ 오케스트레이션 (Orchestration)                  │    │
│  │ • Airflow • Dagster • Prefect • Kestra      │    │
│  └─────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────┘
```

### 2.1 dbt — SQL로 배치 변환

> **"SQL만으로 배치를 돌린다"** → 맞음

```
역할: 웨어하우스 내부에서 SELECT로 테이블 → 테이블 변환
     (Transform in ELT의 T)
```

**특징:**
- SQL SELECT 문 = 하나의 모델(model)
- 모델 간 의존성 자동 관리 (DAG)
- 테스트, 문서화 내장
- BigQuery, Snowflake, Redshift, Postgres 등 지원

```sql
-- models/staging/stg_trips.sql
SELECT
    pickup_datetime,
    dropoff_datetime,
    PULocationID,
    DOLocationID,
    TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE) AS trip_duration_min
FROM {{ source('raw', 'fhvhv_tripdata') }}
WHERE pickup_datetime IS NOT NULL
```

```sql
-- models/marts/daily_trip_summary.sql
SELECT
    DATE(pickup_datetime) AS trip_date,
    COUNT(*) AS total_trips,
    AVG(trip_duration_min) AS avg_duration_min
FROM {{ ref('stg_trips') }}
GROUP BY 1
```

**dbt가 하는 일:**
1. 모델 간 의존성 파악 (DAG 빌드)
2. 올바른 순서로 SQL 실행
3. 결과를 테이블/뷰로 생성
4. 테스트 실행 (null 체크, unique 체크 등)

### 2.2 dlt (data load tool) — 데이터 이동 + 로드

> **"서로 다른 소스에서 데이터를 이동·저장한다"** → 맞음 (EL 부분 담당)

```
역할: API, DB, 파일 등 다양한 소스 → 웨어하우스/레이크로 로드
     (Extract + Load in ELT)
```

**특징:**
- Python 코드로 파이프라인 정의
- 스키마 자동 추론 & 진화(schema evolution)
- Incremental 로딩 내장
- 정규화(normalization) 자동 처리

```python
import dlt

@dlt.source
def github_source(api_token):
    @dlt.resource(write_disposition="merge", primary_key="id")
    def issues():
        response = requests.get(
            "https://api.github.com/repos/org/repo/issues",
            headers={"Authorization": f"token {api_token}"}
        )
        yield response.json()
    return issues

# 파이프라인 실행: GitHub API → BigQuery
pipeline = dlt.pipeline(
    pipeline_name="github_to_bq",
    destination="bigquery",
    dataset_name="github_data"
)
pipeline.run(github_source(api_token="..."))
```

### 2.3 dbt vs dlt 비교

| 항목 | dbt | dlt |
|------|-----|-----|
| **역할** | **T**(Transform) | **E**xtract + **L**oad |
| **언어** | SQL (+ Jinja) | Python |
| **데이터 위치** | 웨어하우스 내부에서만 | 외부 소스 → 웨어하우스로 이동 |
| **스키마 관리** | 수동 정의 | 자동 추론 + 진화 |
| **주 용도** | 데이터 정제/집계/모델링 | 데이터 수집/적재 |
| **조합** | dlt로 로드 → dbt로 변환 | dbt 전 단계에서 사용 |

```
전체 흐름:

  API / DB / Files
       │
       ▼
  ┌─────────┐
  │   dlt   │  Extract + Load
  └────┬────┘
       ▼
  ┌─────────┐
  │   dbt   │  Transform (SQL)
  └────┬────┘
       ▼
  Dashboard / ML / Report
```

### 2.4 기타 주요 도구

| 도구 | 역할 | 특징 |
|------|------|------|
| **Apache Spark** | 대규모 분산 처리 엔진 | Python/Scala/SQL, 메모리 기반, 수 TB~PB |
| **Airbyte** | EL (커넥터 기반) | 300+ 커넥터, 오픈소스, UI 제공 |
| **Fivetran** | EL (매니지드) | SaaS, 자동 스키마 관리, 비쌈 |
| **Trino/Presto** | 분산 SQL 쿼리 엔진 | 여러 소스에 걸친 SQL 질의 |
| **Apache Airflow** | 오케스트레이션 | DAG 기반 워크플로우 스케줄링 |
| **Dagster** | 오케스트레이션 | Asset 기반, 타입 시스템 |
| **Kestra** | 오케스트레이션 | YAML 기반, UI 친화적 |

---

## 3. 배치 프로세싱 핵심 방법론

### 3.1 Full Load (전체 적재)

```
매 실행마다 전체 데이터를 덮어쓰기

Run 1: 전체 테이블 → DROP + INSERT
Run 2: 전체 테이블 → DROP + INSERT
```

**장점:** 단순, 항상 최신 상태 보장
**단점:** 대용량에서 비용·시간 막대
**적합:** 소규모 참조 테이블 (국가 코드, 카테고리 등)

### 3.2 Incremental Load (증분 적재) ⭐

```
변경된 데이터만 추가/업데이트

Run 1: 2024-01-01 ~ 2024-01-01 데이터만 처리
Run 2: 2024-01-02 ~ 2024-01-02 데이터만 처리
```

**구현 방법:**

| 방식 | 설명 | 예시 |
|------|------|------|
| **Timestamp 기반** | `updated_at > last_run_time` | 가장 일반적 |
| **ID 기반** | `id > last_max_id` | 순차 증가 ID |
| **파티션 기반** | 날짜 파티션만 교체 | BigQuery 파티션 테이블 |
| **CDC 기반** | 변경 로그(binlog) 추적 | Debezium + Kafka |

**dbt에서 incremental:**
```sql
-- models/incremental_trips.sql
{{ config(materialized='incremental', unique_key='trip_id') }}

SELECT *
FROM {{ source('raw', 'trips') }}

{% if is_incremental() %}
  WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

**dlt에서 incremental:**
```python
@dlt.resource(write_disposition="merge", primary_key="id")
def orders(last_updated=dlt.sources.incremental("updated_at")):
    # last_updated.last_value 이후 데이터만 로드
    yield api.get_orders(since=last_updated.last_value)
```

### 3.3 Snapshot (스냅샷)

```
특정 시점의 전체 상태를 보존

2024-01-01 스냅샷: {user_1: active, user_2: active}
2024-01-02 스냅샷: {user_1: active, user_2: inactive}  ← 변경 추적
```

**dbt snapshot (SCD Type 2):**
```sql
{% snapshot user_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='user_id',
    strategy='timestamp',
    updated_at='updated_at'
) }}
SELECT * FROM {{ source('raw', 'users') }}
{% endsnapshot %}
```

| 컬럼 | 설명 |
|------|------|
| `dbt_valid_from` | 이 레코드가 유효해진 시점 |
| `dbt_valid_to` | 이 레코드가 무효해진 시점 (NULL = 현재 유효) |

### 3.4 Backfill (과거 데이터 재처리)

```
과거 특정 기간의 데이터를 다시 처리

"2024-01 데이터에 버그가 있었다. 2024-01만 다시 돌려라"
```

```python
# Airflow에서 backfill
airflow dags backfill \
    --start-date 2024-01-01 \
    --end-date 2024-01-31 \
    my_daily_pipeline
```

### 3.5 Idempotency (멱등성)

> **같은 입력으로 몇 번을 실행해도 같은 결과**

```
❌ 비멱등: INSERT INTO target SELECT ... → 중복 행 발생
✅ 멱등:   MERGE INTO target USING source ON id = id → 항상 같은 결과
✅ 멱등:   DELETE partition + INSERT → 파티션 교체
```

배치 파이프라인의 **가장 중요한 원칙**. 재실행·backfill 시 데이터가 꼬이지 않음.

---

## 4. 파이프라인 관리 — 에러, 로깅, 모니터링

### 4.1 에러 처리 전략

```
┌─────────────────────────────────────────┐
│           에러 처리 계층                   │
│                                         │
│  Layer 1: 재시도 (Retry)                 │
│  ├─ 네트워크 타임아웃 → 3회 재시도         │
│  └─ API rate limit → exponential backoff│
│                                         │
│  Layer 2: 알림 (Alert)                   │
│  ├─ Slack / Email / PagerDuty           │
│  └─ 실패 태스크 + 에러 메시지 전달         │
│                                         │
│  Layer 3: Dead Letter Queue             │
│  ├─ 처리 실패한 레코드를 별도 저장         │
│  └─ 나중에 수동/자동으로 재처리            │
│                                         │
│  Layer 4: Circuit Breaker               │
│  └─ 연속 실패 시 파이프라인 일시 중단      │
└─────────────────────────────────────────┘
```

### 4.2 로깅 구성

```python
# Airflow 기본 로깅 구조
$AIRFLOW_HOME/logs/
├── dag_id/
│   ├── task_id/
│   │   ├── 2024-01-01T00:00:00+00:00/
│   │   │   └── 1.log          # 실행 로그
│   │   └── 2024-01-02T00:00:00+00:00/
│   │       └── 1.log
```

**프로덕션 로깅 권장 구성:**

| 항목 | 도구 | 설명 |
|------|------|------|
| 로그 수집 | CloudWatch, Cloud Logging, ELK | 중앙화된 로그 저장 |
| 메트릭 | Prometheus + Grafana | 처리 시간, 행 수, 에러율 |
| 알림 | Slack, PagerDuty, OpsGenie | 실패/SLA 위반 시 |
| 데이터 품질 | Great Expectations, Soda | 데이터 검증 자동화 |

### 4.3 데이터 품질 검증

```python
# Great Expectations 예시
expectation_suite = {
    "expectations": [
        {"expectation_type": "expect_column_values_to_not_be_null",
         "kwargs": {"column": "trip_id"}},
        {"expectation_type": "expect_column_values_to_be_between",
         "kwargs": {"column": "trip_duration_min", "min_value": 0, "max_value": 1440}},
        {"expectation_type": "expect_table_row_count_to_be_between",
         "kwargs": {"min_value": 1000, "max_value": 50000000}},
    ]
}
```

```sql
-- dbt test (schema.yml)
models:
  - name: stg_trips
    columns:
      - name: trip_id
        tests:
          - not_null
          - unique
      - name: trip_duration_min
        tests:
          - not_null
          - accepted_values:
              values: [0, 1440]  # 0~24시간
```

### 4.4 SLA & Observability

```
┌───────────────────────────────────────────┐
│     배치 파이프라인 Observability 대시보드    │
│                                           │
│  ┌─────────┐  ┌──────────┐  ┌──────────┐ │
│  │ 처리 시간 │  │ 처리 행 수 │  │ 에러율   │ │
│  │ avg:12m  │  │ 11.9M    │  │  0.01%   │ │
│  │ p99:25m  │  │ ±5% 이내  │  │          │ │
│  └─────────┘  └──────────┘  └──────────┘ │
│                                           │
│  SLA: "매일 오전 6시 이전 완료"              │
│  위반 시: Slack #data-alerts 알림           │
└───────────────────────────────────────────┘
```

---

## 5. 메달리온 아키텍처 — 데이터 단계별 활용

### 전체 구조

```
┌──────────────────────────────────────────────────────────────┐
│                    Medallion Architecture                     │
│                                                              │
│  Source        Bronze           Silver           Gold         │
│  ──────        ──────           ──────           ────         │
│                                                              │
│  API    ──┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   │
│  DB     ──┼──▶│ Raw Data │──▶│ Cleaned  │──▶│ Business │   │
│  Files  ──┤   │ as-is    │   │ Validated│   │ Aggregated│   │
│  Kafka  ──┘   └──────────┘   └──────────┘   └──────────┘   │
│               │            │              │              │   │
│               │ dlt/       │ dbt stg_*    │ dbt marts/*  │   │
│               │ Airbyte    │ 정제, 타입변환 │ 집계, 조인   │   │
│               │ Fivetran   │ 중복제거      │ 비즈니스 로직 │   │
└──────────────────────────────────────────────────────────────┘
```

### 각 레이어 상세

#### Bronze (Raw) — 원본 그대로

```
목적: 소스 데이터를 변경 없이 그대로 저장
도구: dlt, Airbyte, Fivetran, custom scripts
포맷: JSON, CSV, Parquet (원본 그대로)
```

| 특징 | 설명 |
|------|------|
| 변환 없음 | 소스 데이터 그대로 적재 |
| 이력 보존 | 모든 수집 이력 보관 |
| Append-only | 삭제하지 않고 추가만 |
| 용도 | 감사(audit), 재처리 |

```sql
-- Bronze 테이블 예시
CREATE TABLE bronze.raw_trips (
    _loaded_at      TIMESTAMP,       -- 적재 시각
    _source_file    STRING,          -- 원본 파일명
    raw_data        JSON             -- 원본 데이터 그대로
);
```

#### Silver (Cleaned) — 정제·검증

```
목적: 데이터 정제, 타입 변환, 중복 제거, 검증
도구: dbt (staging models), Spark
포맷: Parquet (스키마 적용됨)
```

| 특징 | 설명 |
|------|------|
| 스키마 적용 | String → Timestamp, Integer 등 |
| 중복 제거 | deduplication |
| Null 처리 | 결측값 필터링 또는 기본값 |
| 정규화 | 컬럼명 통일, 단위 변환 |

```sql
-- Silver: dbt staging model
-- models/staging/stg_trips.sql
SELECT
    hvfhs_license_num,
    dispatching_base_num,
    CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
    CAST(PULocationID AS INT) AS pickup_location_id,
    CAST(DOLocationID AS INT) AS dropoff_location_id,
    TIMESTAMP_DIFF(
        CAST(dropoff_datetime AS TIMESTAMP),
        CAST(pickup_datetime AS TIMESTAMP),
        MINUTE
    ) AS trip_duration_min
FROM {{ source('bronze', 'raw_trips') }}
WHERE pickup_datetime IS NOT NULL
  AND dropoff_datetime IS NOT NULL
  AND PULocationID IS NOT NULL
```

#### Gold (Business) — 비즈니스 레벨 집계

```
목적: 비즈니스 질문에 답하는 최종 테이블
도구: dbt (marts models)
소비자: 대시보드, ML, 리포트
```

| 특징 | 설명 |
|------|------|
| 비즈니스 로직 | 매출, KPI, 지표 계산 |
| 조인 | 여러 Silver 테이블 조인 |
| 집계 | 일별, 주별, 월별 요약 |
| 접근 제어 | 팀/역할별 권한 관리 |

```sql
-- Gold: dbt marts model
-- models/marts/daily_trip_summary.sql
SELECT
    DATE(t.pickup_datetime) AS trip_date,
    z.zone_name AS pickup_zone,
    COUNT(*) AS total_trips,
    AVG(t.trip_duration_min) AS avg_duration_min,
    COUNT(DISTINCT t.dispatching_base_num) AS unique_bases
FROM {{ ref('stg_trips') }} t
JOIN {{ ref('dim_zones') }} z
    ON t.pickup_location_id = z.location_id
GROUP BY 1, 2
```

### dbt 프로젝트 디렉토리와 메달리온 매핑

```
dbt_project/
├─ models/
│  ├─ staging/          ← Silver
│  │  ├─ stg_trips.sql
│  │  └─ stg_zones.sql
│  ├─ intermediate/     ← Silver-Gold 사이
│  │  └─ int_trips_with_zones.sql
│  └─ marts/            ← Gold
│     ├─ daily_trip_summary.sql
│     └─ monthly_revenue.sql
├─ seeds/               ← 참조 데이터 (CSV)
├─ snapshots/           ← SCD Type 2
└─ tests/               ← 데이터 품질 테스트
```

---

## 6. 배치 스케줄링과 유즈케이스

### 스케줄 주기별 정리

#### ⏱️ 시간 단위 (Hourly)

| 유즈케이스 | 설명 |
|-----------|------|
| 실시간에 가까운 대시보드 | 1시간 단위 매출/트래픽 업데이트 |
| 로그 집계 | 서버 로그 시간대별 집계 |
| 이상 탐지 전처리 | 최근 1시간 데이터로 이상치 탐지 |
| API 데이터 수집 | 외부 API를 1시간마다 폴링 |

```python
# Airflow 예시
schedule_interval = "0 * * * *"  # 매시 정각
```

#### 📅 일 단위 (Daily) — 가장 일반적

| 유즈케이스 | 설명 |
|-----------|------|
| 일별 매출 리포트 | 전일 매출 집계, 대시보드 업데이트 |
| ETL 파이프라인 | Raw → Staging → Mart 전체 흐름 |
| ML 피처 엔지니어링 | 학습용 피처 테이블 갱신 |
| 데이터 품질 체크 | 일별 데이터 검증 리포트 |
| 고객 세그먼트 업데이트 | 전일 행동 기반 세그먼트 재계산 |
| 데이터 백업/스냅샷 | 일별 전체 테이블 스냅샷 |

```python
schedule_interval = "0 6 * * *"  # 매일 오전 6시
```

#### 📆 주 단위 (Weekly)

| 유즈케이스 | 설명 |
|-----------|------|
| 주간 KPI 리포트 | 경영진/팀 리더 주간 보고서 |
| ML 모델 재학습 | 최근 1주 데이터로 모델 재학습 |
| 데이터 정합성 검증 | 주 단위 cross-system 데이터 비교 |
| 사용자 코호트 분석 | 주간 리텐션/이탈 분석 |

```python
schedule_interval = "0 2 * * 1"  # 매주 월요일 오전 2시
```

#### 🗓️ 월 단위 (Monthly)

| 유즈케이스 | 설명 |
|-----------|------|
| 월별 재무 마감 | 월 매출, 비용 집계 |
| 규제/감사 리포트 | 규정 준수용 월별 리포트 |
| 대용량 히스토리 재계산 | 전체 히스토리 기반 지표 |
| 파트너 정산 | 월별 정산 데이터 생성 |
| 데이터 아카이빙 | 오래된 데이터를 cold storage로 이동 |

```python
schedule_interval = "0 3 1 * *"  # 매월 1일 오전 3시
```

### 실전 배치 파이프라인 예시

```
[매일 오전 3시 실행]

      ┌──────────────────────────────────────────────────┐
      │                                                  │
  ①  Extract (dlt)                                      │
      │  API/DB → Bronze 테이블 적재                      │
      ▼                                                  │
  ②  Transform (dbt)                                    │
      │  Bronze → Silver (정제)                           │
      │  Silver → Gold (집계)                             │
      ▼                                                  │
  ③  Test (dbt test + Great Expectations)               │
      │  데이터 품질 검증                                  │
      ▼                                                  │
  ④  Publish                                            │
      │  대시보드 캐시 갱신                                │
      │  ML 피처 스토어 업데이트                            │
      ▼                                                  │
  ⑤  Notify                                             │
      │  Slack: "파이프라인 완료. 11.9M rows 처리"          │
      │  또는 에러 시: PagerDuty 알림                      │
      └──────────────────────────────────────────────────┘
```

### 스케줄 선택 가이드

```
데이터 신선도 요구?
│
├─ 수 초 이내 → 스트리밍 (Kafka + Flink)
│
├─ 수 분~1시간 → Micro-batch / 시간 배치
│
├─ 당일 데이터 → 일 배치 (가장 일반적)
│
├─ 트렌드/요약 → 주/월 배치
│
└─ 아카이빙 → 월/분기 배치
```

---

## 부록: 도구 선택 치트시트

```
"어떤 도구를 써야 하지?"

    데이터를 가져와야 한다 (E+L)
    ├─ API/SaaS에서 → dlt (코드) / Airbyte (UI)
    ├─ DB에서 DB로 → Airbyte / Fivetran
    └─ 파일(S3/GCS)에서 → Spark / dlt

    데이터를 변환해야 한다 (T)
    ├─ SQL로 충분 → dbt ⭐
    ├─ 복잡한 로직/ML → Spark / Python
    └─ 둘 다 → Spark SQL + dbt

    워크플로우를 관리해야 한다 (Orchestration)
    ├─ 팀이 크다/복잡 → Airflow / Dagster
    ├─ 간단/YAML 선호 → Kestra
    └─ 클라우드 네이티브 → Cloud Composer / Prefect Cloud
```

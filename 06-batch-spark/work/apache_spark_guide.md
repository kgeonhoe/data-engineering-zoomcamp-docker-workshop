# Apache Spark 완전 가이드

## 목차

1. [Apache Spark란?](#1-apache-spark란)
2. [왜 Spark를 사용하는가?](#2-왜-spark를-사용하는가)
3. [Spark 아키텍처](#3-spark-아키텍처)
4. [Transformation vs Action](#4-transformation-vs-action)
5. [DataFrame & Spark SQL](#5-dataframe--spark-sql)
6. [GroupBy 내부 작동 방식](#6-groupby-내부-작동-방식)
7. [Join 전략](#7-join-전략)
8. [Partitioning 전략](#8-partitioning-전략)
9. [spark-submit](#9-spark-submit)
10. [HDFS vs S3/GCS — 스토리지 전략](#10-hdfs-vs-s3gcs--스토리지-전략)
11. [Spark 튜닝 포인트](#11-spark-튜닝-포인트)

---

## 1. Apache Spark란?

Apache Spark는 **대규모 데이터를 분산 처리**하기 위한 통합 분석 엔진입니다.

```
한 줄 요약:
"여러 대의 컴퓨터를 하나의 컴퓨터처럼 사용하여 TB~PB 규모 데이터를 처리하는 엔진"
```

### 핵심 특성

| 특성 | 설명 |
|------|------|
| **분산 처리** | 데이터를 여러 노드에 나눠서 병렬 처리 |
| **인메모리** | 중간 결과를 메모리에 보관 → 디스크 I/O 최소화 |
| **통합 엔진** | 배치, 스트리밍, ML, 그래프 처리를 하나의 API로 |
| **다국어** | Python, Scala, Java, R, SQL 지원 |
| **Lazy Evaluation** | 실행 계획을 먼저 최적화한 뒤 한번에 실행 |

### 역사

```
2009  UC Berkeley AMPLab에서 시작
2010  오픈소스 공개
2014  Apache 최상위 프로젝트
2016  Spark 2.0 — DataFrame API, Catalyst Optimizer
2020  Spark 3.0 — Adaptive Query Execution
2023  Spark 3.5 — Spark Connect, 향상된 PySpark
```

### Spark 생태계

```
┌────────────────────────────────────────────────┐
│              Applications                       │
│  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────────────┐  │
│  │Spark │ │Spark │ │MLlib │ │GraphX        │  │
│  │SQL   │ │Stream│ │(ML)  │ │(그래프 처리)   │  │
│  └──────┘ └──────┘ └──────┘ └──────────────┘  │
│                                                │
│  ┌──────────────────────────────────────────┐  │
│  │        DataFrame / Dataset API           │  │
│  └──────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────┐  │
│  │   Catalyst Optimizer + Tungsten Engine   │  │
│  └──────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────┐  │
│  │              Spark Core (RDD)            │  │
│  └──────────────────────────────────────────┘  │
│                                                │
│  Storage: HDFS, S3, GCS, Delta Lake, Iceberg  │
│  Cluster: YARN, Kubernetes, Mesos, Standalone │
└────────────────────────────────────────────────┘
```

---

## 2. 왜 Spark를 사용하는가?

### Pandas, Polars, Dask 등이 있는데 왜?

```
"단일 머신에서 처리할 수 있으면 Pandas/Polars로 충분하다.
 그런데 데이터가 단일 머신 메모리를 초과하면?"
```

### 도구별 위치 비교

```
데이터 크기 스펙트럼:

  1MB        1GB        10GB       100GB      1TB        1PB
  │──────────│──────────│──────────│──────────│──────────│
  │  Pandas  │          │          │          │          │
  │──────────────────── │          │          │          │
  │     Polars          │          │          │          │
  │─────────────────────────────── │          │          │
  │           Dask                 │          │          │
  │────────────────────────────────────────── │          │
  │                DuckDB / DaskSQL           │          │
  │──────────────────────────────────────────────────────│
  │                    Apache Spark                      │
  │──────────────────────────────────────────────────────│
```

### 상세 비교

| 항목 | Pandas | Polars | Dask | Spark |
|------|--------|--------|------|-------|
| **처리 규모** | ~수 GB | ~수십 GB | ~수백 GB | **TB ~ PB** |
| **실행 환경** | 단일 머신 | 단일 머신 | 단일/분산 | **진정한 분산** |
| **언어** | Python | Python/Rust | Python | Python/Scala/Java/SQL |
| **메모리** | 데이터 전체 로드 | 지연 실행 | 청크 단위 | **파티션 단위 분산** |
| **생태계** | 풍부 | 성장 중 | 중간 | **매우 풍부** |
| **클러스터** | 불가 | 불가 | 가능(제한적) | **네이티브** |
| **SQL 지원** | 없음 | 있음(제한) | DaskSQL | **Spark SQL (완전)** |
| **스트리밍** | 없음 | 없음 | 제한적 | **Structured Streaming** |
| **ML** | scikit-learn | 없음 | dask-ml | **MLlib** |

### Spark가 지속적으로 쓰이는 5가지 이유

#### ① 진정한 수평 확장 (Horizontal Scaling)

```
Pandas:  서버 1대 (메모리 64GB) → 64GB 이상 데이터 처리 불가

Spark:   서버 100대 (메모리 64GB × 100) = 6.4TB
         필요하면 200대로 늘리면 됨
```

단순히 노드를 추가하면 처리 능력이 **선형**으로 증가합니다.

#### ② 클라우드 네이티브 통합

```
AWS:  EMR (Elastic MapReduce)
GCP:  Dataproc
Azure: HDInsight / Synapse Analytics

→ 클릭 몇 번으로 Spark 클러스터 생성/삭제
→ 처리할 때만 클러스터 띄우고, 끝나면 삭제 → 비용 절약
```

모든 주요 클라우드가 **매니지드 Spark 서비스**를 제공합니다.

#### ③ 배치 + 스트리밍 통합

```python
# 같은 API로 배치도, 스트리밍도 처리
# 배치
df = spark.read.parquet("s3://data/trips/")

# 스트리밍
df = spark.readStream.format("kafka").load()

# 변환 로직은 동일!
result = df.groupBy("zone").count()
```

#### ④ 검증된 안정성과 생태계

```
10년 이상 프로덕션 검증
Fortune 500 기업 80% 이상 사용
커뮤니티: 2000+ contributor, 38000+ GitHub stars
연동: Delta Lake, Iceberg, Hudi, Kafka, Cassandra, ...
```

#### ⑤ SQL 사용자도 접근 가능

```sql
-- 데이터 엔지니어가 아니어도 SQL로 TB 데이터 분석 가능
SELECT
    zone_name,
    COUNT(*) as trips,
    AVG(duration) as avg_duration
FROM trips
JOIN zones ON trips.zone_id = zones.id
GROUP BY zone_name
ORDER BY trips DESC
```

### 왜 연산이 빠를까?

#### 핵심 1: 인메모리 처리

```
MapReduce (Hadoop):
  Read Disk → Process → Write Disk → Read Disk → Process → Write Disk
  ═══════════════════════════════════════════════════════════════════
  매 단계마다 디스크 I/O → 느림

Spark:
  Read Disk → Process (Memory) → Process (Memory) → Write Disk
  ═════════════════════════════════════════════════════════════
  중간 결과를 메모리에 유지 → 디스크 I/O 최소화
  
→ MapReduce 대비 최대 100배 빠름 (Spark 공식 벤치마크)
```

#### 핵심 2: Catalyst Optimizer (쿼리 최적화)

```
사용자가 작성한 코드:
  df.filter(col("age") > 30).select("name", "age").filter(col("name") != "unknown")

Catalyst가 최적화한 코드:
  1. Predicate Pushdown: 필터를 데이터 읽기 단계로 밀어넣음
  2. Filter 합치기: 두 filter를 하나로 병합
  3. Column Pruning: 필요한 컬럼만 읽기
  
  → 실제 실행: 파일에서 name, age만 읽되 age>30 AND name!="unknown"인 것만
```

```
최적화 파이프라인:

  User Code (논리 계획)
      │
      ▼
  ┌──────────────────────┐
  │  Analysis            │  이름 해석, 타입 검증
  └──────────┬───────────┘
             ▼
  ┌──────────────────────┐
  │  Logical Optimization│  Predicate Pushdown, Column Pruning
  └──────────┬───────────┘
             ▼
  ┌──────────────────────┐
  │  Physical Planning   │  최적 실행 전략 선택 (Sort Merge vs Broadcast Join)
  └──────────┬───────────┘
             ▼
  ┌──────────────────────┐
  │  Code Generation     │  JVM 바이트코드 직접 생성 (Whole-Stage CodeGen)
  └──────────────────────┘
```

#### 핵심 3: Tungsten 엔진 (메모리 최적화)

```
일반 JVM 객체:
  객체 헤더 (16B) + 필드 포인터 + 실제 데이터 → 오버헤드 큼

Tungsten:
  Off-heap 메모리에 바이너리 포맷으로 직접 저장
  → GC 부담 감소, 캐시 히트율 향상, 메모리 효율 2~5배
```

#### 핵심 4: Adaptive Query Execution (AQE) — Spark 3.0+

```
기존: 실행 전에 통계 기반으로 계획 수립 → 실행

AQE:  실행 도중 실제 데이터 통계를 보고 계획 수정
      
      예: Join 시 한쪽이 예상보다 작으면
          Sort Merge Join → Broadcast Join으로 전환
          
      예: 셔플 후 파티션이 너무 작으면
          작은 파티션들을 자동으로 합침 (Coalesce)
```

### 그래서 언제 뭘 써야 하나?

```
의사결정 트리:

  데이터 크기?
  │
  ├─ < 1GB         → Pandas (간단), Polars (빠름)
  │
  ├─ 1GB ~ 10GB    → Polars (단일 머신에서 최강)
  │                   DuckDB (SQL 선호 시)
  │
  ├─ 10GB ~ 100GB  → Polars (고성능 머신) or Spark (클러스터)
  │
  └─ > 100GB       → Spark ⭐ (사실상 유일한 선택)
  
  추가 고려:
  ├─ 스트리밍 필요   → Spark / Flink
  ├─ ML 포함        → Spark (MLlib)
  ├─ 클라우드 팀     → Spark (EMR/Dataproc)
  └─ 로컬 분석      → Polars / DuckDB
```

---

## 3. Spark 아키텍처

### 클러스터 구조

```
┌─────────────────────────────────────────────────────────────┐
│                     Spark Application                        │
│                                                             │
│  ┌───────────────────────────┐                              │
│  │      Driver Program       │                              │
│  │  ┌─────────────────────┐  │                              │
│  │  │    SparkContext /    │  │                              │
│  │  │    SparkSession     │  │                              │
│  │  └─────────┬───────────┘  │                              │
│  │            │              │                              │
│  │  ┌─────────▼───────────┐  │                              │
│  │  │   DAG Scheduler     │  │                              │
│  │  │   Task Scheduler    │  │                              │
│  │  └─────────────────────┘  │                              │
│  └───────────┬───────────────┘                              │
│              │                                              │
│  ┌───────────▼───────────────┐                              │
│  │    Cluster Manager         │                              │
│  │  (YARN / K8s / Standalone) │                              │
│  └───────────┬───────────────┘                              │
│              │                                              │
│    ┌─────────┼──────────┐                                   │
│    ▼         ▼          ▼                                   │
│  ┌──────┐ ┌──────┐ ┌──────┐                                │
│  │Worker│ │Worker│ │Worker│                                │
│  │Node 1│ │Node 2│ │Node 3│                                │
│  │      │ │      │ │      │                                │
│  │[Exec]│ │[Exec]│ │[Exec]│  Executor = JVM 프로세스        │
│  │      │ │      │ │      │                                │
│  │[T][T]│ │[T][T]│ │[T][T]│  T = Task (파티션 1개 처리)     │
│  │      │ │      │ │      │                                │
│  │[Cache]│ │[Cache]│ │[Cache]│  Cache = 메모리 캐시           │
│  └──────┘ └──────┘ └──────┘                                │
└─────────────────────────────────────────────────────────────┘
```

### 각 구성 요소

| 구성 요소 | 역할 | 비유 |
|-----------|------|------|
| **Driver** | 프로그램 진입점. DAG 생성, 태스크 분배, 결과 수집 | 지휘자 |
| **SparkSession** | Spark 기능 통합 진입점 (Spark 2.0+) | 리모컨 |
| **Cluster Manager** | 리소스(CPU, 메모리) 할당. YARN, K8s, Standalone | 인사팀 |
| **Worker Node** | 실제 계산을 수행하는 물리 머신 | 공장 |
| **Executor** | Worker 위에서 동작하는 JVM 프로세스 | 작업 라인 |
| **Task** | 하나의 파티션을 처리하는 최소 작업 단위 | 작업자 |

### 실행 흐름

```
① spark.read.parquet("...")               # Driver: 논리 계획 생성
② .filter(col("age") > 30)               # Driver: 논리 계획에 추가
③ .groupBy("dept").count()                # Driver: 논리 계획에 추가
④ .show()                                 # Action! Catalyst 최적화 시작
     │
     ▼
⑤ Catalyst Optimizer                      # 논리 → 물리 계획 변환
     │
     ▼
⑥ DAG Scheduler                           # Stage 분할 (셔플 경계)
     │
     ▼
⑦ Task Scheduler                          # 각 Executor에 태스크 배정
     │
     ▼
⑧ Executor에서 병렬 실행                    # 결과 → Driver로 반환
```

### Job → Stage → Task 계층

```
Job (Action 하나 = Job 하나)
│
├─ Stage 0 (Shuffle 이전)
│   ├─ Task 0  (Partition 0: read + filter)
│   ├─ Task 1  (Partition 1: read + filter)
│   └─ Task 2  (Partition 2: read + filter)
│
│   ── Shuffle (데이터 재분배) ──
│
└─ Stage 1 (Shuffle 이후)
    ├─ Task 0  (부서A 집계)
    ├─ Task 1  (부서B 집계)
    └─ Task 2  (부서C 집계)
```

**Stage 분할 기준**: 셔플(데이터 재분배)이 필요한 시점에서 끊김
- `groupBy`, `join`, `repartition`, `distinct` → 새 Stage

### Deploy Mode: Client vs Cluster

```
Client Mode (개발/테스트):
  Driver가 제출한 머신에서 실행 → 로그를 바로 볼 수 있음
  
  [내 노트북] ← Driver 여기서 실행
      │
      ▼
  [Cluster]
  ├─ Executor 1
  ├─ Executor 2
  └─ Executor 3

Cluster Mode (프로덕션):
  Driver가 클러스터 내부에서 실행 → 안정적, 제출 머신과 독립
  
  [내 노트북] → spark-submit 제출 후 끊어도 OK
      │
      ▼
  [Cluster]
  ├─ Driver (클러스터 노드에서 실행)
  ├─ Executor 1
  ├─ Executor 2
  └─ Executor 3
```

### RDD vs DataFrame

#### RDD (Resilient Distributed Dataset)

```python
# RDD 방식 (저수준)
rdd = sc.textFile("data.csv")
rdd = rdd.map(lambda line: line.split(","))
rdd = rdd.filter(lambda x: int(x[2]) > 30)
rdd = rdd.map(lambda x: (x[3], 1))
rdd = rdd.reduceByKey(lambda a, b: a + b)
rdd.collect()
```

- Spark의 **원래** 데이터 추상화 (2011~)
- Java 객체로 저장 → GC 부담, 메모리 비효율
- 최적화 **불가** — Spark가 내부 구조를 모름

#### DataFrame (구조화된 데이터)

```python
# DataFrame 방식 (고수준)
df = spark.read.option("header", "true").csv("data.csv")
df = df.filter(col("age") > 30)
result = df.groupBy("department").count()
result.show()
```

- **스키마가 있는** 분산 테이블 (2015~, Spark 1.3)
- Catalyst Optimizer가 **자동 최적화**
- Tungsten 엔진으로 **바이너리 메모리 관리**
- SQL로도 동일하게 표현 가능

#### 비교

| 항목 | RDD | DataFrame |
|------|-----|-----------|
| **추상화 수준** | 낮음 (람다 함수) | 높음 (컬럼 연산) |
| **스키마** | 없음 (비구조화) | 있음 (구조화) |
| **최적화** | 수동 (개발자 책임) | **Catalyst 자동 최적화** |
| **메모리** | Java 객체 → 비효율 | **Tungsten 바이너리 → 효율** |
| **타입 안전** | 컴파일 타임 | 런타임 |
| **언어 성능** | Scala >> Python | **Scala ≈ Python** (같은 실행 계획) |
| **SQL 통합** | 불가 | **완벽 통합** |

#### 요즘 RDD를 잘 안 쓰는 이유

```
이유 1: DataFrame이 더 빠름
  ─────────────────────────────────────
  같은 작업을 RDD vs DataFrame으로 하면
  DataFrame이 2~10배 빠름 (Catalyst + Tungsten 덕분)

이유 2: Python에서 성능 차이
  ─────────────────────────────────────
  RDD:       Python 객체 → 직렬화 → JVM → 역직렬화 → Python
             (매 연산마다 Python ↔ JVM 왕복)
  
  DataFrame:  JVM 내부에서 최적화된 코드로 실행
             (Python은 실행 계획만 전달, 실제 연산은 JVM)
  
  → PySpark에서 RDD는 극도로 느림

이유 3: 코드 가독성
  ─────────────────────────────────────
  # RDD (읽기 어려움)
  rdd.map(lambda x: (x[3], int(x[2]))) \
     .reduceByKey(lambda a,b: a+b)
  
  # DataFrame (읽기 쉬움)
  df.groupBy("department").agg(F.sum("salary"))
  
  # SQL (누구나 읽을 수 있음)
  SELECT department, SUM(salary) FROM employees GROUP BY department

이유 4: API 통합
  ─────────────────────────────────────
  DataFrame API = Spark SQL = Dataset API (Scala)
  모두 같은 Catalyst Optimizer를 거침
  → 어떤 API든 같은 실행 계획, 같은 성능

이유 5: 생태계 지원
  ─────────────────────────────────────
  Delta Lake, Iceberg, Hudi → DataFrame 기반
  MLlib → DataFrame 기반 (ML Pipeline)
  Structured Streaming → DataFrame 기반
  새로운 기능은 모두 DataFrame/Dataset 위에 구축됨
```

**RDD를 아직 쓰는 경우:**
- 비구조화 데이터 (바이너리 파일 등) 저수준 처리
- accumulator, broadcast 등 저수준 제어 필요
- 레거시 코드 유지보수

---

## 4. Transformation vs Action

### Lazy Evaluation (지연 평가)

```
Spark의 가장 중요한 개념:

  "Transformation은 실행 계획만 쌓고, Action이 호출될 때 비로소 실행한다"
```

```python
# ① Transformation — 아무것도 실행되지 않음 (계획만 쌓임)
df = spark.read.parquet("trips/")           # 계획: 파일 읽기
df2 = df.filter(col("age") > 30)            # 계획: 필터 추가
df3 = df2.groupBy("dept").count()           # 계획: 집계 추가

# 여기까지 0줄도 읽지 않음!

# ② Action — 이 순간 전체 실행 시작!
df3.show()                                  # → Catalyst 최적화 → 실행
```

### 왜 Lazy?

```
이유 1: 최적화 기회
  ─────────────────
  모든 연산을 한번에 보고 최적화할 수 있음
  
  예: filter → select → filter
      → Catalyst가 filter 2개를 합치고, 필요한 컬럼만 읽도록 최적화

이유 2: 불필요한 계산 방지
  ─────────────────────────
  df = heavy_computation()
  df.take(5)  # 5개만 필요한데 전체를 계산할 필요 없음
              # Spark가 알아서 5개만 계산하고 멈춤
```

### Transformation (변환) — Lazy

**Narrow Transformation** (셔플 없음 — 파티션 내부에서만 처리)

| 연산 | 설명 | 예시 |
|------|------|------|
| `select()` | 컬럼 선택 | `df.select("name", "age")` |
| `filter()` / `where()` | 행 필터링 | `df.filter(col("age") > 30)` |
| `withColumn()` | 컬럼 추가/변경 | `df.withColumn("age2", col("age")*2)` |
| `drop()` | 컬럼 삭제 | `df.drop("temp_col")` |
| `map()` | 행 단위 변환 (RDD) | `rdd.map(lambda x: x*2)` |
| `flatMap()` | 행 → 여러 행 | `rdd.flatMap(lambda x: x.split())` |
| `union()` | 두 DF 합치기 | `df1.union(df2)` |

```
Narrow: 각 파티션이 독립적으로 처리 (네트워크 통신 없음)

  Partition 0 ──filter──▶ Partition 0'
  Partition 1 ──filter──▶ Partition 1'
  Partition 2 ──filter──▶ Partition 2'
```

**Wide Transformation** (셔플 발생 — 데이터 재분배 필요)

| 연산 | 설명 | 예시 |
|------|------|------|
| `groupBy()` | 그룹별 집계 | `df.groupBy("dept").count()` |
| `join()` | 두 DF 조인 | `df1.join(df2, "id")` |
| `repartition()` | 파티션 재분배 | `df.repartition(24)` |
| `distinct()` | 중복 제거 | `df.distinct()` |
| `orderBy()` / `sort()` | 정렬 | `df.orderBy("age")` |
| `reduceByKey()` | 키별 축소 (RDD) | `rdd.reduceByKey(lambda a,b: a+b)` |

```
Wide: 파티션 간 데이터 이동 필요 (네트워크 셔플)

  Partition 0 ──┐
  Partition 1 ──┼── Shuffle ──▶ Partition 0' (dept=Engineering)
  Partition 2 ──┘              Partition 1' (dept=Marketing)
                               Partition 2' (dept=Sales)
```

### Action (실행) — 즉시 실행 트리거

| 연산 | 설명 | 반환 |
|------|------|------|
| `show()` / `display()` | 상위 N개 출력 | 없음 (콘솔 출력) |
| `count()` | 행 수 | 정수 |
| `collect()` | 전체 데이터를 Driver로 | 리스트 (⚠️ 대용량 주의) |
| `take(n)` / `head(n)` | 상위 N개 | 리스트 |
| `first()` | 첫 번째 행 | Row |
| `write.parquet()` | 파일 저장 | 없음 |
| `foreach()` | 각 행에 함수 적용 | 없음 |
| `reduce()` | 축소 연산 | 값 |
| `toPandas()` | Pandas DF로 변환 | pandas.DataFrame |

### 전체 흐름 예시

```python
# ── Transformations (Lazy — 계획만 쌓임) ──────────────

df = spark.read.parquet("trips/")             # T: 파일 읽기 계획

df2 = df.filter(col("duration") > 0)          # T: Narrow (필터)

df3 = df2.withColumn(                          # T: Narrow (컬럼 추가)
    "hour", F.hour("pickup_datetime")
)

df4 = df3.groupBy("hour").agg(                 # T: Wide (셔플!)
    F.count("*").alias("trips"),
    F.avg("duration").alias("avg_duration")
)

df5 = df4.orderBy("hour")                     # T: Wide (정렬!)

# ── Action (실행 트리거!) ─────────────────────────────

df5.show()                                     # A: 지금 실행!

# 실행 순서:
# Stage 0: read → filter → withColumn (Narrow, 셔플 없음)
#   ── Shuffle ──
# Stage 1: groupBy 집계
#   ── Shuffle ──
# Stage 2: orderBy 정렬 → show 출력
```

### 실행 계획 확인

```python
# 논리 + 물리 실행 계획 확인
df5.explain(True)

# 출력 예시:
# == Parsed Logical Plan ==
# Sort [hour ASC]
# +- Aggregate [hour], [hour, count(1) AS trips, avg(duration) AS avg_duration]
#    +- Project [*, hour(pickup_datetime) AS hour]
#       +- Filter (duration > 0)
#          +- Relation [parquet] trips/
#
# == Optimized Logical Plan ==
# Sort [hour ASC]
# +- Aggregate [hour], [...]
#    +- Project [pickup_datetime, duration, hour(pickup_datetime) AS hour]  ← Column Pruning!
#       +- Filter (isnotnull(duration) AND (duration > 0))                ← Predicate Pushdown!
#          +- Relation [parquet] trips/
```

---

## 5. DataFrame & Spark SQL

### Spark SQL이란?

DataFrame 연산을 **SQL 문법**으로 작성할 수 있게 하는 모듈입니다.
DataFrame API와 **100% 동일한** Catalyst Optimizer를 거치므로 성능 차이가 없습니다.

```python
# DataFrame API
result = df.filter(col("age") > 30) \
           .groupBy("department") \
           .agg(F.avg("salary").alias("avg_salary")) \
           .orderBy(F.desc("avg_salary"))

# Spark SQL — 완전히 동일한 실행 계획, 동일한 성능
df.createOrReplaceTempView("employees")

result = spark.sql("""
    SELECT department, AVG(salary) AS avg_salary
    FROM employees
    WHERE age > 30
    GROUP BY department
    ORDER BY avg_salary DESC
""")
```

### 어떨 때 Spark SQL을 쓰는가?

| 상황 | 추천 | 이유 |
|------|------|------|
| SQL에 익숙한 분석가와 협업 | **Spark SQL** | SQL은 보편적 언어 |
| 복잡한 조인/서브쿼리 | **Spark SQL** | SQL이 더 읽기 쉬운 경우 多 |
| 동적 컬럼 처리, UDF | **DataFrame API** | Python 코드가 유연 |
| ML 파이프라인 | **DataFrame API** | MLlib가 DataFrame 기반 |
| 기존 SQL 자산 활용 | **Spark SQL** | 기존 쿼리 재사용 |
| ETL 파이프라인 (혼합) | **둘 다 혼용** | 상황에 맞게 선택 |

### 현업에서 Spark SQL 쓰는 파이프라인 사례

#### Case 1: 데이터 웨어하우스 ETL

```python
# Bronze → Silver → Gold 를 Spark SQL로 처리
# 장점: SQL을 아는 누구나 파이프라인 이해/수정 가능

# Silver 정제
spark.sql("""
    CREATE OR REPLACE TABLE silver.trips AS
    SELECT
        CAST(pickup_datetime AS TIMESTAMP) AS pickup_dt,
        CAST(dropoff_datetime AS TIMESTAMP) AS dropoff_dt,
        CAST(PULocationID AS INT) AS pickup_zone_id,
        CAST(DOLocationID AS INT) AS dropoff_zone_id,
        TIMESTAMPDIFF(MINUTE, pickup_datetime, dropoff_datetime) AS duration_min
    FROM bronze.raw_trips
    WHERE pickup_datetime IS NOT NULL
""")

# Gold 집계
spark.sql("""
    CREATE OR REPLACE TABLE gold.daily_summary AS
    SELECT
        DATE(pickup_dt) AS trip_date,
        z.zone_name,
        COUNT(*) AS total_trips,
        AVG(duration_min) AS avg_duration
    FROM silver.trips t
    JOIN silver.zones z ON t.pickup_zone_id = z.zone_id
    GROUP BY 1, 2
""")
```

#### Case 2: Databricks / EMR 팀 협업

```
데이터 엔지니어: Python + DataFrame API로 파이프라인 구축
데이터 분석가:   Spark SQL로 노트북에서 쿼리
ML 엔지니어:    DataFrame API + MLlib

→ 같은 데이터, 같은 엔진, 다른 인터페이스
```

#### Case 3: SQL 기반 스케줄링 파이프라인

```python
# Airflow에서 Spark SQL 실행
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator

task = SparkSqlOperator(
    task_id="daily_aggregation",
    sql="""
        INSERT OVERWRITE TABLE gold.daily_revenue
        SELECT DATE(order_date), SUM(amount)
        FROM silver.orders
        WHERE order_date = '{{ ds }}'
        GROUP BY 1
    """,
    master="yarn",
    conn_id="spark_default"
)
```

### DataFrame API vs Spark SQL 혼합 사용 (실전 패턴)

```python
# 읽기: DataFrame API
raw_df = spark.read.parquet("s3://bucket/raw/trips/")

# 뷰 등록
raw_df.createOrReplaceTempView("raw_trips")

# 정제: Spark SQL (복잡한 조인/윈도우 함수는 SQL이 편함)
cleaned = spark.sql("""
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY updated_at DESC) AS rn
    FROM raw_trips
    QUALIFY rn = 1  -- 중복 제거
""")

# 후처리: DataFrame API (동적 로직은 Python이 편함)
for col_name in nullable_columns:
    cleaned = cleaned.fillna({col_name: default_values[col_name]})

# 저장: DataFrame API
cleaned.write.mode("overwrite").parquet("s3://bucket/silver/trips/")
```

---

## 6. GroupBy 내부 작동 방식

### 기본 원리

`groupBy`는 **2단계**로 동작합니다:

```
단계 1: Map-side (Partial) Aggregation — 로컬에서 먼저 집계
단계 2: Reduce-side (Final) Aggregation — 셔플 후 최종 집계
```

### 구체적 흐름

```python
df.groupBy("department").agg(F.count("*").alias("cnt"), F.sum("salary").alias("total"))
```

```
=== BEFORE groupBy ===

Partition 0:              Partition 1:              Partition 2:
┌──────┬────────┐        ┌──────┬────────┐        ┌──────┬────────┐
│ dept │ salary │        │ dept │ salary │        │ dept │ salary │
├──────┼────────┤        ├──────┼────────┤        ├──────┼────────┤
│ Eng  │ 100    │        │ Mkt  │ 80     │        │ Eng  │ 110    │
│ Mkt  │ 90     │        │ Eng  │ 120    │        │ Mkt  │ 85     │
│ Eng  │ 105    │        │ Mkt  │ 75     │        │ Sales│ 70     │
│ Sales│ 60     │        │ Sales│ 65     │        │ Eng  │ 95     │
└──────┴────────┘        └──────┴────────┘        └──────┴────────┘


=== Stage 1: Partial Aggregation (Map-side) ===
각 파티션 내부에서 먼저 부분 집계 (셔플 데이터 줄이기)

Partition 0:              Partition 1:              Partition 2:
┌──────┬─────┬───────┐   ┌──────┬─────┬───────┐   ┌──────┬─────┬───────┐
│ dept │ cnt │ total │   │ dept │ cnt │ total │   │ dept │ cnt │ total │
├──────┼─────┼───────┤   ├──────┼─────┼───────┤   ├──────┼─────┼───────┤
│ Eng  │ 2   │ 205   │   │ Mkt  │ 2   │ 155   │   │ Eng  │ 2   │ 205   │
│ Mkt  │ 1   │ 90    │   │ Eng  │ 1   │ 120   │   │ Mkt  │ 1   │ 85    │
│ Sales│ 1   │ 60    │   │ Sales│ 1   │ 65    │   │ Sales│ 1   │ 70    │
└──────┴─────┴───────┘   └──────┴─────┴───────┘   └──────┴─────┴───────┘

  ↓ 12행 → 9행으로 축소 (셔플 데이터 25% 감소!)


=== Shuffle (네트워크 전송) ===
같은 department 키를 가진 행들이 같은 파티션으로 모임

  Eng  행들 ──────→ Reduce Partition 0
  Mkt  행들 ──────→ Reduce Partition 1
  Sales행들 ──────→ Reduce Partition 2

Hash Partitioning: hash("Eng") % 3 = 0
                   hash("Mkt") % 3 = 1
                   hash("Sales") % 3 = 2


=== Stage 2: Final Aggregation (Reduce-side) ===

Reduce Partition 0:       Reduce Partition 1:       Reduce Partition 2:
┌──────┬─────┬───────┐   ┌──────┬─────┬───────┐   ┌──────┬─────┬───────┐
│ Eng  │ 2   │ 205   │   │ Mkt  │ 2   │ 155   │   │ Sales│ 1   │ 60    │
│ Eng  │ 1   │ 120   │   │ Mkt  │ 1   │ 90    │   │ Sales│ 1   │ 65    │
│ Eng  │ 2   │ 205   │   │ Mkt  │ 1   │ 85    │   │ Sales│ 1   │ 70    │
└──────┴─────┴───────┘   └──────┴─────┴───────┘   └──────┴─────┴───────┘
         │ 합산                   │ 합산                   │ 합산
         ▼                       ▼                       ▼
┌──────┬─────┬───────┐   ┌──────┬─────┬───────┐   ┌──────┬─────┬───────┐
│ Eng  │  5  │  530  │   │ Mkt  │  4  │  330  │   │Sales │  3  │  195  │
└──────┴─────┴───────┘   └──────┴─────┴───────┘   └──────┴─────┴───────┘


=== 최종 결과 ===

+──────────+─────+───────+
│department│ cnt │ total │
+──────────+─────+───────+
│ Eng      │  5  │  530  │
│ Mkt      │  4  │  330  │
│ Sales    │  3  │  195  │
+──────────+─────+───────+
```

### GroupBy 최적화 포인트

#### ① Partial Aggregation 효과

```
Without Partial Agg:
  12행 셔플 → 네트워크 전송 12행

With Partial Agg:
  9행 셔플 → 네트워크 전송 9행 (25% 감소)

실제 데이터 (수억 행):
  Without: 1억 행 셔플
  With:    1만 행 셔플 (그룹 수만큼만!)
  → 셔플 데이터 99.99% 감소!
```

#### ② 데이터 스큐 문제

```
문제: 특정 키에 데이터가 몰림

  dept="Eng"  → 900만 행 → Reduce 파티션 0: 혼자서 900만 행 처리 (병목!)
  dept="Mkt"  → 50만 행
  dept="Sales"→ 50만 행
  
해결 방법:

  방법 1: Salting (키에 랜덤 값 추가)
  ──────────────────────────────────
  df = df.withColumn("salt", F.concat(col("dept"), F.lit("_"), (F.rand()*10).cast("int")))
  # "Eng" → "Eng_0", "Eng_1", ..., "Eng_9"
  # 900만 행이 10개 파티션으로 분산!
  
  agg1 = df.groupBy("salt").agg(F.sum("salary"))  # 분산 집계
  agg2 = agg1.withColumn("dept", F.split(col("salt"), "_")[0]) \
              .groupBy("dept").agg(F.sum("sum(salary)"))  # 최종 합산

  방법 2: AQE (Spark 3.0+) 자동 처리
  ──────────────────────────────────
  spark.conf.set("spark.sql.adaptive.enabled", "true")
  spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
  # Spark가 런타임에 스큐를 감지하고 자동으로 파티션 분할
```

#### ③ GroupBy + Join (Shuffle 최소화)

```python
# 안 좋은 패턴: groupBy 후 join → 셔플 2번
grouped = trips.groupBy("zone_id").count()
result = grouped.join(zones, "zone_id")  # 셔플 2번

# 좋은 패턴: broadcast join → 셔플 1번
from pyspark.sql.functions import broadcast
grouped = trips.groupBy("zone_id").count()
result = grouped.join(broadcast(zones), "zone_id")  # 셔플 1번 (zones를 각 노드에 복사)
```

```
Broadcast Join 조건:
  작은 테이블 < spark.sql.autoBroadcastJoinThreshold (기본 10MB)
  → 자동으로 Broadcast Join 선택
  
  수동 힌트:
  SELECT /*+ BROADCAST(zones) */ ...
  FROM trips JOIN zones ON trips.zone_id = zones.zone_id
```

### GroupBy Sort-based vs Hash-based

```
Spark는 기본적으로 Sort-based Aggregation 사용:

  1. 같은 키를 가진 데이터를 셔플로 모음
  2. 키로 정렬
  3. 순차적으로 스캔하며 집계

  장점: 메모리 효율 (전체 데이터를 메모리에 올릴 필요 없음)
  단점: 정렬 비용

Hash-based는 RDD의 reduceByKey 등에서 사용:
  1. 해시 테이블에 키별로 집계
  장점: 정렬 없이 빠름
  단점: 메모리에 해시 테이블 유지 필요
```

---

## 7. Join 전략

### Join이 왜 비싼 연산인가?

```
Join = 두 테이블에서 같은 키를 가진 행을 찾아 합치는 것

문제: 데이터가 여러 노드에 분산되어 있으므로
      같은 키를 가진 행이 서로 다른 노드에 있을 수 있음
      → 네트워크를 통해 데이터를 이동(셔플)해야 함
      → 셔플 = 가장 비싼 연산
```

### Spark의 5가지 Join 전략

| 전략 | 셔플 | 조건 | 속도 |
|------|------|------|------|
| **Broadcast Hash Join** | 없음 | 한쪽 < 10MB | 가장 빠름 |
| **Shuffle Hash Join** | 있음 | 한쪽이 상대적으로 작음 | 빠름 |
| **Sort Merge Join** | 있음 | 양쪽 다 큼 (기본 전략) | 보통 |
| **Broadcast Nested Loop** | 없음 | non-equi join + 한쪽 작음 | 느림 |
| **Cartesian Product** | 있음 | cross join | 매우 느림 |

---

### ① Broadcast Hash Join (BHJ) — 가장 빠름

```
핵심: 작은 테이블을 통째로 모든 Executor에 복사(broadcast)
      → 셔플 없이 각 파티션에서 로컬 조인
```

```
┌─────────────────────────────────────────────────────────┐
│                    Driver                                │
│  작은 테이블 (zones: 265행, 50KB)                         │
│       │                                                 │
│       │ broadcast                                       │
│       ├──────────┬──────────┬──────────┐                │
│       ▼          ▼          ▼          ▼                │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐       │
│  │Executor1│ │Executor2│ │Executor3│ │Executor4│       │
│  │         │ │         │ │         │ │         │       │
│  │trips    │ │trips    │ │trips    │ │trips    │       │
│  │Part 0   │ │Part 1   │ │Part 2   │ │Part 3   │       │
│  │  +      │ │  +      │ │  +      │ │  +      │       │
│  │zones    │ │zones    │ │zones    │ │zones    │       │
│  │(전체복사)│ │(전체복사)│ │(전체복사)│ │(전체복사)│       │
│  │         │ │         │ │         │ │         │       │
│  │→로컬Join│ │→로컬Join│ │→로컬Join│ │→로컬Join│       │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘       │
│                                                         │
│  셔플: 없음!  네트워크: 작은 테이블 × N번 복사만             │
└─────────────────────────────────────────────────────────┘
```

```python
# 자동: 한쪽이 autoBroadcastJoinThreshold (기본 10MB) 이하면 자동 선택
result = trips.join(zones, trips.zone_id == zones.zone_id)

# 수동 힌트 (테이블이 10MB 넘지만 강제로 broadcast)
from pyspark.sql.functions import broadcast
result = trips.join(broadcast(zones), trips.zone_id == zones.zone_id)

# SQL 힌트
result = spark.sql("""
    SELECT /*+ BROADCAST(z) */ *
    FROM trips t
    JOIN zones z ON t.zone_id = z.zone_id
""")
```

```
언제 사용?
  ✅ 한쪽 테이블이 작을 때 (수 MB ~ 수백 MB)
  ✅ lookup/dimension 테이블 조인 (zones, categories 등)
  ✅ 가장 빠른 Join이 필요할 때
  
주의:
  ❌ 큰 테이블을 broadcast하면 OOM (각 Executor 메모리에 복사)
  ❌ broadcast 테이블이 Executor 메모리의 70% 이하여야 안전
```

---

### ② Sort Merge Join (SMJ) — 기본 전략

```
핵심: 양쪽 테이블을 Join 키로 정렬한 뒤, 정렬된 순서로 병합
      → 양쪽 다 큰 경우의 기본 전략
```

```
=== Phase 1: Shuffle (양쪽 테이블을 같은 키 기준으로 재분배) ===

Trips 테이블:                        Orders 테이블:
Part 0: [A,C,B,A]                   Part 0: [B,A,C]
Part 1: [B,A,C,B]                   Part 1: [A,B,A]
    │                                    │
    │ Shuffle (hash(key) % 3)           │ Shuffle (hash(key) % 3)
    ▼                                    ▼
Part 0': [A,A,A,A]                  Part 0': [A,A,A]
Part 1': [B,B,B]                    Part 1': [B,B]
Part 2': [C,C]                      Part 2': [C]


=== Phase 2: Sort (각 파티션 내부 정렬) ===

Trips Part 0': [A,A,A,A] (이미 정렬)   Orders Part 0': [A,A,A] (이미 정렬)
Trips Part 1': [B,B,B] (이미 정렬)     Orders Part 1': [B,B] (이미 정렬)
Trips Part 2': [C,C] (이미 정렬)       Orders Part 2': [C] (이미 정렬)


=== Phase 3: Merge (정렬된 두 테이블을 포인터로 병합) ===

Trips:  [A, A, A, A]     Orders: [A, A, A]
         ↑ ptr1                   ↑ ptr2
         
두 포인터가 같은 값이면 → 결과에 추가
ptr1 < ptr2 → ptr1 전진
ptr1 > ptr2 → ptr2 전진

→ 각 테이블을 한 번씩만 스캔 = O(N + M)
```

```python
# 대부분의 경우 Spark가 자동으로 SMJ 선택 (양쪽 다 클 때)
result = big_table_1.join(big_table_2, "user_id")

# 수동 힌트
result = spark.sql("""
    SELECT /*+ MERGE(t1, t2) */ *
    FROM big_table_1 t1
    JOIN big_table_2 t2 ON t1.user_id = t2.user_id
""")
```

```
언제 사용?
  ✅ 양쪽 테이블이 모두 클 때 (GB ~ TB)
  ✅ 등호 조인(equi-join)일 때  (a.key = b.key)
  ✅ 데이터가 이미 조인 키로 정렬/파티셔닝되어 있으면 셔플 생략 가능

성능 특성:
  - 셔플: 양쪽 모두 발생
  - 정렬: 양쪽 모두 필요
  - 메모리: 전체 데이터를 메모리에 올릴 필요 없음 (스트리밍 병합)
  - 안정성: 대규모 데이터에서 가장 안정적
```

---

### ③ Shuffle Hash Join (SHJ)

```
핵심: 셔플 후 해시 테이블로 조인 (정렬 없음)
      → SMJ보다 빠를 수 있지만 메모리 필요
```

```
=== Phase 1: Shuffle (SMJ와 동일) ===
양쪽 테이블을 join 키 기준으로 같은 파티션에 모음

=== Phase 2: Hash (정렬 대신 해시 테이블) ===

  작은 쪽 (build side):  해시 테이블 구축
  ┌─────────────────────┐
  │ hash(A) → [row1]    │
  │ hash(B) → [row2]    │
  │ hash(C) → [row3]    │
  └─────────────────────┘
  
  큰 쪽 (probe side):   해시 테이블에서 매칭 검색
  row_x → hash(key) → 해시 테이블에서 O(1) 조회
  
→ 정렬 비용 없음, 하지만 해시 테이블이 메모리에 들어가야 함
```

```python
# Spark는 기본적으로 SHJ를 비활성화 (SMJ 선호)
# 활성화하려면:
spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")

# SQL 힌트
result = spark.sql("""
    SELECT /*+ SHUFFLE_HASH(small_table) */ *
    FROM big_table
    JOIN small_table ON big_table.id = small_table.id
""")
```

```
언제 사용?
  ✅ 한쪽이 broadcast하기엔 크지만, SMJ 정렬보다 해시가 유리할 때
  ✅ 한쪽 파티션이 메모리에 들어갈 정도로 작을 때
  
주의:
  ❌ 빌드 사이드가 메모리를 초과하면 디스크 스필 → 성능 급감
  ❌ Spark 기본값은 SMJ 선호 (더 안정적이므로)
```

---

### ④ Broadcast Nested Loop Join (BNLJ)

```
핵심: non-equi join (부등호, BETWEEN 등)에서 한쪽이 작을 때
      작은 쪽을 broadcast하고, 모든 행 조합을 검사
```

```python
# non-equi join 예시
result = events.join(
    broadcast(time_ranges),
    (events.timestamp >= time_ranges.start) &
    (events.timestamp < time_ranges.end)
)
```

```
  events의 각 행 × time_ranges의 모든 행 비교
  → O(N × M) 이지만 time_ranges가 작으면 실용적
```

---

### ⑤ Cartesian Product (Cross Join)

```
핵심: 조인 조건 없이 모든 조합 생성
      N행 × M행 = N×M 행 → 거의 항상 피해야 함
```

```python
# 명시적 cross join
result = df1.crossJoin(df2)

# 주의: 실수로 cross join이 발생하는 경우
result = df1.join(df2)  # 조건 없음 → cross join!
```

---

### Join 전략 선택 의사결정 트리

```
Join 조건이 등호(=)인가?
│
├─ YES (equi-join)
│   │
│   ├─ 한쪽이 충분히 작은가? (< 10MB 기본)
│   │   │
│   │   ├─ YES → ⭐ Broadcast Hash Join (가장 빠름)
│   │   │
│   │   └─ NO → 양쪽 다 큼
│   │       │
│   │       ├─ 한쪽 파티션이 메모리에 들어감? → Shuffle Hash Join
│   │       │
│   │       └─ 둘 다 커서 안전하게 → ⭐ Sort Merge Join (기본)
│   │
│   └─ 이미 같은 키로 파티셔닝/정렬됨? → Sort Merge Join (셔플 생략!)
│
└─ NO (non-equi join: >, <, BETWEEN 등)
    │
    ├─ 한쪽이 작은가? → Broadcast Nested Loop Join
    │
    └─ 양쪽 다 큼 → Cartesian Product (⚠️ 매우 느림)
```

### Join 최적화 실전 팁

#### 팁 1: Broadcast 임계값 조정

```python
# 기본 10MB → 실무에서는 50~200MB로 올리는 경우 많음
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "104857600")  # 100MB

# broadcast 완전 비활성화 (항상 SMJ 사용)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
```

#### 팁 2: 조인 전 필터링 (Predicate Pushdown)

```python
# 나쁨: 전체 데이터 조인 후 필터
result = big_a.join(big_b, "id").filter(col("date") == "2024-01-01")

# 좋음: 필터 후 조인 (셔플 데이터 감소)
# Catalyst가 자동으로 해주지만, 명시적이면 더 확실
filtered_a = big_a.filter(col("date") == "2024-01-01")
result = filtered_a.join(big_b, "id")
```

#### 팁 3: 조인 키 데이터 스큐 해결

```python
# 문제: user_id = "anonymous"가 전체의 30%
# → 하나의 파티션에 30% 데이터 몰림 → 병목

# 해결 1: Salting
from pyspark.sql.functions import lit, rand, concat

df_salted = df.withColumn("salt", (rand() * 10).cast("int"))
df_salted = df_salted.withColumn("salted_key", 
    concat(col("user_id"), lit("_"), col("salt")))

lookup_exploded = lookup.crossJoin(
    spark.range(10).withColumnRenamed("id", "salt")
).withColumn("salted_key",
    concat(col("user_id"), lit("_"), col("salt")))

result = df_salted.join(lookup_exploded, "salted_key")

# 해결 2: AQE 스큐 조인 (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")
```

#### 팁 4: Bucketing (사전 파티셔닝)

```python
# 테이블을 미리 조인 키로 버킷팅해서 저장
# → 이후 조인 시 셔플 완전 생략!

# 저장 시 버킷팅
trips.write \
    .bucketBy(16, "zone_id") \
    .sortBy("zone_id") \
    .saveAsTable("trips_bucketed")

zones.write \
    .bucketBy(16, "zone_id") \
    .sortBy("zone_id") \
    .saveAsTable("zones_bucketed")

# 조인 시 — 셔플 없음! (같은 버킷끼리 로컬 조인)
trips_b = spark.table("trips_bucketed")
zones_b = spark.table("zones_bucketed")
result = trips_b.join(zones_b, "zone_id")  # No Shuffle!
```

```
Bucketing 조건:
  - 양쪽 테이블의 버킷 수가 같아야 함 (또는 배수)
  - 같은 컬럼으로 버킷팅
  - saveAsTable로 저장 (write.parquet은 불가)
  - 반복적으로 같은 키로 조인하는 경우 효과적
```

#### 팁 5: 실행 계획으로 Join 전략 확인

```python
result = trips.join(zones, "zone_id")
result.explain()

# 출력에서 확인:
# == Physical Plan ==
# *(2) BroadcastHashJoin [zone_id], [zone_id], Inner, BuildRight
#    ├─ *(2) ... (trips)
#    └─ BroadcastExchange HashedRelationBroadcastMode(...)
#       └─ *(1) ... (zones)
#
# BroadcastHashJoin  → Broadcast 사용됨 ✅
# SortMergeJoin      → Sort Merge 사용됨
# ShuffledHashJoin   → Shuffle Hash 사용됨
```

### Join 전략별 성능 비교 요약

```
                속도        메모리      셔플       안정성
BHJ            ★★★★★     ★★☆☆☆    없음       작은 테이블만
SHJ            ★★★★☆     ★★★☆☆    있음       중간
SMJ            ★★★☆☆     ★★★★★    있음       ★★★★★
BNLJ           ★★☆☆☆     ★★☆☆☆    없음       non-equi만
Cartesian      ★☆☆☆☆     ★☆☆☆☆    있음       피할 것

현업 비율 (체감):
  BHJ  40% — dimension 테이블 조인에 많이 사용
  SMJ  50% — fact 테이블 간 조인
  SHJ   5% — 특수한 경우
  기타   5%
```

---

## 8. Partitioning 전략

### Partitioning이란?

```
Partition = 데이터를 논리적으로 나눈 조각

Spark에서 모든 데이터는 파티션 단위로 처리됨:
  - 1 파티션 = 1 Task = 1 CPU 코어가 처리
  - 파티션 수 = 병렬 처리 단위
```

```
데이터 (1000만 행)
│
├── Partition 0: 행 1 ~ 250만      → Task 0 (Core 0)
├── Partition 1: 행 250만 ~ 500만   → Task 1 (Core 1)
├── Partition 2: 행 500만 ~ 750만   → Task 2 (Core 2)
└── Partition 3: 행 750만 ~ 1000만  → Task 3 (Core 3)

→ 4개 코어로 4개 파티션을 동시에 처리 = 4배 빠름
```

### 파티션 수가 중요한 이유

```
파티션이 너무 적으면:
  ┌──────────────────────────────────────────────┐
  │ Partition 0: 1000만 행  → Core 0 (혼자 처리)  │
  │ Core 1: 놀고 있음                              │
  │ Core 2: 놀고 있음                              │
  │ Core 3: 놀고 있음                              │
  └──────────────────────────────────────────────┘
  → 병렬성 낭비 + 단일 파티션 메모리 부족 → OOM

파티션이 너무 많으면:
  ┌──────────────────────────────────────────────┐
  │ Partition 0: 10행  → Task 오버헤드 > 처리 시간  │
  │ Partition 1: 8행   → Task 오버헤드 > 처리 시간  │
  │ ... (10000개 파티션)                            │
  │ → 스케줄링 오버헤드 + 작은 파일 다수 생성        │
  └──────────────────────────────────────────────┘
  → 스케줄링 비용 + small file problem

적절한 파티션 크기:
  128MB ~ 256MB per partition (경험적 최적값)
```

### repartition() vs coalesce()

| 항목 | `repartition(n)` | `coalesce(n)` |
|------|-------------------|---------------|
| **방향** | 늘리기/줄이기 모두 가능 | **줄이기만** 가능 |
| **셔플** | **항상 발생** (Full Shuffle) | 셔플 **없음** (파티션 합치기만) |
| **데이터 분포** | **균등 분배** (Hash/Round-Robin) | 불균등할 수 있음 |
| **비용** | 높음 (네트워크 + 디스크 I/O) | 낮음 (로컬 합치기) |
| **사용 시점** | 데이터 스큐 해소, 파티션 증가 | 파티션 수 줄일 때 |

### repartition() 상세

```python
# ═══ 파티션 수 지정 ═══
df.repartition(8)  # 8개 파티션으로 재분배 (Round-Robin)

# ═══ 컬럼 기준 파티셔닝 ═══
df.repartition("date")       # date 값 기준 Hash 파티셔닝
df.repartition(8, "date")    # 8개 파티션, date 기준 Hash

# ═══ 여러 컬럼 기준 ═══
df.repartition("year", "month")  # year+month 조합 기준
```

```
repartition(4) 동작 과정:

BEFORE (3 partitions, 불균등):
  Part 0: ████████████  (1200행)
  Part 1: ██             (200행)
  Part 2: ████████████████  (1600행)
       Total: 3000행

      ↓ Full Shuffle (모든 데이터 재분배)

AFTER (4 partitions, 균등):
  Part 0: ███████  (750행)
  Part 1: ███████  (750행)
  Part 2: ███████  (750행)
  Part 3: ███████  (750행)
       Total: 3000행
```

```
repartition("date") 동작:

BEFORE:
  Part 0: [Jan, Mar, Jan, Feb]
  Part 1: [Feb, Jan, Mar, Mar]

      ↓ Hash Shuffle (같은 date → 같은 파티션)

AFTER:
  Part 0: [Jan, Jan, Jan]     ← hash("Jan") % N = 0
  Part 1: [Feb, Feb]           ← hash("Feb") % N = 1
  Part 2: [Mar, Mar, Mar]     ← hash("Mar") % N = 2

→ 같은 date가 같은 파티션에 모임
→ 이후 date 기준 groupBy/join 시 셔플 생략 가능!
```

### coalesce() 상세

```python
# 파티션 수 줄이기 (셔플 없음)
df.coalesce(2)  # 현재 파티션들을 합쳐서 2개로
```

```
coalesce(2) 동작 과정:

BEFORE (4 partitions):
  Part 0: ████  (Node A)
  Part 1: ████  (Node A)
  Part 2: ████  (Node B)
  Part 3: ████  (Node B)

      ↓ 셔플 없이 인접 파티션 합치기

AFTER (2 partitions):
  Part 0: ████████  (Node A — Part 0 + Part 1 합침)
  Part 1: ████████  (Node B — Part 2 + Part 3 합침)

→ 네트워크 전송 없음!
→ 하지만 데이터가 불균등할 수 있음
```

```
⚠️ coalesce로 늘리면?

df.coalesce(100)  # 현재 4개 → 100개?
→ 실제로는 4개 그대로! (줄이기만 가능, 늘리기 불가)
→ 경고/에러 없이 무시됨 → 버그의 원인
```

### 언제 뭘 쓸까?

```
의사결정 트리:

  파티션 수를 바꾸고 싶다
  │
  ├─ 줄이고 싶다 (예: 200 → 4)
  │   │
  │   ├─ 균등 분배 필요? 
  │   │   ├─ YES → repartition(4)  (셔플 비용 감수)
  │   │   └─ NO  → ⭐ coalesce(4)  (셔플 없이 빠름)
  │   │
  │   └─ 파일 저장 직전? → ⭐ coalesce(4)  (작은 파일 방지)
  │
  ├─ 늘리고 싶다 (예: 2 → 24)
  │   └─ → repartition(24)  (coalesce로는 불가능)
  │
  └─ 특정 컬럼 기준으로 나누고 싶다
      └─ → repartition("column")  (Hash 파티셔닝)
```

### 실전 패턴

#### 패턴 1: CSV 읽기 → Parquet 저장

```python
# CSV는 보통 파티션 수가 파일 수에 의존
df = spark.read.csv("data/*.csv")  # 파일 100개 → 100 파티션

# Parquet 저장 시 적절한 파티션 수로 조정
df.repartition(4).write.parquet("output/")  # 4개 Parquet 파일 생성

# 또는 셔플 없이 (불균등 허용)
df.coalesce(4).write.parquet("output/")
```

#### 패턴 2: 날짜 기준 파티셔닝 (Hive 스타일)

```python
# 날짜별 디렉토리 구조로 저장
df.repartition("date") \
  .write \
  .partitionBy("date") \
  .parquet("output/")

# 결과 디렉토리 구조:
# output/
#   date=2024-01-01/
#     part-00000.parquet
#   date=2024-01-02/
#     part-00000.parquet
#   ...

# 이후 특정 날짜만 읽을 때 → Partition Pruning
df = spark.read.parquet("output/").filter(col("date") == "2024-01-01")
# → date=2024-01-01/ 디렉토리만 읽음! (나머지는 스킵)
```

#### 패턴 3: GroupBy/Join 전 최적화

```python
# groupBy("zone_id") 전에 미리 zone_id로 파티셔닝
# → groupBy 시 셔플 양 감소 (이미 같은 키가 같은 파티션에)

df = df.repartition("zone_id")
result = df.groupBy("zone_id").agg(F.sum("amount"))
```

#### 패턴 4: 파일 저장 시 Small File 방지

```python
# 문제: groupBy 후 기본 200개 파티션 → 200개 작은 파일
result = df.groupBy("dept").count()  # spark.sql.shuffle.partitions = 200
result.write.parquet("output/")  # 200개 파일 (대부분 수 KB)

# 해결: 저장 전 coalesce
result.coalesce(1).write.parquet("output/")  # 1개 파일

# 또는 AQE 자동 합침 (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64MB")
```

### repartition() vs partitionBy()

```
혼동 주의: 이름이 비슷하지만 완전히 다른 것!

  repartition():
    DataFrame의 인메모리 파티션을 재분배
    → 처리 중 병렬성 조정
    
  partitionBy():
    write 시 디렉토리 구조를 컬럼 값 기준으로 분리
    → 저장된 파일의 물리적 디렉토리 구조

  조합 사용:
    df.repartition("date")           # 메모리에서 date 기준으로 분배
      .write                         
      .partitionBy("date")           # 디스크에서 date별 디렉토리
      .parquet("output/")            
    
    → repartition("date")가 없으면 각 date 디렉토리에 여러 파일 생성
    → repartition("date")가 있으면 각 date 디렉토리에 1개 파일
```

### 적절한 파티션 수 계산

```
공식 1: 데이터 크기 기반
  ─────────────────────────────
  파티션 수 = 전체 데이터 크기 / 목표 파티션 크기
  
  예: 10GB 데이터, 목표 128MB/파티션
      → 10,000MB / 128MB ≈ 78 파티션

공식 2: 코어 수 기반
  ─────────────────────────────
  파티션 수 = 전체 코어 수 × 2~4
  
  예: 10노드 × 8코어 = 80코어
      → 80 × 3 = 240 파티션

공식 3: 경험적 가이드
  ─────────────────────────────
  데이터 크기          권장 파티션 수
  < 1GB               2 ~ 8
  1GB ~ 10GB          8 ~ 100
  10GB ~ 100GB        100 ~ 1000
  100GB ~ 1TB         1000 ~ 10000
  > 1TB               10000+

확인 방법:
  df.rdd.getNumPartitions()  # 현재 파티션 수
  df.rdd.glom().map(len).collect()  # 파티션별 행 수
```

### Shuffle Partition 수 조정

```python
# groupBy, join 등 셔플 연산 후 파티션 수 (기본: 200)
# 로컬 환경이나 작은 데이터에서 200은 과도함

# 소규모 데이터
spark.conf.set("spark.sql.shuffle.partitions", "8")

# 대규모 클러스터
spark.conf.set("spark.sql.shuffle.partitions", "2000")

# AQE 사용 시 자동 조절됨 (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
# → 200으로 설정해도, 실제로는 데이터 양에 맞게 자동 합침
```

### 정리: Partitioning 체크리스트

```
✅ 읽기 후 파티션 수 확인           → df.rdd.getNumPartitions()
✅ 1 파티션 = 128~256MB 목표        → 너무 크면 OOM, 너무 작으면 오버헤드
✅ 파일 저장 전 파티션 수 조정        → coalesce() 또는 repartition()
✅ 반복 조인 키 → bucketing 고려     → 셔플 완전 생략
✅ 날짜/카테고리 기준 → partitionBy()  → 읽기 시 Partition Pruning
✅ 셔플 파티션 수 확인               → spark.sql.shuffle.partitions
✅ AQE 활성화 (Spark 3.0+)          → 자동 파티션 합침
✅ 데이터 스큐 확인                  → 파티션별 크기 편차
```

---

## 9. spark-submit

### spark-submit이란?

```
Spark 애플리케이션을 클러스터에 제출(submit)하여 실행하는 표준 배포 도구

  노트북(대화형)  →  개발/테스트
  spark-submit   →  운영/배치 실행  ⭐
```

### 왜 spark-submit을 쓰는가?

#### ① 환경과 코드의 분리

```python
# ❌ 나쁜 예: 코드에 하드코딩
spark = SparkSession.builder \
    .master("local[*]") \          # ← 로컬에서만 동작
    .config("spark.executor.memory", "4g") \  # ← 환경마다 다름
    .getOrCreate()

# ✅ 좋은 예: 코드에는 설정 없이
spark = SparkSession.builder \
    .appName("my-etl-job") \
    .getOrCreate()
# master, memory 등은 spark-submit에서 외부 주입
```

```bash
# 로컬 테스트
spark-submit --master local[*] my_job.py

# Standalone 클러스터
spark-submit --master spark://master:7077 \
  --executor-memory 4g --executor-cores 2 \
  my_job.py --input data/green/ --output data/report/

# YARN 클러스터
spark-submit --master yarn --deploy-mode cluster \
  --num-executors 50 --executor-memory 8g \
  my_job.py --input gs://bucket/green/ --output gs://bucket/report/

# Kubernetes
spark-submit --master k8s://https://k8s-master:443 \
  --deploy-mode cluster \
  my_job.py
```

→ **같은 코드**가 local, Standalone, YARN, K8s 모두에서 동작

#### ② 리소스 제어

```bash
spark-submit \
  --executor-memory 8G \     # Executor당 메모리
  --executor-cores 4 \       # Executor당 CPU 코어
  --num-executors 10 \       # Executor 개수
  --driver-memory 2G \       # Driver 메모리
  app.py
```

→ 병렬도와 리소스를 실행 시점에 자유롭게 조절

#### ③ 외부 라이브러리 추가

```bash
# Maven 패키지 추가 (예: Iceberg)
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 \
  app.py

# 로컬 JAR 추가
spark-submit \
  --jars /path/to/gcs-connector.jar,/path/to/bigquery-connector.jar \
  app.py

# Python 패키지 추가
spark-submit \
  --py-files utils.zip,ml_model.py \
  app.py
```

#### ④ Client 모드 vs Cluster 모드

```
Client 모드 (--deploy-mode client, 기본값):
  Driver가 제출한 머신에서 실행
  → 로그를 바로 볼 수 있음
  → 개발/디버깅에 적합
  
  [내 노트북] ← Driver 여기서 실행
      │
      ▼
  [Cluster]
  ├─ Executor 1
  ├─ Executor 2
  └─ Executor 3

Cluster 모드 (--deploy-mode cluster):
  Driver가 클러스터 내부에서 실행
  → 제출한 머신과 독립적 (제출 후 끊어도 OK)
  → 운영/프로덕션에 적합
  
  [내 서버] → spark-submit 후 연결 끊어도 OK
      │
      ▼
  [Cluster]
  ├─ Driver (클러스터 노드에서 실행)
  ├─ Executor 1
  ├─ Executor 2
  └─ Executor 3
```

#### ⑤ 스케줄러 연동

```python
# Airflow에서 spark-submit 호출
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

etl_task = SparkSubmitOperator(
    task_id="daily_etl",
    application="/opt/spark/jobs/daily_etl.py",
    conn_id="spark_default",
    executor_memory="8g",
    num_executors=10,
    application_args=["--date", "{{ ds }}", "--input", "s3://bucket/raw/"],
)

# Dataproc (GCP)에서 제출
# gcloud dataproc jobs submit pyspark \
#   --cluster=my-cluster \
#   --region=us-central1 \
#   gs://bucket/jobs/daily_etl.py \
#   -- --date 2024-01-01 --input gs://bucket/raw/
```

#### ⑥ CLI 파라미터화 (argparse)

```python
# daily_etl.py — 월/연도별로 재실행 가능
import argparse
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--date", required=True)
args = parser.parse_args()

spark = SparkSession.builder.appName("daily-etl").getOrCreate()

df = spark.read.parquet(args.input)
df_filtered = df.filter(df.date == args.date)
df_filtered.write.mode("overwrite").parquet(args.output)
```

```bash
# 2024년 1월 실행
spark-submit --master yarn daily_etl.py \
  --input s3://bucket/raw/ --output s3://bucket/silver/ --date 2024-01-01

# 2024년 2월 실행 (같은 코드)
spark-submit --master yarn daily_etl.py \
  --input s3://bucket/raw/ --output s3://bucket/silver/ --date 2024-02-01
```

### 노트북에서 spark-submit으로 전환

```bash
# 1단계: 노트북을 Python 스크립트로 변환
jupyter nbconvert --to script 06_spark_sql.ipynb

# 2단계: 스크립트 정리
#   - display(), show() 등 대화형 코드 제거
#   - 하드코딩된 경로를 argparse로 교체
#   - SparkSession에서 master 등 제거

# 3단계: spark-submit으로 실행
spark-submit --master spark://master:7077 \
  --executor-memory 4g \
  06_spark_sql.py --input data/pq/ --output data/report/
```

### spark-submit vs 대화형 비교

```
                  spark-submit          spark-shell / notebook
  ─────────────────────────────────────────────────────────────
  목적            배치 실행               개발/테스트
  자동화          매우 적합 (스케줄러 연동)  제한적
  운영 안정성     높음 (cluster 모드)      낮음 (세션 의존)
  재현성          좋음 (고정된 코드+인자)   나쁨 (셀 실행 순서 의존)
  리소스 제어     유연함                  제한적
  디버깅          로그 기반               대화형 (더 편함)
  모니터링        YARN/K8s/Spark UI      Spark UI
```

### spark-submit 주요 옵션 치트시트

```bash
spark-submit \
  --master <master-url> \          # local[*], spark://host:7077, yarn, k8s://...
  --deploy-mode <mode> \           # client (기본) | cluster
  --executor-memory <mem> \        # 예: 4g, 8g
  --executor-cores <cores> \       # 예: 2, 4
  --num-executors <n> \            # 예: 10, 50
  --driver-memory <mem> \          # 예: 2g
  --driver-cores <cores> \         # 예: 2
  --conf <key>=<value> \           # 임의 Spark 설정
  --packages <maven-coords> \      # Maven 패키지 (콤마 구분)
  --jars <jar-paths> \             # 로컬/원격 JAR
  --py-files <py-files> \          # Python 파일/zip
  --files <files> \                # 작업 디렉토리에 배포할 파일
  <app.py | app.jar> \             # 메인 애플리케이션
  [app-arguments]                  # 애플리케이션 인자
```

---

## 10. HDFS vs S3/GCS — 스토리지 전략

### 핵심: Storage-Compute 분리

```
전통적 구조 (HDFS):
  ┌──────────────────────────────────────────┐
  │          Hadoop Cluster                  │
  │                                          │
  │  Node 1: [CPU] + [Disk] + [HDFS Block]  │
  │  Node 2: [CPU] + [Disk] + [HDFS Block]  │
  │  Node 3: [CPU] + [Disk] + [HDFS Block]  │
  │                                          │
  │  Storage + Compute 결합                  │
  │  → 노드 추가 = 저장 + CPU 동시 증가      │
  │  → 항상 클러스터 유지 필요                │
  └──────────────────────────────────────────┘

클라우드 구조 (S3/GCS + Spark):
  ┌─────────────────────┐    ┌─────────────────────┐
  │   Object Storage    │    │   Spark Cluster     │
  │   (S3 / GCS)        │    │   (EMR / Dataproc)  │
  │                     │    │                     │
  │  데이터 항상 존재    │◄──►│  필요할 때만 생성    │
  │  무제한 확장         │    │  작업 끝나면 삭제    │
  │  저렴한 저장 비용    │    │  시간당 과금          │
  │                     │    │                     │
  │  Storage             │    │  Compute            │
  └─────────────────────┘    └─────────────────────┘
  
  → Storage와 Compute 완전 분리!
  → 각각 독립적으로 확장 가능
```

### 상세 비교

| 항목 | HDFS | S3/GCS |
|------|------|--------|
| **저장 위치** | 클러스터 내부 디스크 | 외부 오브젝트 스토리지 |
| **확장성** | 노드 추가 필요 | 사실상 무제한 |
| **비용 구조** | 항상 서버 유지 (고정) | 저장량 + 사용량 기반 (변동) |
| **Compute 분리** | ❌ 결합 | ✅ 완전 분리 |
| **운영 부담** | NameNode 관리, 디스크 장애, 리밸런싱 | 거의 없음 (관리형) |
| **가용성** | 직접 구성 (HA NameNode) | 기본 99.99% |
| **레이턴시** | 낮음 (로컬 디스크) | 높음 (네트워크 I/O) |
| **멀티 엔진** | Spark/Hive 중심 | Spark, Presto, Flink, Snowflake 등 |
| **테이블 포맷** | Hive | **Iceberg, Delta Lake, Hudi** |

### S3/GCS의 5가지 이점

#### ① 탄력적 확장 (Elasticity)

```
HDFS:
  클러스터 항상 가동 → 사용 안 해도 비용 발생
  ┌────────────────────────────────────────┐
  │  24시간 × 365일 서버 유지               │
  │  야간/주말에도 비용 발생                 │
  └────────────────────────────────────────┘

S3/GCS + Spark:
  데이터는 항상 저장 (저렴)
  Spark 클러스터는 필요할 때만 생성
  ┌────────────────────────────────────────┐
  │  09:00  ETL 시작 → 클러스터 생성        │
  │  09:30  ETL 완료 → 클러스터 삭제        │
  │  나머지 23.5시간 → 비용 $0              │
  └────────────────────────────────────────┘
  → 비용 90%+ 절감 가능
```

#### ② 무제한 확장성

```
HDFS: 물리 디스크 한계 → 노드 추가 → 리밸런싱 필요
      10TB → 100TB: 노드 구매 + 설치 + 데이터 재분배

S3:   10TB → 100TB → 1PB: 그냥 파일 업로드
      인프라 변경 없음
```

#### ③ 운영 단순화

```
HDFS 운영 체크리스트:                S3/GCS:
  □ NameNode HA 구성               ✅ 자동
  □ DataNode 디스크 모니터링         ✅ 자동
  □ 리밸런싱 스케줄링                ✅ 자동
  □ 복제 계수 관리                   ✅ 자동
  □ 스냅샷/백업 설정                 ✅ 버전관리 내장
  □ 네트워크 대역폭 관리             ✅ 자동
```

#### ④ 멀티 엔진 접근

```
S3/GCS 위의 데이터에 여러 엔진이 동시에 접근 가능:

  ┌─────────────────────────────┐
  │        S3 / GCS             │
  │  (Delta Lake / Iceberg)     │
  └──────────┬──────────────────┘
             │
    ┌────────┼────────┬──────────┬──────────┐
    ▼        ▼        ▼          ▼          ▼
  Spark   Presto/   Flink    Snowflake  BigQuery
          Trino
  
  ETL      Ad-hoc   스트리밍    BI         분석
  배치     쿼리     처리       대시보드    쿼리

HDFS에서는 주로 Spark/Hive만 접근 → 사일로 발생
```

#### ⑤ 모던 테이블 포맷과 궁합

```
S3/GCS + Iceberg/Delta Lake:
  ✅ ACID 트랜잭션
  ✅ Time Travel (과거 버전 조회)
  ✅ Schema Evolution
  ✅ Partition Evolution
  ✅ 여러 엔진에서 동시 읽기/쓰기
  
  → "Data Lake를 Data Warehouse처럼" = Lakehouse
```

### S3/GCS의 단점

```
① 네트워크 레이턴시
  HDFS: 로컬 디스크 → ms 단위
  S3:   네트워크 → 수십~수백 ms
  → 대응: Parquet/ORC (컬럼 pruning), 큰 파일 사용

② Small File Problem (더 심각)
  HDFS: 작은 파일 → NameNode 부하
  S3:   작은 파일 → API 호출 폭증 + 높은 레이턴시
  → 대응: repartition/coalesce로 적절한 파일 크기 유지 (128MB~256MB)

③ 일관성
  과거 S3: eventual consistency (읽기 지연 가능)
  현재 S3: strong consistency (2020년 12월~) → 해결됨
```

### 현재 트렌드

```
모던 데이터 레이크 아키텍처:

  ┌─────────────────────────────────────────────────┐
  │                                                 │
  │    S3/GCS  +  Iceberg/Delta Lake  +  Spark     │
  │    (저장)     (테이블 포맷)           (처리)     │
  │                                                 │
  │    + Dataproc/EMR (관리형 Spark)                │
  │    + spark-submit + Kubernetes                  │
  │    + Airflow/Dagster (오케스트레이션)             │
  │                                                 │
  └─────────────────────────────────────────────────┘
  
  "Lakehouse Architecture"
```

```
HDFS가 아직 유리한 경우:
  - 온프레미스 환경 (규제/보안 이유로 클라우드 불가)
  - 초저지연 반복 접근이 필요한 워크로드
  - 기존 Hadoop 인프라가 이미 구축된 경우
  - 네트워크 비용이 클라우드 저장 비용보다 비싼 경우
```

---

## 11. Spark 튜닝 포인트

### 왜 튜닝이 필요한가?

```
기본 설정으로도 Spark는 동작하지만:
  - 기본 shuffle partition = 200 → 소규모 데이터에서 과도
  - 기본 executor memory = 1g → 대규모 데이터에서 부족
  - 기본 broadcast threshold = 10MB → 놓치는 최적화 많음

→ 데이터 규모와 클러스터에 맞는 튜닝이 성능을 좌우
```

### 튜닝 영역 1: Executor 리소스 설계

#### Executor 수 vs 코어 수 vs 메모리

```
잘못된 설정 예시들:

  ❌ Fat Executor (코어 너무 많음)
    --executor-cores 16 --executor-memory 64g --num-executors 2
    → GC 부담 과중, HDFS throughput 병목 (동시 쓰기 제한)
    → 코어당 메모리는 충분하지만 병렬 I/O가 부족
  
  ❌ Tiny Executor (너무 작음)
    --executor-cores 1 --executor-memory 1g --num-executors 100
    → 브로드캐스트 변수 100번 복사 → 메모리 낭비
    → 멀티스레드 이점 없음
  
  ✅ 최적 (경험적 가이드라인)
    --executor-cores 4~5
    --executor-memory 16~20g
    --num-executors (총 코어에 맞게)
```

```
Executor 사이징 공식:

  클러스터: 10 노드 × 16코어 × 64GB
  
  Step 1: OS/Hadoop 데몬용 1코어 + 1GB 예약
    → 가용: 15코어 × 63GB per node
  
  Step 2: executor-cores = 5 (경험적 최적값)
    → 노드당 executor 수 = 15 / 5 = 3개
  
  Step 3: AM(ApplicationMaster)용 1 executor 제외
    → 총 executor = (10 × 3) - 1 = 29개
  
  Step 4: executor-memory
    → 노드 메모리 / 노드당 executor = 63GB / 3 = 21GB
    → overhead (10%) 제외: 21 × 0.9 ≈ 18~19GB
    → --executor-memory 18g
  
  최종:
    spark-submit \
      --executor-cores 5 \
      --executor-memory 18g \
      --num-executors 29
```

### 튜닝 영역 2: Shuffle 최적화

```python
# ① Shuffle Partition 수 조정 (가장 흔한 튜닝)
# 기본값 200 → 데이터 규모에 맞게

# 소규모 (< 1GB)
spark.conf.set("spark.sql.shuffle.partitions", "8")

# 중규모 (1~10GB)
spark.conf.set("spark.sql.shuffle.partitions", "100")

# 대규모 (10~100GB)
spark.conf.set("spark.sql.shuffle.partitions", "500")

# 초대규모 (> 100GB)
spark.conf.set("spark.sql.shuffle.partitions", "2000")

# ② AQE로 자동 조절 (Spark 3.0+ 권장)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64MB")
# → 200으로 설정해도 실제로는 데이터에 맞게 자동 합침
```

```
Shuffle 줄이는 전략:

  ✅ Broadcast Join 활용 (작은 테이블 조인 시 셔플 제거)
  ✅ 조인 전 필터링 (셔플 데이터 양 감소)
  ✅ Bucketing (반복 조인 시 셔플 완전 제거)
  ✅ repartition(key) 후 groupBy/join (셔플 양 감소)
  ✅ AQE 활성화 (런타임 최적화)
```

### 튜닝 영역 3: 메모리 관리

```
Executor 메모리 구조:

  ┌─────────────────────────────────────┐
  │         executor-memory (예: 18g)   │
  │                                     │
  │  ┌──────────────────────────────┐   │
  │  │  Execution Memory (60%)     │   │  ← Shuffle, Join, Sort
  │  │  + Storage Memory (40%)     │   │  ← Cache, Broadcast
  │  │                              │   │
  │  │  (Unified Memory, 동적 공유) │   │
  │  └──────────────────────────────┘   │
  │                                     │
  │  ┌──────────────────────────────┐   │
  │  │  User Memory                 │   │  ← UDF 변수 등
  │  └──────────────────────────────┘   │
  │                                     │
  │  ┌──────────────────────────────┐   │
  │  │  Reserved Memory (300MB)     │   │  ← Spark 내부
  │  └──────────────────────────────┘   │
  │                                     │
  │  + Overhead (10%, executor 외부)     │  ← Off-heap, 컨테이너
  └─────────────────────────────────────┘
```

```python
# 캐시 활용 (반복 사용하는 DataFrame)
df = spark.read.parquet("data/trips/")
df.cache()       # 메모리에 캐시 (MEMORY_AND_DISK)
df.count()       # 캐시 트리거 (Action 필요)

# 사용 후 해제
df.unpersist()

# Storage Level 지정
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK_SER)  # 직렬화 + 디스크 fallback
```

```
캐시 사용 가이드:

  ✅ 같은 DF를 2번 이상 사용할 때 → cache()
  ✅ 반복 조인의 룩업 테이블 → cache()
  ✅ ML 학습 데이터 (반복 접근) → persist(MEMORY_AND_DISK)

  ❌ 1번만 사용하는 DF → cache 불필요 (오히려 메모리 낭비)
  ❌ 전체 데이터를 cache → OOM 위험
```

### 튜닝 영역 4: 데이터 포맷 & I/O

```
파일 포맷별 성능:

  CSV:     느림  | 스키마 없음  | 압축 비효율  | 디버깅 편함
  JSON:    느림  | 스키마 유연  | 압축 비효율  | API 호환
  Parquet: 빠름  | 스키마 내장  | 압축 효율적  | 표준 ⭐
  ORC:     빠름  | 스키마 내장  | 압축 효율적  | Hive 친화
  Avro:    중간  | 스키마 내장  | 행 기반      | 스트리밍

읽기 성능 (같은 데이터 기준):
  CSV     ██████████████████████████████  100%
  JSON    ████████████████████████████    93%
  Avro    ██████████████                  47%
  ORC     ██████████                      33%
  Parquet █████████                       30%  ⭐ 가장 빠름
```

```python
# ① 항상 Parquet 사용 (특별한 이유 없으면)
df.write.parquet("output/")  # snappy 압축 기본

# ② 스키마 명시 (CSV 읽을 때 — 추론 비용 제거)
from pyspark.sql.types import *
schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("amount", DoubleType()),
])
df = spark.read.schema(schema).csv("data.csv")
# inferSchema=True는 한번 전체 파일을 스캔 → 느림

# ③ 파티션 바이 (읽기 시 Partition Pruning)
df.write.partitionBy("year", "month").parquet("output/")
# → 특정 연/월만 읽을 때 나머지 스킵

# ④ 적절한 파일 크기 유지 (128MB~256MB)
df.repartition(target_partitions).write.parquet("output/")
```

### 튜닝 영역 5: Spark UI 읽는 법

```
Spark UI (http://localhost:4040)에서 확인할 것:

  ┌─────────────────────────────────────────────────────────┐
  │  Jobs 탭                                                │
  │  → Job이 몇 개인가? (Action 하나 = Job 하나)             │
  │  → 어떤 Job이 오래 걸리나?                               │
  ├─────────────────────────────────────────────────────────┤
  │  Stages 탭                                              │
  │  → Stage가 몇 개인가? (Shuffle boundary에서 분할)        │
  │  → Shuffle Read/Write가 얼마인가?                       │
  │  → 특정 Stage에 시간이 몰리나? → 병목!                   │
  ├─────────────────────────────────────────────────────────┤
  │  Tasks 탭 (Stage 클릭 → Task 상세)                      │
  │  → Task 수 = Partition 수 적절한가?                     │
  │  → Task Duration 편차 큰가? → 데이터 스큐!              │
  │  → GC Time 높은가? → 메모리 부족!                       │
  │  → Shuffle Spill 높은가? → 파티션 크기 조정 필요!        │
  ├─────────────────────────────────────────────────────────┤
  │  SQL 탭                                                 │
  │  → 실행 계획(DAG) 시각화                                 │
  │  → Join 전략 확인 (BroadcastHashJoin vs SortMergeJoin)  │
  │  → Exchange 노드 = Shuffle 발생 지점                     │
  ├─────────────────────────────────────────────────────────┤
  │  Storage 탭                                             │
  │  → cache된 RDD/DataFrame 확인                           │
  │  → 메모리/디스크 사용량                                  │
  └─────────────────────────────────────────────────────────┘
```

### 튜닝 영역 6: 흔한 문제와 해결

#### 문제 1: OOM (Out of Memory)

```
증상: executor lost, java.lang.OutOfMemoryError

원인 → 해결:
  ① 파티션이 너무 큼
     → repartition()으로 파티션 수 증가
  
  ② collect()로 큰 데이터를 Driver로
     → take(), show(), write 사용
  
  ③ broadcast가 너무 큰 테이블
     → autoBroadcastJoinThreshold 낮추기 or -1
  
  ④ 메모리 자체가 부족
     → --executor-memory 증가
     → --spark.memory.fraction 조정 (기본 0.6)
```

#### 문제 2: 데이터 스큐 (Skew)

```
증상: 대부분 Task는 빠른데, 1~2개가 극도로 느림
      (Spark UI에서 Task Duration 편차 확인)

원인: 특정 키에 데이터가 몰림
  예: user_id = "unknown" → 전체의 30%

해결:
  ① AQE Skew Join (Spark 3.0+, 가장 쉬움)
     spark.conf.set("spark.sql.adaptive.enabled", "true")
     spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
  
  ② Salting (수동)
     큰 키에 랜덤 접미사 추가 → 여러 파티션으로 분산
  
  ③ 문제 키 분리 처리
     unknown = df.filter(col("user_id") == "unknown")
     normal = df.filter(col("user_id") != "unknown")
     # 각각 다른 전략으로 처리 후 union
```

#### 문제 3: Small File Problem

```
증상: 수천~수만 개의 작은 파일 생성
      → 이후 읽기 시 매우 느림 (파일당 Task 생성 오버헤드)

해결:
  ① 저장 전 coalesce(n)
     df.coalesce(4).write.parquet("output/")
  
  ② AQE 파티션 합침
     spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
  
  ③ maxRecordsPerFile 설정
     df.write.option("maxRecordsPerFile", 1000000).parquet("output/")
```

#### 문제 4: Shuffle Spill

```
증상: Spark UI에서 Shuffle Spill (Disk) 값이 큼
      → 메모리에 안 들어가서 디스크에 임시 저장 → 느림

해결:
  ① Shuffle Partition 수 증가 (파티션당 데이터 줄이기)
  ② executor-memory 증가
  ③ spark.memory.fraction 증가 (기본 0.6 → 0.7)
  ④ 데이터 필터링을 더 일찍 수행
```

### 튜닝 체크리스트

```
✅ 파일 포맷       → Parquet 사용 중인가?
✅ 스키마 명시      → inferSchema 대신 schema() 사용?
✅ 파티션 수        → df.rdd.getNumPartitions() 적절한가?
✅ 셔플 파티션      → spark.sql.shuffle.partitions 조정?
✅ AQE 활성화      → spark.sql.adaptive.enabled = true?
✅ Broadcast Join  → 작은 테이블 broadcast 되고 있나? (explain 확인)
✅ 캐시            → 반복 사용 DF 캐시 중인가?
✅ Executor 사이징 → 코어 4~5, 메모리 16~20g 기준?
✅ Spark UI 확인   → Shuffle Spill, Task Skew, GC Time?
✅ 파일 크기       → 128~256MB per file?
```

---

## 부록: Spark 설정 치트시트

```python
# 메모리 설정
spark.conf.set("spark.executor.memory", "4g")
spark.conf.set("spark.driver.memory", "2g")

# 셔플 파티션 수 (groupBy 등 셔플 후 파티션 수)
spark.conf.set("spark.sql.shuffle.partitions", "200")  # 기본 200

# AQE 활성화 (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Broadcast Join 임계값
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB

# 동적 파티션 pruning
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
```

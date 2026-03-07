# Spark Repartition 완전 가이드

## 1. 파티션이란?

Spark에서 **파티션(Partition)**은 데이터를 나누는 논리적 단위입니다.

```
전체 DataFrame (1200만 행)
├─ Partition 0  (50만 행)  →  Task 0  →  Core 1
├─ Partition 1  (50만 행)  →  Task 1  →  Core 2
├─ Partition 2  (50만 행)  →  Task 2  →  Core 3
│  ...
└─ Partition 23 (50만 행)  →  Task 23 →  Core N
```

- **파티션 수 = 병렬 태스크 수** → 코어가 많을수록 동시에 처리
- 각 파티션은 메모리에 독립적으로 로드됨
- `.write()` 시 **파티션 1개 = 출력 파일 1개**

## 2. repartition() vs coalesce()

### repartition(n)

```python
df = df.repartition(24)
```

- **Full Shuffle** 발생 — 전체 데이터를 네트워크를 통해 재분배
- 파티션 수를 **늘리거나 줄이기** 모두 가능
- 데이터가 **균등하게** 분배됨
- 비용이 높지만 결과가 균일

### coalesce(n)

```python
df = df.coalesce(4)
```

- 셔플 **없음** — 기존 파티션을 합치기만 함
- 파티션 수를 **줄이기만** 가능 (늘리기 불가)
- 데이터가 **불균등**할 수 있음
- 비용이 낮음

### 비교 표

| 항목 | `repartition(n)` | `coalesce(n)` |
|------|-------------------|---------------|
| 셔플 | O (full shuffle) | X |
| 파티션 증가 | 가능 | 불가 |
| 파티션 감소 | 가능 | 가능 |
| 데이터 균등 | 균등 | 불균등 가능 |
| 네트워크 비용 | 높음 | 낮음 |
| 주 용도 | 파티션 재구성 | 출력 파일 수 줄이기 |

## 3. repartition의 핵심 효과

### 3.1 병렬 처리 극대화

```
Before: CSV 1개 → 파티션 1개 → 코어 1개만 사용
After:  repartition(24) → 파티션 24개 → 코어 24개 동시 사용
```

### 3.2 데이터 스큐(Skew) 해소

```
Before (편향):
  Partition 0: 900만 행  ← 병목!
  Partition 1: 100만 행
  Partition 2: 200만 행

After repartition(24) (균등):
  Partition 0~23: 각 50만 행
```

### 3.3 출력 파일 크기 제어

```python
# 24개 Parquet 파일 생성 (각 ~30MB)
df.repartition(24).write.parquet('output/')

# 1개 파일로 합치기
df.coalesce(1).write.parquet('output/')
```

### 3.4 다운스트림 읽기 성능

```python
# 24개 파일 → 24개 태스크가 병렬로 읽기
df = spark.read.parquet('output/')
# local[*] (8코어) → 3라운드로 완료
```

## 4. 파티션 수 결정 기준

### 경험적 규칙

| 기준 | 권장 |
|------|------|
| 파티션당 크기 | **128MB ~ 256MB** |
| 파티션 수 | 클러스터 총 코어 수의 **2~4배** |
| 최소 | 코어 수 이상 |
| 최대 | 너무 많으면 스케줄링 오버헤드 |

### 계산 예시

```
데이터 크기: 10GB
목표 파티션 크기: 128MB

파티션 수 = 10GB / 128MB = ~80개

클러스터 코어: 20개
권장 = 20 × 3 = 60개

→ 60~80개 사이가 적절
```

### 현재 파티션 수 확인

```python
print(f'파티션 수: {df.rdd.getNumPartitions()}')
```

## 5. 컬럼 기반 repartition

특정 컬럼 값을 기준으로 파티셔닝할 수 있습니다.

```python
# PULocationID 기준으로 파티셔닝
df = df.repartition('PULocationID')

# 컬럼 기준 + 파티션 수 지정
df = df.repartition(24, 'PULocationID')
```

**효과**: 같은 `PULocationID`를 가진 행이 같은 파티션에 모임 → `groupBy`, `join` 시 셔플 감소

## 6. 실전 유즈케이스

### Case 1: CSV → Parquet 변환 (ETL)

> 단일 대용량 CSV를 적절한 크기의 Parquet 파일로 분할

```python
df = spark.read.option('header', 'true').csv('raw/huge_file.csv')
df = df.repartition(24)
df.write.parquet('processed/output/')
```

**왜?** CSV는 1개 파일 → 파티션 적음 → 저장 시 적절한 파일 수로 분할

### Case 2: 데이터 스큐 해소

> 특정 키에 데이터가 몰려 있어 `groupBy`/`join`이 느릴 때

```python
# Before: skewed (뉴욕 맨해튼 데이터만 90%)
df_skewed = spark.read.parquet('skewed_data/')

# After: 균등 재분배
df_balanced = df_skewed.repartition(100)
result = df_balanced.groupBy('zone').agg(F.count('*'))
```

### Case 3: 날짜/컬럼 기반 파티셔닝 저장

> Data Lake에 날짜별 디렉토리 구조로 저장

```python
df.repartition('pickup_date') \
  .write \
  .partitionBy('pickup_date') \
  .parquet('data_lake/trips/')

# 결과:
# data_lake/trips/pickup_date=2021-01-01/part-*.parquet
# data_lake/trips/pickup_date=2021-01-02/part-*.parquet
# ...
```

**왜?** `repartition('pickup_date')` → 같은 날짜가 같은 파티션 → 파일 수 최소화

### Case 4: Join 전 최적화

> 두 DataFrame을 같은 키로 파티셔닝하면 셔플 비용 절감

```python
df_trips = df_trips.repartition(24, 'zone_id')
df_zones = df_zones.repartition(24, 'zone_id')

# 이미 같은 키로 파티셔닝 → Shuffle-Free Join
result = df_trips.join(df_zones, 'zone_id')
```

### Case 5: 단일 파일 출력 (소규모 결과)

> 집계 결과를 1개 CSV/Parquet로 내보내기

```python
result = df.groupBy('department').agg(F.avg('salary'))

# coalesce(1)로 단일 파일 출력 (repartition보다 저렴)
result.coalesce(1).write.csv('reports/dept_salary/', header=True)
```

### Case 6: Streaming Micro-batch 크기 조절

> Structured Streaming에서 출력 파일 수 제어

```python
query = (
    df_stream
    .writeStream
    .trigger(processingTime='1 minute')
    .option('checkpointLocation', 'checkpoint/')
    .foreachBatch(lambda batch_df, _: 
        batch_df.repartition(4).write.mode('append').parquet('output/')
    )
    .start()
)
```

## 7. 안티패턴 (하지 말아야 할 것)

| 안티패턴 | 문제 | 해결 |
|----------|------|------|
| `repartition(1)` 대용량 | 단일 파티션에 모든 데이터 → OOM | `coalesce(1)` 또는 결과가 작을 때만 |
| `repartition(10000)` 소규모 | 빈 파티션 다수 → 빈 파일 생성 | 데이터 크기에 맞춰 조정 |
| 매 변환마다 repartition | 불필요한 셔플 반복 → 성능 저하 | 필요한 시점에만 1회 |
| `repartition` 후 바로 `coalesce` | 셔플 비용 낭비 | 처음부터 `coalesce`만 사용 |

## 8. 요약 플로우차트

```
파티션 수를 바꿔야 하나?
│
├─ 늘려야 함 → repartition(n)
│
├─ 줄여야 함
│   ├─ 균등해야 함 → repartition(n)
│   └─ 빠르게만 → coalesce(n)
│
├─ 특정 컬럼 기준 → repartition(n, 'col')
│
└─ 안 바꿔도 됨 → 그대로 두기
```

# 06-batch-spark — Docker 기반 Spark 환경

Docker Compose로 **Spark Master + Worker + Jupyter Notebook** 클러스터를 로컬에서 실행합니다.

## 사전 요구사항

| 항목 | 최소 버전 |
|------|----------|
| Docker Desktop | 4.x |
| Docker Compose | v2 (Docker Desktop 내장) |

## 빠른 시작

```bash
# 1. 이 디렉토리로 이동
cd 06-batch-spark

# 2. 컨테이너 시작 (백그라운드)
docker compose up -d

# 3. 상태 확인
docker compose ps
```

## 접속 URL

| 서비스 | URL |
|--------|-----|
| Spark Master UI | http://localhost:8080 |
| Spark Application UI | http://localhost:4040 |
| Jupyter Notebook | http://localhost:8888 |

> Jupyter 토큰은 `docker compose logs spark-notebook` 로 확인하세요.
> 
```bash
cd d:/mycoding/DataEngineerZoomcamp/data-engineering-zoomcamp-docker-workshop/06-batch-spark && docker compose logs spark-notebook 2>&1 | grep "token=" | tail -1
```

## 예제 실행

```bash
# spark-submit으로 예제 실행
docker exec spark-master spark-submit \
  --master local[*] \
  /opt/bitnami/spark/work/spark_example.py
```

## 종료

```bash
docker compose down
```

## 구조

```
06-batch-spark/
├─ docker-compose.yml      # Spark Master + Worker + Jupyter
├─ README.md               # 이 문서
├─ work/                   # 마운트 → Spark + Jupyter 공유 작업 디렉토리
│  └─ spark_example.py     # PySpark 예제 스크립트
└─ data/                   # 마운트 → 데이터 입출력
```

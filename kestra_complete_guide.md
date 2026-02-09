# Kestra 워크플로우 오케스트레이션 가이드

## Kestra의 장점

Kestra는 데이터 엔지니어링 및 워크플로우 오케스트레이션을 위한 오픈소스 도구로, 다음과 같은 주요 장점을 제공합니다:

### 1. **YAML 기반 선언적 구성**
- 코드 대신 YAML 파일로 워크플로우를 정의하여 가독성과 유지보수성이 뛰어납니다.
- **버전 관리 용이성**: YAML은 텍스트 기반 선언적 형식으로, Git에서 diff/merge가 직관적입니다. Python 코드 기반 도구(Airflow, Dagster)와 달리, 워크플로우 변경 시 코드 로직 대신 설정 변경만으로 diff가 명확히 보입니다. 예를 들어, 태스크 추가/삭제가 한 줄로 나타나 협업 충돌이 적습니다.
- 버전 관리(Git)와 협업이 용이합니다.

### 2. **광범위한 플러그인 생태계**
- 500개 이상의 내장 플러그인으로 데이터베이스, 클라우드 서비스(AWS, GCP, Azure), AI/ML, ETL 등을 지원합니다.
- 커스텀 플러그인 개발 가능.

### 3. **병렬 처리 및 확장성**
- 태스크를 병렬로 실행하여 대용량 데이터 처리 성능을 향상시킵니다.
- Kubernetes, Docker 등과 통합하여 수평 확장 가능.

### 4. **실시간 모니터링 및 디버깅**
- 웹 UI로 워크플로우 실행 상태, 로그, 메트릭을 실시간 확인.
- 오류 처리, 재시도, 알림 기능 내장.

### 5. **다양한 실행 환경 지원**
- 로컬, 서버, 클라우드에서 실행 가능.
- 이벤트 기반 트리거(예: 파일 변경, API 호출) 지원.

### 6. **커뮤니티 및 기업 지원**
- 활발한 오픈소스 커뮤니티.
- 엔터프라이즈 버전으로 고급 기능(보안, SLA 등) 제공.

Kestra는 Apache Airflow나 Prefect와 유사하지만, 더 가벼운 YAML 중심 설계로 빠른 프로토타이핑에 적합합니다.

## YAML 파일에서 병렬 처리 및 여러 태스크 동시 적용 방법

Kestra 워크플로우는 기본적으로 태스크를 순차적으로 실행하지만, 병렬 처리를 위해 다음과 같은 방법을 사용할 수 있습니다:

### 1. **Parallel 태스크 사용**
- `io.kestra.plugin.core.flow.Parallel` 타입으로 여러 서브태스크를 동시에 실행.
- 각 서브태스크는 독립적으로 실행되며, 모든 서브태스크가 완료될 때까지 기다립니다.

예시 YAML:
```yaml
tasks:
  - id: parallel_processing
    type: io.kestra.plugin.core.flow.Parallel
    tasks:
      - id: task1
        type: io.kestra.plugin.core.log.Log
        message: "Task 1 실행"
      - id: task2
        type: io.kestra.plugin.core.log.Log
        message: "Task 2 실행"
      - id: task3
        type: io.kestra.plugin.core.log.Log
        message: "Task 3 실행"
```

### 2. **EachParallel 태스크 사용**
- `io.kestra.plugin.core.flow.EachParallel`로 배열 데이터를 반복 처리하며 병렬 실행.
- 예: 여러 파일을 동시에 처리.

예시 YAML:
```yaml
tasks:
  - id: each_parallel_example
    type: io.kestra.plugin.core.flow.EachParallel
    value: ["file1.csv", "file2.csv", "file3.csv"]
    tasks:
      - id: process_file
        type: io.kestra.plugin.core.log.Log
        message: "처리 중: {{ taskrun.value }}"
```

### 3. **Switch 또는 Branching으로 조건부 병렬**
- `io.kestra.plugin.core.flow.Branch`로 조건에 따라 다른 태스크 그룹 실행.
- 각 브랜치 내에서 병렬 처리 가능.

### 4. **동시 적용 팁**
- 태스크 간 의존성을 최소화하여 병렬성을 높임.
- 리소스 제한(CPU, 메모리)을 고려해 클러스터에서 실행.
- `concurrency` 속성으로 동시 실행 제한 가능.

## Kestra 태스크 Type 정리

Kestra의 태스크는 `type` 필드로 지정되며, 주로 `io.kestra.plugin.`으로 시작합니다. 주요 카테고리별 정리:

### 1. **코어 태스크 (Core)**
- `io.kestra.plugin.core.log.Log`: 로그 출력.
- `io.kestra.plugin.core.flow.Parallel`: 병렬 실행.
- `io.kestra.plugin.core.flow.EachParallel`: 반복 병렬.
- `io.kestra.plugin.core.flow.If`: 조건문.
- `io.kestra.plugin.core.flow.Branch`: 브랜칭.

### 2. **데이터베이스 태스크 (Database)**
- `io.kestra.plugin.jdbc.postgres.Query`: PostgreSQL 쿼리 실행.
- `io.kestra.plugin.jdbc.mysql.Query`: MySQL 쿼리.
- `io.kestra.plugin.jdbc.snowflake.Query`: Snowflake 쿼리.

### 3. **파일 및 스토리지 태스크 (File/Storage)**
- `io.kestra.plugin.fs.http.Download`: HTTP 파일 다운로드.
- `io.kestra.plugin.fs.s3.Upload`: S3 업로드.
- `io.kestra.plugin.fs.gcs.Download`: GCS 다운로드.

### 4. **AI/ML 태스크 (AI)**
- `io.kestra.plugin.ai.rag.IngestDocument`: 문서 임베딩 생성 (RAG용).
- `io.kestra.plugin.ai.rag.ChatCompletion`: AI 채팅 with RAG.
- `io.kestra.plugin.ai.provider.GoogleGemini`: Gemini AI 제공자.

### 5. **클라우드 태스크 (Cloud)**
- `io.kestra.plugin.aws.s3.List`: AWS S3 목록.
- `io.kestra.plugin.gcp.bigquery.Query`: BigQuery 쿼리.
- `io.kestra.plugin.azure.blob.Upload`: Azure Blob 업로드.

### 6. **기타 유틸리티 태스크 (Utility)**
- `io.kestra.plugin.scripts.python.Script`: Python 스크립트 실행.
- `io.kestra.plugin.scripts.shell.Script`: 셸 명령 실행.
- `io.kestra.plugin.notifications.slack.SlackIncomingWebhook`: Slack 알림.

더 자세한 타입은 [Kestra 공식 문서](https://kestra.io/docs)를 참조하세요. 제공된 YAML 예시(`11_chat_with_rag.yaml`)에서는 AI 관련 타입을 사용해 RAG 워크플로우를 구현했습니다.

## YAML 기반 워크플로우에서 데이터 전처리 수행 방법

Kestra에서 워크플로우를 YAML 파일로 정의할 때, Python처럼 데이터 전처리(예: 데이터 필터링, 변환, 집계 등)를 수행하려면 주로 다음 두 가지 방법을 사용합니다. Kestra는 YAML 기반이지만, Python 스크립트를 직접 실행할 수 있는 태스크를 제공하므로 코드 작성의 유연성을 유지합니다.

### 1. **Python 스크립트를 YAML 태스크 내에 직접 작성**
   - `io.kestra.plugin.scripts.python.Script` 태스크를 사용하면 YAML 파일 안에 Python 코드를 인라인으로 작성할 수 있습니다.
   - 데이터 전처리 로직을 Python 코드로 구현하고, 필요한 라이브러리(예: pandas, numpy)를 설치/사용.
   - 장점: 모든 로직을 하나의 YAML 파일에 집중. 단점: 코드가 길어지면 가독성이 떨어질 수 있음.
   - 예시: CSV 파일을 읽어 전처리하고 결과를 출력.

   ```yaml
   tasks:
     - id: preprocess_data
       type: io.kestra.plugin.scripts.python.Script
       description: Inline Python script for data preprocessing
       beforeCommands:
         - pip install pandas  # 필요한 라이브러리 설치
       script: |
         import pandas as pd
         
         # 데이터 로드 (예: CSV 파일)
         df = pd.read_csv('input_data.csv')
         
         # 전처리: 결측치 제거, 컬럼 변환 등
         df = df.dropna()
         df['new_column'] = df['existing_column'] * 2
         
         # 결과 저장 또는 출력
         df.to_csv('processed_data.csv', index=False)
         print("전처리 완료: processed_data.csv 생성")
   ```

### 2. **별도의 .py 파일을 만들고 오케스트레이션이 실행**
   - Python 스크립트를 별도 파일(예: `preprocess.py`)로 작성하고, YAML 태스크에서 이를 실행.
   - `io.kestra.plugin.scripts.python.Script`의 `script` 대신 `inputFiles`나 `docker` 옵션으로 외부 파일을 참조하거나, 스크립트를 파일로 실행.
   - 장점: 코드 재사용성 높음, 버전 관리 용이. 단점: 파일 관리가 필요.
   - 실행 방식: YAML에서 `python preprocess.py` 명령을 실행하거나, 스크립트를 직접 호출.

   **단계별 예시:**
   - 먼저, 별도 .py 파일 생성 (예: `preprocess.py`).
     ```python
     import pandas as pd
     import sys
     
     def main():
         input_file = sys.argv[1] if len(sys.argv) > 1 else 'input_data.csv'
         output_file = 'processed_data.csv'
         
         # 데이터 로드 및 전처리
         df = pd.read_csv(input_file)
         df = df.dropna()
         df['new_column'] = df['existing_column'] * 2
         
         # 결과 저장
         df.to_csv(output_file, index=False)
         print(f"전처리 완료: {output_file} 생성")
     
     if __name__ == '__main__':
         main()
     ```
   - YAML 워크플로우에서 이 파일을 실행.
     ```yaml
     tasks:
       - id: run_preprocessing_script
         type: io.kestra.plugin.scripts.python.Script
         description: Execute external Python script for data preprocessing
         beforeCommands:
           - pip install pandas  # 라이브러리 설치
         inputFiles:
           preprocess.py: "{{ read('scripts/preprocess.py') }}"  # 외부 파일 참조 (또는 경로 직접 지정)
         script: |
           python preprocess.py input_data.csv  # 스크립트 실행
     ```

### 추가 팁
- **라이브러리 관리**: `beforeCommands`로 pip install을 사용하거나, Docker 기반 실행 시 이미지에 미리 설치.
- **데이터 입력/출력**: Kestra의 파일 태스크(예: `io.kestra.plugin.fs.http.Download`)로 데이터를 다운로드하고, 전처리 후 업로드.
- **병렬 처리**: 전처리 태스크를 `Parallel`로 묶어 여러 데이터셋을 동시에 처리.
- **비교**: YAML은 워크플로우 오케스트레이션에 집중하고, 실제 로직은 Python으로 위임. 이는 Airflow의 PythonOperator와 유사.
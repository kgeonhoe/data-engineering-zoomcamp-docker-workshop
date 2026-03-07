# DuckDB `-ui` 에러 해결방법

## 증상

```bash
$ uv run duckdb -ui
duckdb: Error: unknown option: -ui
```

DuckDB CLI에서 `-ui` 옵션을 인식하지 못하는 에러 발생.

---

## 원인

### 1. DuckDB CLI 버전 문제

`-ui` 플래그는 **DuckDB v1.2.1 이상**에서만 지원된다.  
시스템에 설치된 CLI 바이너리가 **v1.1.1**(구버전)이었기 때문에 해당 옵션을 인식하지 못했다.

### 2. Python 패키지 vs CLI 바이너리 버전 불일치

| 구분 | 버전 | 설치 경로 |
|------|------|-----------|
| Python 패키지 (`duckdb`) | v1.4.4 ✅ | `.venv/Lib/site-packages` (uv 관리) |
| CLI 바이너리 (`duckdb.exe`) | v1.1.1 ❌ | WinGet 경로 (`C:\Users\...\WinGet\Packages\...`) |

- `uv add duckdb`로 설치하는 것은 **Python 라이브러리**이다.
- `duckdb -ui`로 실행하는 것은 **CLI 바이너리**이며, 이 둘은 **별도로 관리**된다.
- CLI 바이너리는 **WinGet**을 통해 설치되어 있었고, 구버전(v1.1.1)이 유지되고 있었다.

### 3. CLI 바이너리 위치 확인

```bash
$ where duckdb
C:\Users\greybot\AppData\Local\Microsoft\WinGet\Packages\DuckDB.cli_Microsoft.Winget.Source_8wekyb3d8bbwe\duckdb.exe
```

→ WinGet으로 설치된 시스템 CLI가 호출되고 있었음.

---

## 해결 방법

### WinGet을 통해 DuckDB CLI 업그레이드

```bash
winget upgrade DuckDB.cli
```

### 업그레이드 결과

```
찾음 DuckDB CLI [DuckDB.cli] 버전 1.4.4
...
설치 성공
```

### 버전 확인

```bash
$ duckdb --version
v1.4.4 (Andium) 6ddac802ff
```

---

## 사용법

```bash
# 기본 UI 실행 (localhost:4213)
duckdb -ui

# 특정 DB 파일과 함께 UI 실행
duckdb taxi_rides_ny.duckdb -ui
```

브라우저에서 `http://localhost:4213` 으로 접속하면 DuckDB 웹 UI를 사용할 수 있다.

---

## 핵심 정리

> **`uv add duckdb`** = Python 라이브러리 설치 (코드에서 `import duckdb`용)  
> **`winget install/upgrade DuckDB.cli`** = CLI 바이너리 설치 (터미널에서 `duckdb` 명령어용)  
> 두 개는 **별도로 관리**되므로 버전을 각각 확인해야 한다.

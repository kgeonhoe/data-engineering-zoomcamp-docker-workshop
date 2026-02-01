## Click vs argparse 비교

### 1. 코드 비교

**argparse (표준 라이브러리)**
```python
import argparse

def main():
    parser = argparse.ArgumentParser(description='Ingest data')
    parser.add_argument('--pg-user', default='root', help='PostgreSQL user')
    parser.add_argument('--pg-pass', default='root', help='PostgreSQL password')
    parser.add_argument('--pg-port', type=int, default=5432, help='PostgreSQL port')
    args = parser.parse_args()
    
    # args.pg_user, args.pg_pass 등으로 접근
    print(args.pg_user)

if __name__ == '__main__':
    main()
```

**Click**
```python
import click

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
def main(pg_user, pg_pass, pg_port):
    # 함수 인자로 바로 접근
    print(pg_user)

if __name__ == '__main__':
    main()
```

---

### 2. Click이 더 간결한 이유

| 항목 | argparse | Click |
|------|----------|-------|
| **파서 생성** | `ArgumentParser()` 객체 직접 생성 필요 | 데코레이터가 자동 처리 |
| **인자 접근** | `args.pg_user` (namespace 객체) | 함수 매개변수로 바로 전달 |
| **타입 변환** | `type=int` 별도 지정 | `type=int` 또는 자동 추론 |
| **서브커맨드** | 복잡한 subparser 설정 필요 | `@click.group()` 데코레이터로 간단 |
| **프롬프트 입력** | 직접 구현 필요 | `prompt=True` 옵션 한 줄 |
| **컬러 출력** | 외부 라이브러리 필요 | `click.style()` 내장 |

---

### 3. Click의 추가 장점

```python
# 비밀번호 숨김 입력
@click.option('--password', prompt=True, hide_input=True)

# 파일 자동 처리
@click.option('--input', type=click.File('r'))

# 선택지 제한
@click.option('--env', type=click.Choice(['dev', 'prod']))

# 확인 프롬프트
@click.confirmation_option(prompt='정말 실행할까요?')
```

---

### 4. 요약

| Click 장점 |
|------------|
| 데코레이터 기반 → 보일러플레이트 감소 |
| 옵션이 함수 인자로 직접 전달 → `args.` 접근 불필요 |
| 서브커맨드, 프롬프트, 컬러 등 기능 내장 |
| 코드 가독성 향상 |
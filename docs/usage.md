# 실행·코드 안내

[README](../README.md) · [v1 구현 기록](v1.md) · [v2 복구 기록](v2.md) · [전체 계획](../PLAN.md)

## 컨테이너 구성

| 서비스 | 역할 |
| --- | --- |
| `postgres` | Airflow 메타데이터 DB와 업무용 `orders` DB를 별도 데이터베이스·역할로 구성 |
| `airflow-init` | Airflow 마이그레이션·초기 계정·Pool·업무 테이블 생성 후 종료 |
| `airflow-scheduler` | DAG 스케줄링과 LocalExecutor의 파일별 작업 프로세스 실행 |
| `airflow-webserver` | `127.0.0.1:8085`의 UI·REST API |

`airflow-init`의 `Exited (0)`은 정상이다. 스케줄러·웹서버·PostgreSQL은 healthy 상태여야 한다. 외부 사내 DB·NAS·API를 사용하지 않으며 PostgreSQL 포트는 호스트에 공개하지 않는다.

초기 계정 생성은 최초 한 번 수행한다. 이미 존재하는 사용자의 비밀번호는 `.env` 변경만으로 바뀌지 않는다. `CUSTOMER_TOKEN_KEY`는 재실행 중 유지해야 한다. 키가 바뀌면 고객 토큰이 달라지고 기존 주문과 내용 충돌이 생긴다.

## 실행 순서와 코드

```text
scripts/generate_orders.py → input/<dataset>/shop_a/*.csv, shop_b/*.json
    ↓ DAG 실행
orders_etl.discover_batches → pipeline.discover
    ↓ 원본 스냅샷·manifest, 작은 XCom 목록
orders_etl.process_batch.expand → pipeline.process
    ↓ parse_rows → normalize → store_orders
orders_etl.finalize_run → pipeline.finalize
    ↓ orders·batch_rows·표준 파일 대조 → report.json
```

| 파일 | 확인할 내용 |
| --- | --- |
| [DAG](../dags/orders_etl.py) | 매핑·전용 Pool·최종 실패 판정 |
| [파이프라인](../src/commerce_etl/pipeline.py) | 원본 고정·파일별 처리·결과 검증 |
| [변환](../src/commerce_etl/transform.py) | CSV/JSON 계약·UTC 변환·원화 정수 검증·개인정보 제외 |
| [DB](../src/commerce_etl/database.py) | 고유 키·동시 INSERT 보호·내용 충돌·실행별 처리 결과 |
| [실행 도구](../scripts/run_demo.py) | REST로 Pool 변경·실행·장애 주입 설정·결과 수집 |
| [장애 주입](../src/commerce_etl/faults.py) | 파일 저장 후 DB 오류 예외 / DB 커밋 후 프로세스 종료 |

XCom에는 파일 경로·해시·배치 ID·건수만 전달한다. 주문 1건을 태스크 하나로 만들지 않아 태스크 수는 파일 수에 비례한다. 입력은 최대 200파일, 동일 소스의 바이트가 같은 파일은 한 배치로 묶는다. 현재는 파일 전체를 메모리에서 처리하므로 대용량 단일 파일은 후속 분할·스트리밍 검토 대상이다.

## 병렬도 변경

`run_demo.py --slots 1|2|4`는 실행 중인 DAG가 없는지 확인하고 `orders_pool` 슬롯을 변경한다. DAG의 `max_active_tasks`와 mapped task 제한은 4, executor parallelism은 8이다. 다른 설정을 줄이면 Pool 슬롯이 남아도 실제 동시 실행 수가 줄 수 있다.

```bash
python3 scripts/run_demo.py --slots 4 --dataset demo --output .local/demo-4.json
```

UI로 직접 실행하려면 `orders_etl`을 unpause한 뒤 Trigger DAG에서 `dataset`을 입력한다. 동시 실행 중인 DAG는 한 개로 제한한다. 별도 실행 데이터는 `--dataset sample2`처럼 다른 이름으로 생성한다.

## v2 — 장애 주입과 복구

실패한 태스크만 자동 재시도하고 성공한 태스크를 유지하는 정책은 v1부터 있다. 아래 v2 실험은 새로 추가한 장애 주입 기능으로 기존 재시도와 중복 방지가 부분 저장 상태에서도 동작하는지 확인한다. 파일 파싱 실패·해시 불일치의 재시도 중단은 v2에서 추가했다.

기존 v1 환경에서는 실행 중인 DAG가 없을 때 `docker compose up -d --build`로 새 코드를 반영한다. 기본 실행은 `fault_point=none`이며, 장애는 명시적으로 선택한 경우에만 발생한다.

### 경로 1: 자동 재시도

```bash
# 새 데이터셋: 4파일 × 1,000주문
docker compose exec airflow-scheduler python scripts/generate_orders.py \
  --dataset v2_auto --files-per-shop 2 --rows-per-file 1000

# 두 번째 파일만, 결과 파일 저장 후 DB 오류 예외를 한 번 발생
python3 scripts/run_demo.py --dataset v2_auto --slots 4 \
  --fault-point after_file --fault-mode once --fault-index 1
```

```text
나머지 파일 성공 / 두 번째 파일 실패
    ↓ Airflow가 15초 후 1회 자동 재시도
실패한 파일만 처음부터 처리 → 전체 검증 성공
```

`--fault-point after_commit`으로 바꾸면 DB 커밋 직후 태스크 프로세스를 종료한다. 완료 파일을 쓰기 전에 종료되므로 DB는 성공·로컬 요약은 미완료 상태가 될 수 있다. 재시도에서는 이미 저장한 주문을 중복으로 판정하고 기록을 다시 작성한다.

### 경로 2: 재시도 소진과 실패 판정

```bash
docker compose exec airflow-scheduler python scripts/generate_orders.py \
  --dataset v2_exhausted --files-per-shop 2 --rows-per-file 1000

# 최초 시도와 자동 재시도를 모두 실패시킴. 종료 코드 1은 이 데모의 예상 결과.
python3 scripts/run_demo.py --dataset v2_exhausted --slots 4 \
  --fault-point after_file --fault-mode always --fault-index 1
```

```text
첫 시도 실패 → Airflow 자동 재시도 → 다시 실패
    ↓ 재시도 1회 소진
실패 보고서·DAG 실패 → 자동 실행 종료
```

실제 일시 장애가 재시도 한도를 넘기면 사람이 로그·보고서로 원인을 확인한다. 원인을 해결한 뒤 같은 DAG 실행의 실패한 mapped task와 `finalize_run`을 Airflow UI에서 선택해 clear할 수 있다. 성공한 태스크는 선택하지 않고, 재실행 대상 미리보기에서 범위를 확인한다. 이는 운영자가 수행하는 절차이며 별도 복구 자동화는 구현 범위에 포함하지 않는다.

`always` 데모는 같은 설정으로 clear해도 계속 실패한다. 장애 없는 결과를 확인하려면 아래처럼 `none`으로 새 DAG 실행을 시작한다. 이 명령은 전체 입력을 다시 처리하며, 같은 실행의 실패 태스크만 복구하는 명령이 아니다.

```bash
python3 scripts/run_demo.py --dataset v2_exhausted --slots 4 --fault-point none
```

| 옵션 | 의미 |
| --- | --- |
| `--fault-point none / after_file / after_commit` | 장애 없음 / 결과 파일 저장 직후 DB 오류 예외 / DB 커밋 직후 강제 종료 |
| `--fault-mode once / always` | 같은 실행·배치·장애 지점에서 한 번 / 해당 지점에 도달할 때마다 |
| `--fault-index 1` | 두 번째 mapped task 선택. `-1`은 모든 파일 |

`after_file`은 특정 작업 안에서 DB 접근 오류를 모사하며 PostgreSQL 서버를 중단하지 않는다. `after_commit`은 해당 태스크 프로세스만 실제 종료한다. 새 DAG 실행에는 새 장애 상태가 적용된다.

읽을 수 없는 JSON/CSV 파일과 원본 해시 불일치는 자동 재시도하지 않고 파일 실패로 기록한다. 개별 주문 값 오류는 `invalid`로 남기고 나머지 정상 행을 처리하며, 최종 검증을 실패시킨다. 고정된 원본을 바꿔야 하는 오류라면 입력을 수정한 뒤 새 DAG 실행으로 처리한다.

## 저장 데이터

| 위치 / 테이블 | 내용 |
| --- | --- |
| `data/input/<dataset>/` | 생성한 합성 원본 |
| `data/raw/<source>/` | 입력 해시로 고정한 원본 사본 |
| `data/normalized/<batch_id>.jsonl` | 개인정보 제외 후 유효한 주문 후보. 내용 충돌 후보도 포함 |
| `data/runs/<run_hash>/manifest.json` | 해당 실행의 입력 목록·변환 버전 |
| `data/runs/<run_hash>/<batch_id>.json` | 파일별 상태·건수·처리 시각 |
| `data/runs/<run_hash>/report.json` | 전체 결과·검증 오류 |
| `data/runs/<run_hash>/faults/` | `once` 장애 발생 표시. 해당 실행·배치·장애 지점에만 적용 |
| `orders` | 고유 주문과 표준 payload |
| `batch_runs` / `batch_rows` | 실행별 배치 요약 / 주문별 inserted·duplicate·invalid·conflict |

컨테이너의 `/opt/airflow/data`는 named volume이다. `run_demo.py`는 실행 결과를 호스트 `.local/`로 내보낸다. 실행 결과는 로컬 확인용이며 별도 `evaluation/` 종합 파일은 만들지 않는다. DB 결과는 다음처럼 확인한다.

```bash
docker compose exec postgres psql -U orders -d orders \
  -c 'SELECT source, count(*) FROM orders GROUP BY source;'
docker compose exec postgres psql -U orders -d orders \
  -c 'SELECT source, order_id, payload FROM orders LIMIT 3;'
```

DB 트랜잭션 안에서 같은 배치에 대한 advisory lock을 잡고 파일을 원자적으로 교체한다. 서로 다른 배치의 주문 키는 정렬된 순서로 삽입한다. `ON CONFLICT DO NOTHING` 이후 실제 저장된 내용과 비교하여 동일 주문과 충돌을 구분한다. 파일 저장과 DB 커밋은 단일 트랜잭션이 아니므로 부분 실패 상태는 남을 수 있다.

주문 오류는 격리된 `batch_rows` 기록으로 남기고 정상 주문은 적재한다. invalid·conflict가 있으면 최종 검증은 실패시킨다. 분석 결과에 원본 고객 ID·이름·이메일·주소를 남기지 않으며 원본은 `raw`에 유지한다.

## 테스트와 현재 검증 범위

```bash
# 호스트: Airflow·DB 없는 순수 로직 테스트
uv sync --extra dev --python 3.12
uv run pytest -q
uv run ruff check src scripts dags tests

# 컨테이너: DB 통합 + 실제 Airflow DAG import·구조 검사
docker compose exec -e TEST_ORDERS_DATABASE_URL=postgresql://orders:orders@postgres/orders \
  airflow-scheduler python -m pytest /opt/airflow/tests -q
```

DB 통합 테스트는 임의 이름의 테스트 스키마를 만들고 종료 시 해당 스키마만 삭제한다. `TEST_ORDERS_DATABASE_URL`이 없으면 DB 통합 테스트를, Airflow가 설치되지 않았으면 DAG 검사를 skip한다.

기존에 저장한 DAG 실행 기록에서는 두 저장 경계의 자동 재시도와 재시도 소진 시 실패 판정, 성공 태스크 유지, 빈 입력·전체 파일 실패·파싱 실패·주문 값 오류를 확인했다. 자동 재시도 기록은 `.local/v2-file-once.json`, `.local/v2_commit_once.json`에 있다. 현재 코드의 테스트 결과는 아래에 별도로 기록한다. 별도 `orders_integrity` DAG와 공정한 성능 실험은 v3 계획이다.

`tests/test_recovery.py`의 DB 통합 테스트는 `process()`를 다시 호출해 부분 저장 후 재처리를 검증한다. Airflow 스케줄러의 자동 재시도 검증은 위 DAG 실행 기록과 구분한다.

**현재 코드 검증 (2026-10-14):** 호스트 `.venv/bin/pytest -q`에서 DAG 구조 검사를 포함해 **29개 통과·DB 통합 14개 skip**, `.venv/bin/ruff check src scripts dags tests` 통과. Docker 데몬이 실행되지 않아 이번 변경 후 컨테이너 DB 통합 테스트와 실제 DAG 실행은 다시 수행하지 않았다.

`finalize_run`은 `all_done`으로 리포트를 남기며 상류 태스크 실패·미완료·데이터 오류가 있으면 예외를 올린다. 단순히 최종 리포트가 만들어졌다고 DAG를 성공 처리하지 않는다.

## 재시작과 진단

```bash
docker compose ps -a
docker compose logs --tail=50 airflow-init airflow-scheduler
docker compose exec airflow-scheduler airflow dags list-import-errors
```

소스 코드는 이미지에 포함돼 있으므로 수정 후 `docker compose build airflow-scheduler && docker compose up -d`로 반영한다. 실행 중인 DAG가 없을 때 재시작한다. DAG·스크립트·테스트는 읽기 전용 마운트로 연결돼 있다.

초기화는 기존 계정·Pool 슬롯·데이터를 덮어쓰지 않는다. 데이터 전체 삭제는 평소 종료 절차에 포함하지 않는다.

구성 참고: [Airflow 2.11 Docker](https://airflow.apache.org/docs/apache-airflow/2.11.0/howto/docker-compose/index.html), [Dynamic Task Mapping](https://airflow.apache.org/docs/apache-airflow/2.11.0/authoring-and-scheduling/dynamic-task-mapping.html), [LocalExecutor](https://airflow.apache.org/docs/apache-airflow/2.11.0/core-concepts/executor/local.html).

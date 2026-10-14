# Commerce ETL Pipeline

**Airflow로 주문 파일을 병렬 처리하고, 중복 적재와 데이터 누락을 검증하는 ETL 포트폴리오입니다.**

의료영상 ETL 실무에서 다룬 입력 형식 차이·개인정보 처리·부분 실패 문제를 가상 쇼핑몰 주문으로 재구성했습니다. 모든 입력은 직접 생성한 합성 데이터이며, 커머스 서비스의 실제 운영 경력을 의미하지 않습니다.

**현재: v2 구현.** v1의 파일별 병렬 적재·자동 재시도 구조에 장애 주입과 영구 오류 구분을 추가했습니다. 파일 저장·DB 커밋 직후 실패해도 재처리 후 데이터가 유지되는지 검증했습니다. 공정한 성능 비교와 별도 정합성 검사 DAG는 v3 계획입니다.

[프로젝트 제안](docs/v0.md) · [구현 계획](PLAN.md) · [v1 구현 기록](docs/v1.md) · [v2 복구 기록](docs/v2.md) · [실행 안내](docs/usage.md)

## 실행 흐름

```text
쇼핑몰 A CSV + 쇼핑몰 B JSON
    ↓ discover_batches: 원본 보관·파일 목록 확정
Dynamic Task Mapping — Pool 슬롯 1·2·4
    ├─ 파일 A: 검증·가명화 → 표준 파일 → PostgreSQL
    ├─ 파일 B: 검증·가명화 → 표준 파일 → PostgreSQL
    └─ 파일 C: 검증·가명화 → 표준 파일 → PostgreSQL
    ↓ finalize_run
처리 결과 집계 → 주문 키·금액·파일 대조 → 성공 또는 실패
```

Airflow는 작업 실행과 재시도를 관리합니다. 주문 중복은 PostgreSQL의 `(source, order_id)` 고유 키로 막고, 같은 키의 다른 내용은 덮어쓰지 않고 충돌로 기록합니다. 고객 ID는 가명화하고 이름·이메일·주소는 분석 결과에서 제외합니다.

## 빠른 시작

Docker Compose와 호스트 Python 3가 필요합니다. Docker 메모리는 최소 4GB, 다른 서비스를 함께 실행한다면 8GB 이상을 권장합니다. 아래 구성은 로컬 데모용입니다.

```bash
python3 scripts/setup_env.py
docker compose up -d --build
docker compose ps
```

서비스가 `healthy`가 되면 [Airflow UI](http://127.0.0.1:8085)에 접속합니다. 기본 로그인은 **admin / admin**이며, `.env`에서 포트·계정을 설정할 수 있습니다.

```bash
# 처음 한 번 합성 데이터 생성: 8파일 × 5,000주문
# 같은 dataset이 있으면 덮어쓰지 않습니다.
docker compose exec airflow-scheduler python scripts/generate_orders.py \
  --dataset demo --files-per-shop 4 --rows-per-file 5000

# Pool 설정 → DAG 실행 → 완료 대기 → 검증 결과를 .local/에 저장
python3 scripts/run_demo.py --slots 4 --dataset demo
```

UI에서 `orders_etl`의 mapped task 실행을 확인할 수 있습니다. 아래 명령은 같은 입력을 다시 처리하며, 새 주문 대신 중복으로 집계합니다.

```bash
python3 scripts/run_demo.py --slots 1 --dataset demo
python3 scripts/run_demo.py --slots 2 --dataset demo
```

재처리와 최초 적재는 작업량이 달라 위 실행 시간을 성능 개선 수치로 비교하지 않습니다. v1의 실제 검증 결과와 범위는 [v1 구현 기록](docs/v1.md)에 남깁니다.

**v1 확인 결과:** 8파일·40,000주문 적재 성공, Pool 1·2·4에서 실제 처리 중첩 1·2·4 확인, 재처리 시 신규 주문 0건·중복 40,000건. DB 통합·DAG 구조를 포함한 테스트 36개 통과.

## v2: 장애 주입으로 기존 재시도 구조 검증

```bash
# 기존 demo 데이터의 두 번째 파일에서 장애를 한 번 발생시킵니다.
python3 scripts/run_demo.py --dataset demo --slots 4 \
  --fault-point after_file --fault-mode once --fault-index 1
```

**실패한 파일만 재시도하고 성공한 파일은 유지하는 동작은 v1부터 있었습니다.** v2에서는 두 저장 경계에 장애를 주입해 기존 재시도 정책과 중복 방지 로직이 부분 저장 상태에서도 동작하는지 확인합니다. `--fault-mode once`이면 재시도에서 같은 장애를 다시 주입하지 않습니다. `always`이면 자동 재시도도 실패하고 실패 보고서·DAG 실패 상태를 남깁니다. 새 데이터로 재현하는 절차와 DB 커밋 후 종료 실험은 [실행 안내](docs/usage.md#v2--장애-주입과-복구)에 있습니다.

**v2 기존 DAG 검증 결과:** 두 장애 지점에서 자동 재시도 성공, 실험별 4,000주문 유지, 성공한 파일 재실행 없음. 현재 코드의 테스트 범위와 실행 결과는 [실행 안내](docs/usage.md#테스트와-현재-검증-범위)에 구분해 기록합니다.

## 검증과 기술

```bash
# 실제 PostgreSQL의 격리된 임시 스키마 + Airflow DAG 검사
# 업무용 데모 테이블은 변경하지 않습니다.
docker compose exec -e TEST_ORDERS_DATABASE_URL=postgresql://orders:orders@postgres/orders \
  airflow-scheduler python -m pytest /opt/airflow/tests -q
```

**Python 3.12 · Airflow 2.11.0 / LocalExecutor · PostgreSQL 16.9 · Docker Compose · pytest**

단일 머신의 프로세스 병렬 처리입니다. 합성 원본은 별도 보관하며, 가명화가 완전한 익명성을 보장한다는 의미는 아닙니다. 실행 종료는 `docker compose down`이며 DB·파일·로그 볼륨은 유지됩니다.

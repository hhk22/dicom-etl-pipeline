# Commerce ETL Pipeline

**Airflow로 주문 파일을 병렬 처리하고, 중복 적재와 데이터 누락을 검증하는 ETL 포트폴리오입니다.**

의료영상 ETL 실무에서 다룬 입력 형식 차이·개인정보 처리·부분 실패 문제를 가상 쇼핑몰 주문으로 재구성했습니다. 모든 입력은 직접 생성한 합성 데이터이며, 커머스 서비스의 실제 운영 경력을 의미하지 않습니다.

**현재: v1 구현.** 파일별 병렬 적재와 결과 검증을 실행할 수 있습니다. 장애 주입·선택적 태스크 복구 데모는 v2, 공정한 성능 비교와 별도 정합성 검사 DAG는 v3 범위입니다.

[구현 계획](PLAN.md) · [실행·코드 안내](docs/v1.md) · [v1 검증 기록](evaluation/v1-smoke.json)

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

재처리와 최초 적재는 작업량이 달라 위 실행 시간을 성능 개선 수치로 비교하지 않습니다. v1의 실제 검증 결과와 범위는 [검증 기록](evaluation/v1-smoke.json)에 남깁니다.

**v1 확인 결과:** 8파일·40,000주문 적재 성공, Pool 1·2·4에서 실제 처리 중첩 1·2·4 확인, 재처리 시 신규 주문 0건·중복 40,000건. DB 통합·DAG 구조를 포함한 테스트 36개 통과.

## 검증과 기술

```bash
# 실제 PostgreSQL의 격리된 임시 스키마 + Airflow DAG 검사
# 업무용 데모 테이블은 변경하지 않습니다.
docker compose exec -e TEST_ORDERS_DATABASE_URL=postgresql://orders:orders@postgres/orders \
  airflow-scheduler python -m pytest /opt/airflow/tests -q
```

**Python 3.12 · Airflow 2.11.0 / LocalExecutor · PostgreSQL 16.9 · Docker Compose · pytest**

단일 머신의 프로세스 병렬 처리입니다. 합성 원본은 별도 보관하며, 가명화가 완전한 익명성을 보장한다는 의미는 아닙니다. 실행 종료는 `docker compose down`이며 DB·파일·로그 볼륨은 유지됩니다.

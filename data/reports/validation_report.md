# Pipeline Reconciliation & SLA Sentinel Validation Report

**Run ID:** `run_20261028_021702_557a4509`  
**Generated At:** `2026-10-28T02:17:14.530179+00:00`  
**Pipeline Status:** `PARTIAL_SUCCESS`  
**Execution Duration:** `12.1236 seconds`

## 1. Mathematical Conservation & Integrity Check

- **Conservation Equation:** `Input (10000) = Curated (9895) + Quarantine (105)` -> **✅ PASSED**
- **Discrepancy Count:** `0`
- **Primary Key Violations (Curated Batches):** `0`
- **Primary Key Violations (Alert Events):** `0`
- **Referential Integrity Violations:** `0`

## 2. Ingested Datasets Operational Summary

| Dataset ID | Total Batches | Ingested Rows | Late Batches | SLA Breached | Avg Delay (min) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| `clickstream_events_15m` | 1971 | 45,551,087 | 142 | 37 | 3.67m |
| `customer_profiles_daily` | 2015 | 47,637,131 | 137 | 47 | 13.27m |
| `inventory_snapshots_6h` | 1964 | 47,250,000 | 143 | 39 | 9.69m |
| `orders_stream_hourly` | 1972 | 47,792,721 | 141 | 37 | 5.27m |
| `payments_settlement_daily` | 1981 | 46,624,441 | 149 | 44 | 7.28m |

## 3. Dataset Freshness & SLA Lag Overview

| Dataset ID | Freshness Status | Current Lag (min) | Max Allowed (min) | SLA Breached |
| :--- | :---: | :---: | :---: | :---: |
| `clickstream_events_15m` | 🔴 `CRITICAL_STALE` | 14400.0m | 30.0m | True |
| `clickstream_events_15m` | 🔴 `CRITICAL_STALE` | 1425.2m | 30.0m | True |
| `customer_profiles_daily` | 🟢 `FRESH` | 271.0m | 1560.0m | False |
| `customer_profiles_daily` | 🟢 `FRESH` | 0.0m | 1560.0m | False |
| `inventory_snapshots_6h` | 🔴 `CRITICAL_STALE` | 14400.0m | 480.0m | True |
| `inventory_snapshots_6h` | 🔴 `CRITICAL_STALE` | 1080.0m | 480.0m | True |
| `orders_stream_hourly` | 🟢 `FRESH` | 95.0m | 120.0m | False |
| `orders_stream_hourly` | 🟢 `FRESH` | 0.0m | 120.0m | False |
| `payments_settlement_daily` | 🟢 `FRESH` | 151.0m | 1500.0m | False |
| `payments_settlement_daily` | 🟢 `FRESH` | 0.0m | 1500.0m | False |

## 4. Active Alert Distribution

| Alert Type | Severity | Count |
| :--- | :---: | :---: |
| `LATE_ARRIVAL` | 🔴 CRITICAL | 158 |
| `LATE_ARRIVAL` | 🟡 WARNING | 259 |
| `MISSING_LOAD` | 🔴 CRITICAL | 74 |
| `MISSING_LOAD` | 🟡 WARNING | 356 |
| `SLA_BREACH` | 🔴 CRITICAL | 120 |
| `SLA_BREACH` | 🟡 WARNING | 37 |
| `STALE_DATA` | 🟡 WARNING | 4 |

## 5. Idempotency & Quarantine Audit Findings

- All quarantined records have deterministic `quarantine_id` hashes and are preserved in `quarantine_records`.
- Rerunning the identical pipeline with the same input preserves exact row counts with zero duplicate insertion.
- Alerts use deterministic SHA-256 deduplication keys preventing alert flooding.

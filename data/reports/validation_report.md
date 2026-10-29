# Pipeline Reconciliation & SLA Sentinel Validation Report

**Run ID:** `run_20261028_031338_662af661`  
**Generated At:** `2026-10-28T03:13:38.910810+00:00`  
**Pipeline Status:** `PARTIAL_SUCCESS`  
**Execution Duration:** `0.614 seconds`

## 1. Mathematical Conservation & Integrity Check

- **Conservation Equation:** `Input (14) = Curated (8) + Quarantine (6)` -> **✅ PASSED**
- **Discrepancy Count:** `0`
- **Primary Key Violations (Curated Batches):** `0`
- **Primary Key Violations (Alert Events):** `0`
- **Referential Integrity Violations:** `0`

## 2. Ingested Datasets Operational Summary

| Dataset ID | Total Batches | Ingested Rows | Late Batches | SLA Breached | Avg Delay (min) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| `clickstream_events_15m` | 19626 | 460,171,149 | 1418 | 381 | 3.57m |
| `customer_profiles_daily` | 19942 | 470,478,364 | 1366 | 422 | 13.33m |
| `inventory_snapshots_6h` | 19872 | 471,387,364 | 1401 | 405 | 9.28m |
| `orders_stream_hourly` | 19859 | 469,288,619 | 1419 | 403 | 5.28m |
| `payments_settlement_daily` | 19706 | 464,734,926 | 1425 | 386 | 7.29m |

## 3. Dataset Freshness & SLA Lag Overview

| Dataset ID | Freshness Status | Current Lag (min) | Max Allowed (min) | SLA Breached |
| :--- | :---: | :---: | :---: | :---: |
| `clickstream_events_15m` | 🔴 `CRITICAL_STALE` | 1425.0m | 30.0m | True |
| `clickstream_events_15m` | 🔴 `CRITICAL_STALE` | 388185.0m | 30.0m | True |
| `customer_profiles_daily` | 🟢 `FRESH` | 0.0m | 1560.0m | False |
| `customer_profiles_daily` | 🟢 `FRESH` | 601.0m | 1560.0m | False |
| `inventory_snapshots_6h` | 🔴 `CRITICAL_STALE` | 1080.0m | 480.0m | True |
| `inventory_snapshots_6h` | 🔴 `CRITICAL_STALE` | 387840.0m | 480.0m | True |
| `orders_stream_hourly` | 🟢 `FRESH` | 0.0m | 120.0m | False |
| `orders_stream_hourly` | 🔴 `CRITICAL_STALE` | 425.0m | 120.0m | True |
| `payments_settlement_daily` | 🟢 `FRESH` | 0.0m | 1500.0m | False |
| `payments_settlement_daily` | 🟢 `FRESH` | 481.0m | 1500.0m | False |

## 4. Active Alert Distribution

| Alert Type | Severity | Count |
| :--- | :---: | :---: |
| `LATE_ARRIVAL` | 🔴 CRITICAL | 658 |
| `LATE_ARRIVAL` | 🟡 WARNING | 1288 |
| `MISSING_LOAD` | 🔴 CRITICAL | 74 |
| `MISSING_LOAD` | 🟡 WARNING | 356 |
| `SLA_BREACH` | 🔴 CRITICAL | 494 |
| `SLA_BREACH` | 🟡 WARNING | 356 |
| `STALE_DATA` | 🔴 CRITICAL | 1 |
| `STALE_DATA` | 🟡 WARNING | 4 |

## 5. Idempotency & Quarantine Audit Findings

- All quarantined records have deterministic `quarantine_id` hashes and are preserved in `quarantine_records`.
- Rerunning the identical pipeline with the same input preserves exact row counts with zero duplicate insertion.
- Alerts use deterministic SHA-256 deduplication keys preventing alert flooding.

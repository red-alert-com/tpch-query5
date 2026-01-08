# TPC-H Query 5: Performance Analysis and Load Test Report

## 1. Introduction
The objective of this project was to implement and optimize **TPC-H Query 5** (Local Supplier Volume) to handle a **Scale Factor 2 (SF2)** dataset. This involves joining six tables and processing approximately 12 million rows in the `lineitem` table to calculate revenue for local suppliers within a specific region and time window.

## 2. System Specifications
* **Dataset:** TPC-H SF2 (approx. 2.4 GB of raw text data)
* **Hardware:** Apple M4 Chip (10-core CPU: 4 performance and 6 efficiency cores)
* **Compiler:** Apple Clang
* **Execution Environment:** Unix-based Terminal (macOS)

## 3. Performance Results
The following benchmarks were recorded using the `time` utility. The baseline was established with a single thread, compared against an optimized run with 4 threads.

| Metric | Single-Threaded (Baseline) | Multi-Threaded (4 Threads) |
| :--- | :--- | :--- |
| **Real Time (Total)** | **81.61s** | **37.22s** |
| **User Time (CPU)** | 71.46s | 74.42s |
| **Sys Time (Kernel)** | 9.38s | 21.23s |
| **CPU Utilization** | 99% | 256% |
| **Speedup** | 1.00x | **2.19x** |

## 4. Technical Analysis

### Speedup and Parallel Efficiency
The implementation achieved a **2.19x speedup** with 4 threads. While total execution time was reduced by more than half, the scaling is not perfectly linear (4x). This is primarily due to the transition of the bottleneck from CPU-bound computation to I/O-bound data retrieval.



### Why User Time Increased
The "User Time" (Total CPU work) increased from 71.46s to 74.42s. This marginal increase is attributed to:
1.  **Thread Management:** The computational cost of spawning, synchronizing, and joining parallel threads.
2.  **Locking Overhead:** Minimal contention during the final aggregation of results from local thread-maps to the global results map.
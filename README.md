# Schema Drift Detection Tool

A high-performance desktop application for detecting structural changes between datasets and intelligently mapping column differences using semantic similarity.

---

## Overview

This tool is designed to validate and compare two datasets:

* **Previous file** → known correct schema
* **Current file** → new incoming data

It detects:

* Schema mismatches
* Column reordering
* Missing / new columns
* Renamed columns (via AI-based similarity)

---

##  Key Features

###  Structural Validation

* Column count comparison
* Column order validation
* Detection of:

  * Missing columns
  * New columns
  * Reordered schemas

---

###  Intelligent Column Matching

Uses **semantic embeddings + lexical similarity** to identify renamed columns:

* Embedding similarity (context-aware)
* Token overlap (lexical fallback)
* Weighted scoring system

---

###  High Performance (Built for Large Data)

* Uses **Polars lazy execution** for fast schema extraction
* Converts `.xlsx` → `.csv` for optimized processing
* Caches converted files to avoid recomputation
* Batch embedding inference

---

###  User Interface

* Simple **Tkinter GUI**
* File selection via dialog
* Real-time validation output

---

##  Supported File Formats

| Format  | Supported | Notes                       |
| ------- | --------- | --------------------------- |
| CSV     | ✅         | Fast (lazy loading)         |
| Parquet | ✅         | Fastest                     |
| XLSX    | ✅         | Converted to CSV internally |

---

## 🏗️ Architecture

```text
User Input (Tkinter UI)
        ↓
File Normalization (CSV conversion + caching)
        ↓
Schema Extraction (Polars lazy)
        ↓
Structural Validation
        ↓
Column Matching Engine
   ├── Embeddings (semantic)
```

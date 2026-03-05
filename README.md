# Schema Validator

A lightweight schema drift analysis utility for tabular ETL refreshes (CSV/Parquet), with a desktop file-picker workflow and a multi-sheet Excel report output.

The tool compares a **previous** dataset against a **refresh** dataset and surfaces:
- column order changes,
- missing/extra columns,
- inferred data format drift,
- header-level and semantic column similarity,
- AI-assisted best-match column mapping suggestions.

---

## 1) What this tool does

At a high level, the script performs six analyses and writes each result to a dedicated Excel sheet:

1. **Column order validation** (`column_order`)
2. **Missing columns from refresh** (`missing_columns`)
3. **Extra columns in refresh** (`extra_columns`)
4. **Detected format comparison** (`format_analysis`)
5. **Pairwise similarity matrix** (`column_similarity`)
6. **Suggested column mapping** (`embedding_mapping`)

This makes it practical for ETL engineers to identify hard schema breaks and soft naming drifts before promoting new refreshes to production.

---

## 2) Execution flow

### Input collection
- Uses a Tkinter dialog to select:
  - previous dataset,
  - refresh dataset,
  - output folder.

### Preprocessing
- CSV files are converted to Parquet once and cached as `<original>.parquet` in the same folder.
- Data is read via `polars` lazy readers and sampled to `SAMPLE_SIZE` rows (default: `100000`).

### Analysis
- Compares schema metadata and inferred column-level formats.
- Computes lexical (`difflib`) and semantic (`sentence-transformers`) similarity.
- Produces best-match mapping based on weighted scoring.

### Output
- Writes `etl_schema_analysis.xlsx` to the selected output directory.

---

## 3) Core components

## 3.1 `detect_currency_structure(series)`
Classifies currency-like string patterns with regex heuristics:
- `currency_code` (e.g., `USD`)
- `currency_with_description` (e.g., `USD - US Dollar`)
- `currency_numeric` (e.g., `840`)
- `currency_extended` (e.g., `USD (US Dollar)`)
- `empty` when null/empty

Used as a first-pass specialization before generic format inference.

## 3.2 `detect_format(series)`
Infers a coarse type label in this order:
1. currency structures,
2. `numeric` (castable to float),
3. `date` (`YYYY-MM-DD` pattern),
4. `monetary` (`123.45` pattern),
5. `categorical` (unique ratio < 0.1),
6. `text` fallback.

## 3.3 `header_similarity(a, b)`
Computes lexical header similarity using `difflib.SequenceMatcher` ratio.

## 3.4 `semantic_similarity(col1, col2)`
Embeds header names with `all-MiniLM-L6-v2`, then calculates cosine similarity.

## 3.5 `load_sample(path)`
Reads CSV/Parquet with Polars and collects up to `SAMPLE_SIZE` rows.

## 3.6 `convert_to_parquet(file_path)`
Converts CSV input to Parquet for repeated faster local reads.

## 3.7 `analyze(previous_file, refresh_file, output_folder)`
Main orchestration function. Runs all analysis steps and persists report sheets.

## 3.8 `run_ui()`
Tkinter-based launcher for interactive local usage.

---

## 4) Scoring logic for suggested mapping

For each previous column `p`, each refresh column `n` gets a score:

`score = 0.5 * semantic_similarity + 0.3 * header_similarity + 0.2 * format_match`

Where:
- `semantic_similarity` is cosine similarity of header embeddings,
- `header_similarity` is lexical sequence ratio,
- `format_match` is binary (1 if inferred formats match, else 0).

The refresh column with maximum score is selected as the suggestion.

---

## 5) Dependencies

Python packages used directly by the script:
- `polars`
- `pandas`
- `sentence-transformers`
- `scikit-learn`
- `tkinter` (standard library GUI)

Optional utility imports:
- `difflib`, `os`, `threading` (standard library)

> Note: First execution with `SentenceTransformer("all-MiniLM-L6-v2")` may download model artifacts if not already cached.

---

## 6) How to run

Because the script file is named `schema_validator` (without `.py`), run it explicitly with Python:

```bash
python schema_validator
```

Then select:
1. previous dataset,
2. refresh dataset,
3. output folder.

The final report will be created as:

```text
<output_folder>/etl_schema_analysis.xlsx
```

---

## 7) Output sheet definitions

### `column_order`
- `position`: ordinal index
- `previous_column`
- `refresh_column`
- `status`: `OK` / `DIFFERENT`

### `missing_columns`
Columns present in previous but not refresh.

### `extra_columns`
Columns present in refresh but not previous.

### `format_analysis`
- `column`
- `previous_format`
- `refresh_format` (or `missing`)

### `column_similarity`
All pairwise combinations of previous × refresh headers:
- `header_similarity`
- `semantic_similarity`

### `embedding_mapping`
Best suggested mapping per previous column:
- `suggested_refresh_column`
- `confidence_score`
- `previous_format`
- `refresh_format`

---

## 8) Performance and scaling notes

- Pairwise similarity and mapping complexity is **O(P × R)** where:
  - `P` = previous column count,
  - `R` = refresh column count.
- Semantic embedding calls are the most expensive operation.
- Sampling limits row-processing overhead but not header-pair combinatorics.
- CSV→Parquet conversion improves subsequent runs if repeatedly comparing same source files.

---

## 9) Reliability considerations

Current implementation is pragmatic but has known production hardening opportunities:

1. **No explicit exception handling around file selection or analysis runtime**.
2. **Model initialization at import-time** can fail in constrained/no-network environments.
3. **Format inference is heuristic**, not strict typing.
4. **Categorical detection threshold (`< 0.1`) is static** and domain-agnostic.
5. **Potential duplicate mapping assignments** (multiple previous columns can map to same refresh column).

For enterprise usage, consider adding structured logging, robust error surfaces, deterministic mapping constraints, and configurable thresholds.

---

## 10) Recommended production improvements

If evolving this into a production-grade schema governance utility:

- Add CLI mode (`argparse`) alongside GUI mode.
- Make `SAMPLE_SIZE`, regex thresholds, and mapping weights configurable.
- Cache embeddings per unique column header to reduce repeated inference calls.
- Add one-to-one assignment optimization (e.g., Hungarian algorithm) for final mapping.
- Persist run metadata (timestamp, dataset IDs, runtime stats, config snapshot).
- Add test coverage for format detection and scoring.
- Add CI checks (`ruff`, `pytest`) and reproducible dependency locking.

---

## 11) Minimal operator checklist

Before each run:
- Ensure Python environment has required packages.
- Confirm datasets are readable CSV/Parquet.
- Ensure output folder is writable.

After each run:
- Review `missing_columns` and `extra_columns` first for hard breaks.
- Review low `confidence_score` mappings for manual remediation.
- Validate critical finance/date columns from `format_analysis` for drift.


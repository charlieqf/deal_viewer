# Repository Guidelines

## Project Structure & Module Organization
The root of `dome1` hosts runnable data workflows such as `ABN2024.py`, `ABN2025.py`, `fxwj2023.py`, and `stbg_2025.py`. Supporting configuration lives in `config.py`, which centralizes FTP paths and credentials. Generated artifacts land in `abn_file_cache/` (created on demand) and durable reference documents sit under `file_cache/`; keep cached output out of version control when possible. `last_updated_product` holds the most recent sync timestamp and should be updated programmatically.

## Build, Test, and Development Commands
Use Python 3.10+ with a virtual environment: `python -m venv .venv && source .venv/bin/activate` (or `Scripts\\activate` on Windows). Install runtime dependencies with `pip install pandas pymssql pyodbc pypinyin requests python-dateutil chardet selenium` and add any new modules to a `requirements.txt`. Run individual pipelines with `python ABN2025.py`, `python fxwj2023.py`, etc.; add CLI flags rather than hard-coding if you introduce new behaviour.

## Coding Style & Naming Conventions
Follow PEP 8 with 4-space indentation and snake_case for functions and variables. Maintain the existing module naming pattern (`ABNYYYY.py`, `fxwjYYYY.py`) when cloning yearly workflows, and co-locate helper functions near their primary script until a shared utility module is extracted. Keep secrets out of source—read from environment variables (see `config.py`) and fallback defaults only for local testing.

## Testing Guidelines
Prefer `pytest` for new automated coverage and place tests in a `tests/` directory that mirrors the module layout. Mock network, FTP, and database calls so tests run offline, and validate caching logic by asserting on files created within a temporary directory. `python test.py` currently performs a manual Selenium smoke-check; expand or replace it with scripted browser tests only when they can run headlessly in CI.

## Commit & Pull Request Guidelines
Write commits in the imperative mood (“Add 2025 ABN download automation”) and keep related changes together. Each pull request should list affected scripts, note required environment variables, and include sample output paths (e.g., `/DealViewer/TrustAssociatedDoc/ProductCreditRatingFiles`). Attach logs or screenshots when UI automation changes behaviour, and update these guidelines whenever you alter workflow conventions.

## Security & Configuration Tips
Store FTP and database credentials via environment variables before running scripts (`export FTP1_PASS=...`) and avoid committing machine-specific values. Rotate passwords in `config.py` immediately if they appear in logs, and ensure new scripts respect the shared `PATHS` map so downstream systems receive files in the expected locations.

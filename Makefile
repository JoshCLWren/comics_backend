PY_VERSION = 3.12
APP_MODULE = main:app

# ------------------------------------
# color palette
# ------------------------------------
RESET = \033[0m
COLOR_HEADER = \033[1;36m
COLOR_STEP = \033[0;34m
COLOR_SKIP = \033[0;33m
COLOR_DONE = \033[0;32m

# ------------------------------------
# friendly logging helpers
# ------------------------------------
define log_header
	if [ -n "$(strip $(2))" ]; then \
		printf "\n$(COLOR_HEADER)🌟 [%s/%s] %s$(RESET)\n" "$(2)" "$(3)" "$(1)"; \
	else \
		printf "\n$(COLOR_HEADER)🌟 %s$(RESET)\n" "$(1)"; \
	fi
endef

define log_step
	printf "$(COLOR_STEP)   • %s$(RESET)\n" "$(1)"
endef

define log_skip
	printf "$(COLOR_SKIP)   ⚠️  %s (skipped)$(RESET)\n" "$(1)"
endef

define log_done
	printf "$(COLOR_DONE)   ✅ %s$(RESET)\n" "$(1)"
endef

# ------------------------------------
# install uv only if not already found
# ------------------------------------
install-uv:
	@$(call log_header,Install uv CLI,1,5)
	@if ! command -v uv >/dev/null 2>&1; then \
		$(call log_step,Downloading uv installer); \
		curl -LsSf https://astral.sh/uv/install.sh | sh; \
	else \
		$(call log_skip,uv already installed); \
	fi

# ------------------------------------
# create pyproject if not exists
# ------------------------------------
init:
	@$(call log_header,Initialize project scaffolding,2,5)
	@if [ ! -f pyproject.toml ]; then \
		$(call log_step,Creating pyproject.toml via uv init); \
		uv init; \
	else \
		$(call log_skip,pyproject.toml already present); \
	fi

	@if ! uv python list | grep -q " $(PY_VERSION)"; then \
		$(call log_step,Installing Python $(PY_VERSION)); \
		uv python install $(PY_VERSION); \
	else \
		$(call log_skip,Python $(PY_VERSION) already installed); \
	fi

	@if ! uv python pin | grep -q "$(PY_VERSION)"; then \
		$(call log_step,Pinning Python $(PY_VERSION)); \
		uv python pin $(PY_VERSION); \
	else \
		$(call log_skip,Python already pinned); \
	fi

	@if [ ! -d .venv ]; then \
		$(call log_step,Creating virtual environment); \
		uv venv; \
	else \
		$(call log_skip,.venv already exists); \
	fi

# ------------------------------------
# install dependencies safely
# ------------------------------------
deps:
	@$(call log_header,Ensure runtime dependencies,3,5)
	@if ! grep -q 'fastapi' pyproject.toml; then \
		$(call log_step,Adding fastapi dependency); \
		uv add fastapi; \
	else \
		$(call log_skip,fastapi already listed); \
	fi

	@if ! grep -q 'uvicorn' pyproject.toml; then \
		$(call log_step,Adding uvicorn[standard] dependency); \
		uv add "uvicorn[standard]"; \
	else \
		$(call log_skip,uvicorn already listed); \
	fi

# ------------------------------------
# full setup, safe to run repeatedly
# ------------------------------------
setup: install-uv init deps migrate import-data
	@$(call log_header,Setup complete)
	@$(call log_done,Environment ready to go)

# ------------------------------------
# run the dev server
# ------------------------------------
run:
	@$(call log_header,Run development server)
	@$(call log_step,Launching uvicorn for $(APP_MODULE))
	uv run uvicorn $(APP_MODULE) --reload --host 0.0.0.0 \
		--reload-exclude "collection_images" \
		--reload-exclude "collection_images/*" \
		--reload-exclude "collection_images/**"

# ------------------------------------
# database helpers
# ------------------------------------
migrate:
	@$(call log_header,Apply Alembic migrations,4,5)
	@$(call log_step,Bringing schema up to head)
	uv run alembic upgrade head

import-data:
	@$(call log_header,Import CSV data into SQLite,5,5)
	@$(call log_step,Refreshing local database)
	uv run python database/build_library.py

# ------------------------------------
# linting helpers
# ------------------------------------
lint:
	@$(call log_header,Run lint checks)
	@$(call log_step,Running Ruff static analysis)
	uv run ruff check .
	@$(call log_step,Running Pyright type checking)
	uv run mypy .

lint-fix:
	@$(call log_header,Autofix lint issues)
	@$(call log_step,Running Ruff autofix pass)
	uv run ruff check --fix .
	@$(call log_step,Applying Ruff formatter)
	uv run ruff format .

# ------------------------------------
# testing helpers
# ------------------------------------
test:
	@$(call log_header,Run tests with coverage guard)
	@$(call log_step,Executing pytest with 95% minimum coverage)
	uv run pytest --cov=. --cov-report=term-missing --cov-fail-under=93


# ------------------------------------
# clean workspace
# ------------------------------------
clean:
	@$(call log_header,Clean workspace artifacts)
	@$(call log_step,Removing generated files)
	@rm -rf .venv
	@rm -f pyproject.toml
	@rm -rf src
	@rm -rf __pycache__

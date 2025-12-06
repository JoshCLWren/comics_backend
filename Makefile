PY_VERSION = 3.12
APP_MODULE = main:app

# ------------------------------------
# install uv only if not already found
# ------------------------------------
install-uv:
	@if ! command -v uv >/dev/null 2>&1; then \
		echo "Installing uv..."; \
		curl -LsSf https://astral.sh/uv/install.sh | sh; \
	else \
		echo "uv already installed"; \
	fi

# ------------------------------------
# create pyproject if not exists
# ------------------------------------
init:
	@if [ ! -f pyproject.toml ]; then \
		echo "Initializing project..."; \
		uv init; \
	else \
		echo "pyproject.toml already exists, skipping uv init"; \
	fi

	@if ! uv python list | grep -q " $(PY_VERSION)"; then \
		echo "Installing Python $(PY_VERSION)..."; \
		uv python install $(PY_VERSION); \
	else \
		echo "Python $(PY_VERSION) already installed"; \
	fi

	@if ! uv python pin | grep -q "$(PY_VERSION)"; then \
		echo "Pinning Python $(PY_VERSION)..."; \
		uv python pin $(PY_VERSION); \
	else \
		echo "Python already pinned"; \
	fi

	@if [ ! -d .venv ]; then \
		echo "Creating virtual environment..."; \
		uv venv; \
	else \
		echo ".venv already exists, skipping venv creation"; \
	fi

# ------------------------------------
# install dependencies safely
# ------------------------------------
deps:
	@if ! grep -q 'fastapi' pyproject.toml; then \
		echo "Adding fastapi..."; \
		uv add fastapi; \
	else \
		echo "fastapi already in pyproject.toml"; \
	fi

	@if ! grep -q 'uvicorn' pyproject.toml; then \
		echo "Adding uvicorn[standard]..."; \
		uv add "uvicorn[standard]"; \
	else \
		echo "uvicorn already in pyproject.toml"; \
	fi

# ------------------------------------
# full setup, safe to run repeatedly
# ------------------------------------
setup: install-uv init deps
	@echo ""
	@echo "Setup complete."

# ------------------------------------
# run the dev server
# ------------------------------------
run:
	uv run uvicorn $(APP_MODULE) --reload

# ------------------------------------
# clean workspace
# ------------------------------------
clean:
	rm -rf .venv
	rm -f pyproject.toml
	rm -rf src
	rm -rf __pycache__

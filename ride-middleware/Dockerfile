# ONE image for every Python service (Person 1). Each service picks its program with `command:`.
#   - small base, pinned deps, no build tools left behind
#   - runs as a non-root user (uid 10001); compose adds read_only, no-new-privileges, cap_drop ALL
FROM python:3.12-slim AS base
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt \
 && useradd --uid 10001 --user-group --no-create-home --shell /usr/sbin/nologin app
COPY common/ common/
COPY db/ db/
COPY services/ services/
COPY monitoring/__init__.py monitoring/check_counts.py monitoring/
USER 10001:10001
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=4 CMD ["python", "-m", "common.health"]
CMD ["python", "-m", "common.health"]

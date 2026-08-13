FROM --platform=linux/amd64 ghcr.io/seleniumhq/standalone-chrome@sha256:c41da59363c0a9dcb4364b4da5e7d35bd50870a7125c91936c96faf3740efb90

USER root

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        curl \
        freetds-dev \
        gnupg \
        libxml2-dev \
        libxslt1-dev \
        pkg-config \
        python3.10 \
        python3.10-dev \
        python3.10-venv \
        unixodbc \
        unixodbc-dev \
        zlib1g-dev \
    && curl -fsSLo /tmp/packages-microsoft-prod.deb \
        https://packages.microsoft.com/config/ubuntu/24.04/packages-microsoft-prod.deb \
    && dpkg -i /tmp/packages-microsoft-prod.deb \
    && rm -f /tmp/packages-microsoft-prod.deb \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

RUN python3.10 -m venv /opt/dealviewer-venv

COPY requirements.lock /tmp/requirements.lock
RUN /opt/dealviewer-venv/bin/python -m pip install --no-cache-dir \
        --requirement /tmp/requirements.lock \
    && rm -f /tmp/requirements.lock

RUN install -d -o 1200 -g 1201 /opt/dealviewer /state /logs
COPY --chown=1200:1201 app/ /opt/dealviewer/

ENV PATH=/opt/dealviewer-venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOME=/tmp/home \
    CHROME_BIN=/usr/bin/google-chrome \
    CHROMEDRIVER=/usr/bin/chromedriver \
    OPENSSL_CONF=/opt/dealviewer/openssl-sql-legacy.cnf \
    DEALVIEWER_SECRETS_FILE=/run/dealviewer/secrets.json

WORKDIR /opt/dealviewer
USER 1200:1201

ENTRYPOINT ["python", "/opt/dealviewer/run_crawler.py"]
CMD ["preflight"]

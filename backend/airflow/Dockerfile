FROM apache/airflow:3.1.6

USER root

# 1. Instalamos las dependencias
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    procps \
    libpq-dev \
    gcc \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. TRUCO DE SENIOR: Detectar JAVA_HOME dinámicamente
# Buscamos dónde se instaló java y subimos dos niveles para obtener el HOME
RUN JP=$(readlink -f $(which java)) && \
    JH=${JP%/bin/java} && \
    echo "export JAVA_HOME=$JH" >> /etc/profile && \
    ln -s "$JH" /usr/lib/jvm/default-java

# 3. Seteamos las variables de entorno de forma genérica
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
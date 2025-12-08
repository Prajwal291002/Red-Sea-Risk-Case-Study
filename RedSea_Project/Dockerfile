FROM python:3.10-slim-bookworm

# 1. Install Java 17 & System Tools
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless procps curl gnupg2 ca-certificates lsb-release && \
    apt-get clean

# 2. Install SQL Server Drivers (Debian 12 Fix)
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
RUN echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev

# 3. Env Variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV SPARK_HOME=/usr/local/lib/python3.10/site-packages/pyspark

# 4. Setup App
RUN mkdir -p $DAGSTER_HOME
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /opt/dagster/app
COPY . .

EXPOSE 3000 8888

CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-f", "jobs/repo.py"]
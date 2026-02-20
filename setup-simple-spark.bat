@echo off
echo =====================================================
echo Setting up Simple Spark Environment for Practice
echo =====================================================
echo.

REM Check if Docker is running
echo Checking Docker status...
docker info >nul 2>&1
if errorlevel 1 (
    echo ERROR: Docker is not running properly.
    pause
    exit /b 1
)

echo Docker is running! Starting simple setup...
echo.

REM Clean up any existing containers
docker stop spark-lab >nul 2>&1
docker rm spark-lab >nul 2>&1

echo Starting Ubuntu container...
docker run -d -it --name spark-lab -p 4040:4040 ubuntu:22.04 bash

echo Container started! Installing basic tools...
docker exec spark-lab apt-get update
docker exec spark-lab apt-get install -y openjdk-8-jdk wget curl

echo Installing Spark...
docker exec spark-lab bash -c "cd /opt && wget -q https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz"
docker exec spark-lab bash -c "cd /opt && tar -xzf spark-3.4.1-bin-hadoop3.tgz && mv spark-3.4.1-bin-hadoop3 spark"

echo Setting up environment...
docker exec spark-lab bash -c "echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> ~/.bashrc"
docker exec spark-lab bash -c "echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc"
docker exec spark-lab bash -c "echo 'export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin' >> ~/.bashrc"

echo.
echo =====================================================
echo SUCCESS! Simple Spark environment is ready!
echo =====================================================
echo.
echo Container 'spark-lab' is running with Apache Spark
echo To connect: docker exec -it spark-lab bash
echo.
echo Once connected, run: source ~/.bashrc
echo Then you can use: spark-shell
echo.
echo For file operations, we'll simulate HDFS with regular filesystem
echo.
pause

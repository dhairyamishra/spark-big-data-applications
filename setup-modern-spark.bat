@echo off
echo =====================================================
echo Setting up Modern Spark Environment with Pseudo-HDFS
echo =====================================================
echo.

REM Check if Docker is running
echo Checking Docker status...
docker info >nul 2>&1
if errorlevel 1 (
    echo ERROR: Docker is not running properly.
    echo Please ensure Docker Desktop is fully started.
    pause
    exit /b 1
)

echo Docker is running! Proceeding with setup...
echo.

REM Clean up any existing containers
echo Cleaning up any existing containers...
docker stop spark-lab >nul 2>&1
docker rm spark-lab >nul 2>&1

echo Starting Spark container with Ubuntu base...
docker run -d -it ^
    --name spark-lab ^
    -p 4040:4040 ^
    -p 8080:8080 ^
    -p 8081:8081 ^
    ubuntu:22.04 ^
    bash

if errorlevel 1 (
    echo ERROR: Failed to start container.
    pause
    exit /b 1
)

echo Installing Java, Hadoop, and Spark in container...
docker exec spark-lab bash -c "
apt-get update && 
apt-get install -y openjdk-8-jdk wget curl python3 python3-pip && 
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 && 
cd /opt && 
wget -q https://archive.apache.org/dist/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz && 
wget -q https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz && 
tar -xzf hadoop-3.3.4.tar.gz && 
tar -xzf spark-3.4.1-bin-hadoop3.tgz && 
mv hadoop-3.3.4 hadoop && 
mv spark-3.4.1-bin-hadoop3 spark && 
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> ~/.bashrc && 
echo 'export HADOOP_HOME=/opt/hadoop' >> ~/.bashrc && 
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc && 
echo 'export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$SPARK_HOME/bin:\$SPARK_HOME/sbin' >> ~/.bashrc && 
echo 'export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop' >> ~/.bashrc && 
echo 'export SPARK_DIST_CLASSPATH=\$(\$HADOOP_HOME/bin/hadoop classpath)' >> ~/.bashrc
"

if errorlevel 1 (
    echo ERROR: Failed to install Hadoop/Spark.
    pause
    exit /b 1
)

echo Configuring Hadoop for pseudo-distributed mode...
docker exec spark-lab bash -c "
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 && 
export HADOOP_HOME=/opt/hadoop && 
cd \$HADOOP_HOME/etc/hadoop && 
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> hadoop-env.sh && 
cat > core-site.xml << 'EOF'
<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
EOF

cat > hdfs-site.xml << 'EOF'
<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>/tmp/hdfs/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>/tmp/hdfs/datanode</value>
    </property>
</configuration>
EOF
"

echo Setting up SSH and initializing HDFS...
docker exec spark-lab bash -c "
apt-get install -y openssh-server && 
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && 
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && 
chmod 0600 ~/.ssh/authorized_keys && 
service ssh start && 
export HADOOP_HOME=/opt/hadoop && 
export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin && 
mkdir -p /tmp/hdfs/namenode /tmp/hdfs/datanode && 
\$HADOOP_HOME/bin/hdfs namenode -format -force
"

echo Starting Hadoop services...
docker exec spark-lab bash -c "
export HADOOP_HOME=/opt/hadoop && 
export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin && 
service ssh start && 
\$HADOOP_HOME/sbin/start-dfs.sh && 
sleep 5 && 
\$HADOOP_HOME/bin/hdfs dfsadmin -safemode leave
"

echo.
echo =====================================================
echo SUCCESS! Modern Spark/Hadoop environment is ready!
echo =====================================================
echo.
echo Container 'spark-lab' is running with:
echo - Hadoop HDFS (pseudo-distributed mode)
echo - Apache Spark
echo - Java 8
echo.
echo To connect: docker exec -it spark-lab bash
echo Or run: connect-spark.bat
echo.
echo Once connected, source your environment:
echo   source ~/.bashrc
echo.
pause

@echo off
echo =====================================================
echo Setting up local Hadoop/Spark environment using Docker
echo =====================================================
echo.

REM Check if Docker is running
echo Checking Docker status...
docker info >nul 2>&1
if errorlevel 1 (
    echo ERROR: Docker is not running properly.
    echo Please ensure Docker Desktop is fully started.
    echo You can check Docker Desktop system tray icon should be running.
    pause
    exit /b 1
)

echo Docker is running! Proceeding with setup...
echo.

REM Clean up any existing containers with the same name
echo Cleaning up any existing hadoop-lab container...
docker stop hadoop-lab >nul 2>&1
docker rm hadoop-lab >nul 2>&1

echo Pulling Hadoop/Spark Docker image (this may take a few minutes)...
docker pull sequenceiq/hadoop-docker:2.7.0

if errorlevel 1 (
    echo ERROR: Failed to pull Docker image.
    echo Check your internet connection and try again.
    pause
    exit /b 1
)

echo.
echo Starting Hadoop container with necessary ports...
docker run -d ^
    -p 8088:8088 ^
    -p 50070:50070 ^
    -p 8040:8040 ^
    -p 8042:8042 ^
    -p 9000:9000 ^
    --name hadoop-lab ^
    sequenceiq/hadoop-docker:2.7.0

if errorlevel 1 (
    echo ERROR: Failed to start Hadoop container.
    pause
    exit /b 1
)

REM Wait for container to start
echo Waiting for container to initialize (30 seconds)...
timeout /t 30

echo.
echo =====================================================
echo SUCCESS! Local Hadoop/Spark environment is ready!
echo =====================================================
echo.
echo Container started! You can now connect using:
echo   docker exec -it hadoop-lab bash
echo.
echo Or run: connect-hadoop.bat
echo.
echo Web interfaces available at:
echo - Hadoop ResourceManager: http://localhost:8088
echo - HDFS NameNode: http://localhost:50070
echo.
echo The container is running in the background.
echo To stop it later, run: docker stop hadoop-lab
echo.
pause

@echo off
echo =====================================================
echo Connecting to local Hadoop/Spark environment
echo =====================================================
echo.

REM Check if container is running
docker ps | findstr "hadoop-lab" >nul 2>&1
if errorlevel 1 (
    echo ERROR: Hadoop container 'hadoop-lab' is not running.
    echo Please run 'setup-local-hadoop.bat' first.
    pause
    exit /b 1
)

echo Container is running! Connecting...
echo.
echo You are now inside the Hadoop/Spark container.
echo You can run all the lab commands here for practice.
echo.
echo To exit the container, type: exit
echo.
docker exec -it hadoop-lab bash

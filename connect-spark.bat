@echo off
echo =====================================================
echo Connecting to Modern Spark/Hadoop Environment
echo =====================================================
echo.

REM Check if container is running
docker ps | findstr "spark-lab" >nul 2>&1
if errorlevel 1 (
    echo ERROR: Spark container 'spark-lab' is not running.
    echo Please run 'setup-modern-spark.bat' first.
    pause
    exit /b 1
)

echo Container is running! Connecting...
echo.
echo IMPORTANT: Once connected, run these commands first:
echo   source ~/.bashrc
echo   service ssh start
echo.
echo Then you can practice all your lab commands.
echo Type 'exit' to leave the container.
echo.
docker exec -it spark-lab bash

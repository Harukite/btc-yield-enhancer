@echo off
chcp 65001 >nul
title BTC Yield Enhancer - Start

cd /d "%~dp0"

echo ==============================
echo   BTC Yield Enhancer - Start
echo ==============================

echo [1/3] Loading .env...
if not exist ".env" (
    echo [ERROR] .env file not found.
    echo Copy .env.example to .env and fill OKX credentials first.
    pause
    exit /b 1
)
for /f "usebackq tokens=1,2 delims==" %%a in (".env") do set "%%a=%%b"
if "%OKX_API_KEY%"=="" (
    echo [ERROR] OKX_API_KEY is missing in .env
    pause
    exit /b 1
)
if "%OKX_API_SECRET%"=="" (
    echo [ERROR] OKX_API_SECRET is missing in .env
    pause
    exit /b 1
)
if "%OKX_PASSPHRASE%"=="" (
    echo [ERROR] OKX_PASSPHRASE is missing in .env
    pause
    exit /b 1
)
echo   OK

echo [2/3] Finding Python...
set PYTHON=
for %%d in (
    "%~dp0venv\Scripts\python.exe"
    "%~dp0.venv\Scripts\python.exe"
    "%HOMEDRIVE%%HOMEPATH%\.workbuddy\binaries\python\envs\default\Scripts\python.exe"
    "%HOMEDRIVE%%HOMEPATH%\.workbuddy\binaries\python\versions\3.13.12\python.exe"
    "%LocalAppData%\Programs\Python\Python314\python.exe"
    "%LocalAppData%\Programs\Python\Python313\python.exe"
    "%LocalAppData%\Programs\Python\Python312\python.exe"
) do if exist %%d set "PYTHON=%%~f" & goto :found_python
where python >nul 2>&1 && set "PYTHON=python" & goto :found_python
echo [ERROR] Python not found
pause
exit /b 1

:found_python
echo   Using: %PYTHON%
echo.

echo [3/3] Starting Flask...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":5050 " ^| findstr "LISTENING"') do (
    taskkill /F /PID %%a >nul 2>&1
)
timeout /t 2 /nobreak >nul

start /B "" "%PYTHON%" app.py
timeout /t 3 /nobreak >nul

echo   Initializing engine...
for /l %%i in (1,1,20) do (
    >nul 2>&1 curl -s -X POST http://127.0.0.1:5050/btc-enhancer/api/init && (
        echo   Engine initialized
        goto :done
    )
    >nul ping -n 2 127.0.0.1
)
echo   Init timed out. Open http://127.0.0.1:5050/ manually.

:done
start http://127.0.0.1:5050/
echo.
echo Done.
echo Use stop.bat to stop the strategy.
echo ==============================

@echo off
setlocal EnableExtensions EnableDelayedExpansion

rem ================================================================================
rem mini-etcd cluster startup script (Windows CMD)
rem
rem What this script does:
rem 1) Parse startup arguments.
rem 2) Prepare runtime directories under scripts\runtime.
rem 3) Build etcd-kernel jar/classes via Maven.
rem 4) Start N nodes with MiniEtcdNodeLauncher.
rem 5) Keep this window alive to manage node lifecycle (unless --noHold=true).
rem 6) Stop started nodes automatically on graceful exit.
rem ================================================================================

rem ==================== Paths (relative) ====================
set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..\..") do set "PROJECT_ROOT=%%~fI"
set "RUNTIME_DIR=%SCRIPT_DIR%runtime"

rem ==================== Defaults ====================
set "CLUSTER_SIZE=3"
set "HOST=127.0.0.1"
set "BASE_PORT=2379"
set "PROFILE=default"
set "DATA_ROOT="
set "LOG_DIR="
set "ELECTION_TIMEOUT_TICKS=10"
set "HEARTBEAT_TIMEOUT_TICKS=3"
set "SNAPSHOT_TRIGGER_LOG_COUNT=50"
set "NO_HOLD=0"

rem ==================== Parse Args ====================
rem Supported formats:
rem - --key=value
rem - --key value
:parse_args
if "%~1"=="" goto args_done
set "ARG=%~1"
set "ARG_KEY="
set "ARG_VALUE="
for /f "tokens=1,2 delims==" %%A in ("%ARG%") do (
    set "ARG_KEY=%%~A"
    set "ARG_VALUE=%%~B"
)

if /I "!ARG_KEY!"=="--noHold" (
    if "!ARG_VALUE!"=="" (
        set "NO_HOLD=1"
    ) else if /I "!ARG_VALUE!"=="true" (
        set "NO_HOLD=1"
    ) else if /I "!ARG_VALUE!"=="false" (
        set "NO_HOLD=0"
    ) else (
        echo [mini-etcd] --noHold only supports true or false.
        set "FAIL_MESSAGE=invalid --noHold value."
        goto :fail_and_exit
    )
    shift
    goto parse_args
)

if "!ARG_VALUE!"=="" (
    if "%~2"=="" (
        echo [mini-etcd] argument value is missing: !ARG_KEY!
        set "FAIL_MESSAGE=argument value is missing."
        goto :fail_and_exit
    )
    set "ARG_VALUE=%~2"
    shift
)

if /I "!ARG_KEY!"=="--clusterSize" (
    set "CLUSTER_SIZE=!ARG_VALUE!"
) else if /I "!ARG_KEY!"=="--host" (
    set "HOST=!ARG_VALUE!"
) else if /I "!ARG_KEY!"=="--basePort" (
    set "BASE_PORT=!ARG_VALUE!"
) else if /I "!ARG_KEY!"=="--profile" (
    set "PROFILE=!ARG_VALUE!"
) else if /I "!ARG_KEY!"=="--dataRoot" (
    set "DATA_ROOT=!ARG_VALUE!"
) else if /I "!ARG_KEY!"=="--logDir" (
    set "LOG_DIR=!ARG_VALUE!"
) else if /I "!ARG_KEY!"=="--electionTimeoutTicks" (
    set "ELECTION_TIMEOUT_TICKS=!ARG_VALUE!"
) else if /I "!ARG_KEY!"=="--heartbeatTimeoutTicks" (
    set "HEARTBEAT_TIMEOUT_TICKS=!ARG_VALUE!"
) else if /I "!ARG_KEY!"=="--snapshotTriggerLogCount" (
    set "SNAPSHOT_TRIGGER_LOG_COUNT=!ARG_VALUE!"
) else (
    echo [mini-etcd] unknown argument: %~1
    set "FAIL_MESSAGE=unknown argument."
    goto :fail_and_exit
)
shift
goto parse_args

:args_done
rem Basic numeric and required argument validation.
call :validate_positive_number "%CLUSTER_SIZE%" "clusterSize"
if errorlevel 1 (
    set "FAIL_MESSAGE=invalid clusterSize."
    goto :fail_and_exit
)
call :validate_positive_number "%BASE_PORT%" "basePort"
if errorlevel 1 (
    set "FAIL_MESSAGE=invalid basePort."
    goto :fail_and_exit
)
if "%PROFILE%"=="" (
    echo [mini-etcd] profile must not be empty.
    set "FAIL_MESSAGE=profile must not be empty."
    goto :fail_and_exit
)

set "PROFILE_RUNTIME_DIR=%RUNTIME_DIR%\profiles\%PROFILE%"
set "STATE_DIR=%PROFILE_RUNTIME_DIR%\state"
set "PID_FILE=%STATE_DIR%\cluster.pids"
if "%DATA_ROOT%"=="" set "DATA_ROOT=%PROFILE_RUNTIME_DIR%\data"
if "%LOG_DIR%"=="" set "LOG_DIR=%PROFILE_RUNTIME_DIR%\logs"

echo [mini-etcd] startup config: profile=%PROFILE%, clusterSize=%CLUSTER_SIZE%, host=%HOST%, basePort=%BASE_PORT%

rem ==================== Prepare Dirs ====================
if not exist "%STATE_DIR%" mkdir "%STATE_DIR%"
if not exist "%DATA_ROOT%" mkdir "%DATA_ROOT%"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

rem If a stale PID file exists, stop old processes first.
if exist "%PID_FILE%" (
    echo [mini-etcd] stale pid file found, cleaning previous processes first...
    call :stop_nodes_by_pid_file
)
rem Defensive cleanup for orphaned launcher java processes.
call :stop_residual_launcher_processes

cd /d "%PROJECT_ROOT%"
echo [mini-etcd] building kernel module...
call mvn -q -pl etcd-kernel -am -DskipTests package
if errorlevel 1 (
    echo [mini-etcd] build failed.
    set "FAIL_MESSAGE=kernel module build failed."
    goto :fail_and_exit
)

rem ==================== Build Peer Endpoints ====================
rem Build peer endpoints text:
rem n1@host:port,n2@host:port,...
set "PEER_ENDPOINTS="
for /L %%I in (1,1,%CLUSTER_SIZE%) do (
    set /a NODE_PORT=%BASE_PORT% + %%I - 1
    set "ENDPOINT_ITEM=n%%I@%HOST%:!NODE_PORT!"
    if "!PEER_ENDPOINTS!"=="" (
        set "PEER_ENDPOINTS=!ENDPOINT_ITEM!"
    ) else (
        set "PEER_ENDPOINTS=!PEER_ENDPOINTS!,!ENDPOINT_ITEM!"
    )
)

type nul > "%PID_FILE%"

rem ==================== Start Nodes ====================
rem Start each node as a background Maven process.
rem Wait for node PID file as startup-ready signal.
pushd "%PROJECT_ROOT%\etcd-kernel"
for /L %%I in (1,1,%CLUSTER_SIZE%) do (
    set /a NODE_PORT=%BASE_PORT% + %%I - 1
    set "NODE_ID=n%%I"
    set "NODE_DATA_DIR=%DATA_ROOT%\!NODE_ID!"
    set "NODE_LOG_FILE=%LOG_DIR%\!NODE_ID!.log"
    set "NODE_PID_FILE=%STATE_DIR%\!NODE_ID!.pid"
    if not exist "!NODE_DATA_DIR!" mkdir "!NODE_DATA_DIR!"
    if exist "!NODE_PID_FILE!" del /f /q "!NODE_PID_FILE!" >nul 2>nul

    set "LAUNCH_ARGS=--nodeId=!NODE_ID! --host=%HOST% --port=!NODE_PORT! --peerEndpoints=!PEER_ENDPOINTS! --dataDir=!NODE_DATA_DIR! --pidFile=!NODE_PID_FILE! --electionTimeoutTicks=%ELECTION_TIMEOUT_TICKS% --heartbeatTimeoutTicks=%HEARTBEAT_TIMEOUT_TICKS% --snapshotTriggerLogCount=%SNAPSHOT_TRIGGER_LOG_COUNT%"
    start "" /b mvn -q -l "!NODE_LOG_FILE!" exec:java -Dexec.mainClass=com.xhj.etcd.kernel.etcd.bootstrap.MiniEtcdNodeLauncher -Dexec.args="!LAUNCH_ARGS!"

    call :wait_for_file "!NODE_PID_FILE!" 40
    if errorlevel 1 (
        echo [mini-etcd] node start timeout: !NODE_ID!, pid file not found.
        call :stop_nodes_by_pid_file
        set "FAIL_MESSAGE=node start timeout."
        goto :fail_and_exit
    )

    set "NODE_PID="
    for /f "usebackq delims=" %%P in ("!NODE_PID_FILE!") do set "NODE_PID=%%P"
    set "NODE_PID=!NODE_PID: =!"
    if "!NODE_PID!"=="" (
        echo [mini-etcd] empty pid from launcher: !NODE_ID!
        call :stop_nodes_by_pid_file
        set "FAIL_MESSAGE=empty pid from launcher."
        goto :fail_and_exit
    )

    >> "%PID_FILE%" echo(!NODE_ID!^|!NODE_PID!^|%HOST%:!NODE_PORT!^|!NODE_LOG_FILE!^|!NODE_DATA_DIR!
    echo [mini-etcd] started !NODE_ID! on %HOST%:!NODE_PORT!, pid=!NODE_PID!
)
popd

echo [mini-etcd] cluster started successfully.
echo [mini-etcd] logs dir: %LOG_DIR%
echo [mini-etcd] state dir: %STATE_DIR%

if "%NO_HOLD%"=="1" goto :success_and_exit
if /I "%MINI_ETCD_NO_HOLD%"=="1" goto :success_and_exit

rem Keep this terminal alive to manage lifecycle.
rem User can press Q to stop all nodes and close the window.
echo [mini-etcd] window must stay open to keep cluster lifecycle managed.
echo [mini-etcd] press Q to stop all nodes and close this window.

:hold_loop
choice /C Q /N /M "[mini-etcd] input Q to stop cluster: "
if errorlevel 1 goto :graceful_shutdown
goto hold_loop

:graceful_shutdown
rem Graceful stop: terminate all nodes listed in PID file.
call :stop_nodes_by_pid_file
goto :success_and_exit

:stop_nodes_by_pid_file
rem Stop nodes recorded in cluster.pids, then remove node pid files.
if not exist "%PID_FILE%" goto :stop_nodes_done
for /f "usebackq tokens=1-5 delims=|" %%A in ("%PID_FILE%") do (
    if not "%%B"=="" (
        taskkill /PID %%B /T /F >nul 2>nul
    )
    if exist "%STATE_DIR%\%%A.pid" del /f /q "%STATE_DIR%\%%A.pid" >nul 2>nul
)
for %%F in ("%STATE_DIR%\*.pid") do (
    if exist "%%~fF" (
        set "ORPHAN_NODE_PID="
        for /f "usebackq delims=" %%P in ("%%~fF") do set "ORPHAN_NODE_PID=%%P"
        if not "!ORPHAN_NODE_PID!"=="" taskkill /PID !ORPHAN_NODE_PID! /T /F >nul 2>nul
        del /f /q "%%~fF" >nul 2>nul
    )
)
del /f /q "%PID_FILE%" >nul 2>nul
:stop_nodes_done
exit /b 0

:stop_residual_launcher_processes
rem Fallback kill: remove leaked Java processes containing MiniEtcdNodeLauncher.
for /f "tokens=2 delims==" %%P in ('wmic process where "Name='java.exe' and CommandLine like '%%MiniEtcdNodeLauncher%%'" get ProcessId /value ^| find "="') do (
    set "RESIDUAL_PID=%%P"
    for /f "tokens=1 delims= " %%Q in ("!RESIDUAL_PID!") do (
        if not "%%Q"=="" taskkill /PID %%Q /T /F >nul 2>nul
    )
)
exit /b 0

:validate_positive_number
rem Utility: validate a positive integer argument.
set "VALUE=%~1"
set "ARG_NAME=%~2"
if "%VALUE%"=="" (
    echo [mini-etcd] %ARG_NAME% must not be empty.
    exit /b 1
)
for /f "delims=0123456789" %%X in ("%VALUE%") do (
    echo [mini-etcd] %ARG_NAME% must be a positive integer.
    exit /b 1
)
if "%VALUE%"=="0" (
    echo [mini-etcd] %ARG_NAME% must be greater than 0.
    exit /b 1
)
exit /b 0

:wait_for_file
rem Utility: wait for a file path to appear within N seconds.
set "WAIT_FILE=%~1"
set "WAIT_SECONDS=%~2"
set /a WAIT_ELAPSED=0
:wait_for_file_loop
if exist "%WAIT_FILE%" exit /b 0
if %WAIT_ELAPSED% GEQ %WAIT_SECONDS% exit /b 1
ping 127.0.0.1 -n 2 >nul
set /a WAIT_ELAPSED=%WAIT_ELAPSED%+1
goto wait_for_file_loop

:fail_and_exit
rem Unified failure exit:
rem - print failure reason
rem - print runtime log/state locations
if not "%FAIL_MESSAGE%"=="" (
    echo [mini-etcd] error: %FAIL_MESSAGE%
)
if not "%LOG_DIR%"=="" (
    echo [mini-etcd] logs dir: %LOG_DIR%
    echo [mini-etcd] node logs: %LOG_DIR%\n1.log, %LOG_DIR%\n2.log, %LOG_DIR%\n3.log
)
if not "%STATE_DIR%"=="" (
    echo [mini-etcd] state dir: %STATE_DIR%
)
if /I "%MINI_ETCD_NO_PAUSE%"=="1" exit /b 1
echo [mini-etcd] press any key to close...
pause >nul
exit /b 1

:success_and_exit
rem Unified success exit.
if /I "%MINI_ETCD_NO_PAUSE%"=="1" exit /b 0
echo [mini-etcd] press any key to close...
pause >nul
exit /b 0

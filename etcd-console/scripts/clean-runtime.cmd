@echo off
setlocal EnableExtensions EnableDelayedExpansion

rem ================================================================================
rem mini-etcd runtime cleanup script (Windows CMD)
rem
rem What this script does:
rem 1) Stop running node processes found from runtime PID files.
rem 2) Kill residual MiniEtcdNodeLauncher java processes as fallback.
rem 3) Remove runtime artifacts:
rem    - data
rem    - logs
rem    - pid/state files
rem
rem Usage:
rem - clean all profiles: clean-runtime.cmd
rem - clean one profile:  clean-runtime.cmd --profile=default
rem ================================================================================

rem ==================== Paths (relative) ====================
set "SCRIPT_DIR=%~dp0"
set "RUNTIME_DIR=%SCRIPT_DIR%runtime"
set "PROFILE="

rem ==================== Parse Args ====================
rem Supported formats:
rem - --profile=value
rem - --profile value
:parse_args
if "%~1"=="" goto args_done
set "ARG=%~1"
set "ARG_KEY="
set "ARG_VALUE="
for /f "tokens=1,2 delims==" %%A in ("%ARG%") do (
    set "ARG_KEY=%%~A"
    set "ARG_VALUE=%%~B"
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

if /I "!ARG_KEY!"=="--profile" (
    set "PROFILE=!ARG_VALUE!"
) else (
    echo [mini-etcd] unknown argument: %~1
    set "FAIL_MESSAGE=unknown argument."
    goto :fail_and_exit
)
shift
goto parse_args

:args_done
echo [mini-etcd] runtime clean usage:
echo [mini-etcd] 1) remove raft data files (runtime\profiles\*\data)
echo [mini-etcd] 2) remove runtime logs (runtime\profiles\*\logs)
echo [mini-etcd] 3) remove pid/state files (runtime\profiles\*\state)

if not exist "%RUNTIME_DIR%" (
    echo [mini-etcd] runtime directory not found: runtime
    goto :success_and_exit
)

rem If no profile is provided, clean all profiles under runtime\profiles.
if "%PROFILE%"=="" (
    echo [mini-etcd] cleaning all runtime artifacts under: runtime
    call :stop_nodes_from_runtime "%RUNTIME_DIR%"
    call :stop_residual_launcher_processes
    rmdir /s /q "%RUNTIME_DIR%" >nul 2>nul
    mkdir "%RUNTIME_DIR%" >nul 2>nul
    goto :success_and_exit
)

set "PROFILE_RUNTIME_DIR=%RUNTIME_DIR%\profiles\%PROFILE%"
if exist "%PROFILE_RUNTIME_DIR%" (
    echo [mini-etcd] cleaning runtime profile: runtime\profiles\%PROFILE%
    call :stop_nodes_by_pid_file "%PROFILE_RUNTIME_DIR%\state\cluster.pids"
    call :stop_residual_launcher_processes
    rmdir /s /q "%PROFILE_RUNTIME_DIR%" >nul 2>nul
) else (
    echo [mini-etcd] profile runtime directory not found: runtime\profiles\%PROFILE%
)
goto :success_and_exit

:stop_nodes_from_runtime
rem Iterate all profile folders and stop nodes by each profile's cluster.pids.
set "TARGET_RUNTIME_DIR=%~1"
if not exist "%TARGET_RUNTIME_DIR%\profiles" exit /b 0
for /d %%D in ("%TARGET_RUNTIME_DIR%\profiles\*") do (
    call :stop_nodes_by_pid_file "%%~fD\state\cluster.pids"
)
exit /b 0

:stop_nodes_by_pid_file
rem Stop all pids listed in a specific cluster.pids file.
set "TARGET_PID_FILE=%~1"
if not exist "%TARGET_PID_FILE%" exit /b 0
for /f "usebackq tokens=1-5 delims=|" %%A in ("%TARGET_PID_FILE%") do (
    if not "%%B"=="" taskkill /PID %%B /T /F >nul 2>nul
)
del /f /q "%TARGET_PID_FILE%" >nul 2>nul
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

:fail_and_exit
rem Unified failure exit.
if not "%FAIL_MESSAGE%"=="" echo [mini-etcd] error: %FAIL_MESSAGE%
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

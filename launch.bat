@echo off
setlocal
set "TARGET=%~dp0scripts\cli\launch.bat"
if not exist "%TARGET%" (
  echo ERROR: "%TARGET%" not found
  exit /b 1
)
call "%TARGET%"
endlocal
exit /b %ERRORLEVEL%

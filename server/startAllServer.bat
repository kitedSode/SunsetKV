@echo off
go build
for %%a in (0 1 2) do start startOneServer.bat %%a
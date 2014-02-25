@echo off

setlocal

cd /D %~dp0

set PATH_THRIFT=..\..\packages\Thrift.0.9.1.3\tools\thrift-0.9.1.exe

%PATH_THRIFT% -gen csharp -I . impala\ImpalaService.thrift 
%PATH_THRIFT% -gen csharp -I . impala\beeswax.thrift 
%PATH_THRIFT% -gen csharp -I . impala\Status.thrift 
%PATH_THRIFT% -gen csharp -I . impala\cli_service.thrift 
%PATH_THRIFT% -gen csharp -I . hive_metastore.thrift 
%PATH_THRIFT% -gen csharp -I . fb303.thrift




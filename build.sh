#!/usr/bin/env bash
dotnet publish -c Release -r linux-x64 --self-contained false
zip -rXq SnowflakeConnector.zip ./bin/Release/net10.0/linux-x64/publish/*
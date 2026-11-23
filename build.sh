#!/usr/bin/env bash
dotnet clean -c Release
dotnet publish -c Release -r linux-x64 --no-self-contained
zip -jq SnowflakeConnector.zip ./bin/Release/net10.0/linux-x64/publish/*
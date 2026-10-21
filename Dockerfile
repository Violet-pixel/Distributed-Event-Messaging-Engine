FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY . .
RUN dotnet publish src/Brado.Server/Brado.Server.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .
RUN mkdir -p /app/data && chown -R $APP_UID /app/data
USER $APP_UID
ENV ASPNETCORE_URLS=http://0.0.0.0:5080
EXPOSE 5080 7420
VOLUME ["/app/data"]
ENTRYPOINT ["dotnet", "Brado.Server.dll"]

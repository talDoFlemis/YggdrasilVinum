# Use the official .NET SDK image for building
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

# Copy project files and restore dependencies
COPY *.sln ./
COPY YggdrasilVinum/*.csproj ./YggdrasilVinum/
COPY YggdrasilVinum.Tests/*.csproj ./YggdrasilVinum.Tests/
RUN dotnet restore

# Copy source code and build
COPY . .
WORKDIR /src/YggdrasilVinum
RUN dotnet build -c Release -o /app/build

# Publish the application
RUN dotnet publish -c Release -o /app/publish --no-restore

# Use the official .NET runtime image for the final stage
FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS final
WORKDIR /app

# Copy the published application first
COPY --from=build /app/publish .

# Create necessary directories for runtime
RUN mkdir -p /app/logs /app/storage /app/data

# Copy the wines.csv to a different location to avoid conflicts
COPY --from=build /src/YggdrasilVinum/Data/wines.csv /app/data/

# Copy appsettings.json from the YggdrasilVinum directory
COPY --from=build /src/YggdrasilVinum/appsettings.json .

# Set environment variables
ENV DOTNET_ENVIRONMENT=Production
ENV ASPNETCORE_ENVIRONMENT=Production

# Create non-root user for security
RUN adduser --disabled-password --gecos '' appuser && \
    chown -R appuser:appuser /app
USER appuser

# Set the entrypoint with the wine data file path as an argument
ENTRYPOINT ["dotnet", "YggdrasilVinum.dll"]
CMD ["/app/data/wines.csv"]

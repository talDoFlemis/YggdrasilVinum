# YggdrasilVinum

## Overview

YggdrasilVinum is a .NET application for managing and analyzing wine data.

## Development

This project is built with .NET 9.0.

### Prerequisites

- .NET 9.0 SDK
- JetBrains Rider or Visual Studio 2022

### Building the Project

```bash
dotnet restore YggdrasilVinum.sln
dotnet build YggdrasilVinum.sln
```

### Running Tests

```bash
dotnet test YggdrasilVinum.sln
```

## Continuous Integration

This project uses GitHub Actions for CI/CD. The following checks run on every PR and commit to master:

1. **Build**: Ensures the solution compiles successfully.
2. **Tests**: Runs all unit, integration, and simulation tests.
3. **Format Check**: Verifies code formatting using both `dotnet format` and ReSharper's command-line tools.
4. **Code Quality**: Runs code inspection to identify potential issues.

### Local Code Formatting

You can format your code locally using:

```bash
# Using .NET format tool
dotnet tool install -g dotnet-format
dotnet format YggdrasilVinum.sln

# Using ReSharper command-line tools
dotnet tool install -g JetBrains.ReSharper.GlobalTools
jb cleanupcode YggdrasilVinum.sln --profile="Built-in: Full Cleanup"
```

The project uses `.editorconfig` to maintain consistent code style. JetBrains Rider automatically applies these settings.
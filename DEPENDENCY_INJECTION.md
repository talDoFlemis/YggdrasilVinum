# Injeção de Dependência (DI) e Inversão de Controle (IoC) - YggdrasilVinum

## Implementação Realizada

### 1. Configuração dos Serviços

Foi implementado o padrão de Injeção de Dependência utilizando `Microsoft.Extensions.DependencyInjection`. A
configuração foi centralizada na classe `ServiceCollectionExtensions.cs`.

#### Principais Componentes Configurados:

- **IFileManager**: Gerenciamento de arquivos (singleton)
- **IBufferManager**: Gerenciamento de buffer LRU (singleton)
- **IBPlusTreeIndex**: Índice B+ Tree (singleton)
- **Database**: Banco de dados principal (singleton)
- **IWineProcessor**: Processamento de dados de vinho (singleton)
- **Command Processors**: Processadores de comandos (transient)

### 2. Classe de Configuração

```csharp
public class ApplicationConfiguration
{
    public string StoragePath { get; set; } = "./storage";
    public ulong HeapSizeInBytes { get; set; } = 40 * 1024 * 1024;
    public ulong PageSizeInBytes { get; set; } = 4096;
    // ... outros parâmetros
}
```

### 3. Refatoração do Program.cs

O método `RunApplication` foi completamente refatorado para usar o container DI:

#### Antes (Acoplamento Alto):

```csharp
var fileManager = ApplicationFactory.CreateFileManager(/*...*/);
var bufferManager = ApplicationFactory.CreateBufferManager(/*...*/);
var bPlusTree = ApplicationFactory.CreateBPlusTree(/*...*/);
// Criação manual de todas as dependências...
```

#### Depois (Baixo Acoplamento):

```csharp
var services = new ServiceCollection();
services.ConfigureApplicationServices(configuration);
var serviceProvider = services.BuildServiceProvider();

// Dependências são injetadas automaticamente
var database = serviceProvider.GetRequiredService<Database>();
```

## Benefícios Obtidos

### 1. **Desacoplamento**

- As classes não precisam saber como criar suas dependências
- Mudanças nas dependências não afetam as classes consumidoras

### 2. **Manutenibilidade**

- Configuração centralizada em um único local
- Facilita mudanças na arquitetura da aplicação

### 3. **Testabilidade**

- Fácil substituição de dependências por mocks/stubs em testes
- Exemplo fornecido em `DependencyInjectionExample.cs`

### 4. **Gerenciamento de Ciclo de Vida**

- Singleton: Uma instância por toda a aplicação
- Transient: Nova instância a cada requisição
- Controle automático de disposição de recursos

## Como Usar

### Configuração Normal:

```csharp
var configuration = new ApplicationConfiguration();
var services = new ServiceCollection();
services.ConfigureApplicationServices(configuration);
var serviceProvider = services.BuildServiceProvider();
```

### Para Testes:

```csharp
var testProvider = DependencyInjectionExample.ConfigureTestServices(testConfig);
var database = DependencyInjectionExample.GetTestService<Database>(testProvider);
```

## Estrutura de Arquivos

```
YggdrasilVinum/
├── Services/
│   ├── ServiceCollectionExtensions.cs  # Configuração DI
│   ├── ApplicationConfiguration.cs     # Classe de configuração
│   └── ...
├── DependencyInjectionExample.cs       # Exemplo para testes
└── Program.cs                          # Aplicação principal refatorada
```

## Próximos Passos

1. **Implementar Interfaces de Repositories**: Para facilitar ainda mais os testes
2. **Configuration Provider**: Usar `IConfiguration` para parâmetros externos
3. **Logging DI**: Injetar `ILogger<T>` ao invés de usar `Log.ForContext<T>()`
4. **Health Checks**: Adicionar verificações de saúde dos serviços

Esta implementação segue as melhores práticas do .NET e torna o sistema muito mais flexível e testável.

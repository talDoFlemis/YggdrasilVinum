# TRABALHO I - ÍNDICES

## 1. Aspectos Gerais

Um banco de dados armazenado em disco possui uma relação que salva as informações relativas aos vinhos produzidos em vinícolas. A Figura 1 apresenta o esquema da relação **Vinho**. Normalmente, a carga de trabalho que envolve essa relação realiza uma consulta considerando o ano no qual a uva utilizada para produzir o vinho foi colhida. Assim, o trabalho consiste em implementar um índice para essa relação, sendo que a chave de busca é o atributo `ano_colheita`, que é um número inteiro.

### Figura 1: Relação a ser indexada

```
+----------------------------------+
|              Vinho               |
+----------------------------------+
| PK  vinho_id                     |
|     rotulo: string               |
|     ano_colheita: integer        |
|     tipo: enum <tinto|
|           branco, rosé>          |
+----------------------------------+
```

Para este trabalho, deverão ser utilizados os dados presentes na instância da relação **Vinho** que está no arquivo `vinhos.csv` que será publicado junto a esse documento.

## 2. Requisitos

Cada equipe de, no máximo, dois alunos implementará um índice usando árvore B+. Considere que a relação **Vinho** está armazenada em um arquivo do disco no qual cada linha (registro) desse arquivo é uma página do banco de dados.

A seguir, apresentamos a especificação da interface que deve ser implementada:

- Busca por Igualdade;
- Inserção;

## 3. Implementação

Nesse trabalho, um banco de dados será mapeado em disco de modo que dois arquivos o representem:
1. O arquivo de dados condizente à relação, contendo um registro por linha, em texto;
2. O segundo represente o arquivo de índice, contendo as informações do índice.

Deve-se utilizar uma linha do arquivo como armazenamento do nó do índice, com as referências aos outros nós do índice sendo representadas por referências a outros registros do arquivo de índice.

Os seguintes pontos são obrigatórios:

- A implementação deverá ser feita somente nas linguagens **C, C++, C# ou Objective-C**. Nada além disso!

- O buffer do SGBD é de apenas dois frames, onde um pode ser usado para manter uma página de dados (registro do arquivo que se refere à tabela) e outro uma página de índice (registro do arquivo que se refere aos nós da árvore). Portanto seu programa deve manter em memória apenas uma única página de dados e uma página de índice por vez. **Não é permitido manter todos os dados em memória simultaneamente**.

O arquivo **Main** será o responsável por realizar chamadas às funcionalidades implementadas. Para tanto, um único arquivo de entrada `in.txt` será fornecido. Nesse arquivo de entrada, estarão as operações de índice que devem ser realizadas, sendo que cada linha desse arquivo é composta por uma das alternativas seguintes:

```
INC:x
BUS=:x
```

De maneira intuitiva, `INC`, `BUS=`, representam, respectivamente, a operação, no índice, de inclusão e busca por igualdade. Ademais, em ambas as operações, `x` representa o inteiro a ser usado nas respectivas operações.

A primeira linha do arquivo de entrada, antes das operações em si, irá indicar a quantidade de filhos de um nó para árvore B+. A sintaxe dessa linha é a seguinte:

```
FLH/<quantidade de filhos que um nó pode ter>
```

Ao final um arquivo de saída nomeado de `out.txt` deve ser gerado. A linha inicial do arquivo deve ser igual à primeira linha do arquivo de entrada e, após cada operação do arquivo de entrada ser realizada, deve ser incluída uma linha no arquivo de saída da seguinte forma:

```
INC:x/<quantidade de tuplas inseridas>
BUS=:x/<quantidade de tuplas selecionadas>
```

Por fim, a última linha do arquivo de saída do índice em árvore B+ deve ser `H/<altura da árvore>`.

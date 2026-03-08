# Como Contribuir

Obrigado pelo interesse em contribuir com o `sintetizador-newave`! Este guia descreve
o fluxo de trabalho e as convencoes adotadas pelo projeto.

## Configuracao do Ambiente

Clone o repositorio e instale as dependencias de desenvolvimento:

```bash
git clone https://github.com/rjmalves/sintetizador-newave.git
cd sintetizador-newave
uv sync --all-extras
```

> **Requisitos**: Python 3.10+ e [uv](https://docs.astral.sh/uv/) instalados.

## Pre-commit Hooks

O projeto utiliza [pre-commit](https://pre-commit.com/) para garantir qualidade de codigo
antes de cada commit. Apos instalar as dependencias de desenvolvimento, ative os hooks:

```bash
pre-commit install
```

Os hooks configurados em `.pre-commit-config.yaml` sao executados automaticamente a cada
`git commit`:

| Hook          | O que faz                                                 |
| ------------- | --------------------------------------------------------- |
| `ruff`        | Lint automatico com correcao (`--fix`) seguindo PEP8      |
| `ruff-format` | Formatacao automatica de codigo                           |
| `mypy`        | Verificacao de tipos em modo estrito no diretorio `./app` |

Para executar todos os hooks manualmente sem realizar um commit:

```bash
pre-commit run --all-files
```

## Executando Testes

Execute a suite de testes com pytest:

```bash
uv run pytest ./tests
```

Para executar com relatorio de cobertura de codigo:

```bash
uv run pytest ./tests --cov=app
```

## Verificacao de Tipos

A tipagem estatica e verificada pelo mypy em modo estrito:

```bash
uv run mypy ./app
```

A verificacao e executada automaticamente pelo pre-commit hook e pelo CI. Todo codigo
submetido ao repositorio deve passar sem erros de tipo.

## Estilo de Codigo

O projeto segue as convencoes abaixo:

- **PEP8**: Estilo de codigo segundo as diretrizes do guia oficial do Python,
  verificado e corrigido automaticamente pelo `ruff`.
- **Comprimento de linha**: `line-length = 80` (configurado em `pyproject.toml`).
- **Tipagem estatica obrigatoria**: Todas as funcoes e metodos publicos devem ter
  anotacoes de tipo completas. Nao utilize `Any` exceto quando estritamente necessario.
- **Ferramentas**: O projeto usa exclusivamente `ruff` para lint e formatacao.
  Nao utilize `black`, `flake8` ou `isort` separadamente -- o `ruff` os substitui.

## Fluxo de Contribuicao

1. **Fork** o repositorio no GitHub.
2. **Crie um branch** a partir de `main` com um nome descritivo:
   ```bash
   git checkout -b feat/minha-nova-funcionalidade
   ```
3. **Implemente** as alteracoes, adicionando testes quando aplicavel.
4. **Verifique** que os hooks e testes passam:
   ```bash
   pre-commit run --all-files
   uv run pytest ./tests
   ```
5. **Faca commit** seguindo as convencoes de commit descritas abaixo.
6. **Envie** o branch para o seu fork:
   ```bash
   git push origin feat/minha-nova-funcionalidade
   ```
7. **Abra um Pull Request** no repositorio original descrevendo as mudancas realizadas
   e referenciando issues relacionadas, se houver.

Os mantenedores revisarao o PR e poderao solicitar ajustes antes da aprovacao.

## Convencoes de Commit

O projeto adota [Conventional Commits](https://www.conventionalcommits.org/). O formato
de cada mensagem de commit e:

```
<tipo>(<escopo opcional>): <descricao curta>
```

Tipos aceitos:

| Tipo       | Quando usar                                             |
| ---------- | ------------------------------------------------------- |
| `feat`     | Nova funcionalidade                                     |
| `fix`      | Correcao de bug                                         |
| `refactor` | Refatoracao sem mudanca de comportamento                |
| `test`     | Adicao ou correcao de testes                            |
| `docs`     | Alteracoes apenas na documentacao                       |
| `chore`    | Tarefas de manutencao (dependencias, CI, configuracoes) |
| `perf`     | Melhoria de desempenho                                  |

Exemplos:

```
feat(scenarios): add spatial aggregation for wind generation
fix(deck): correct reading of PARP file when clast is missing
docs: update CONTRIBUTING.md with pre-commit instructions
chore: bump inewave to 1.9.3
```

## Dependencias do Modulo inewave

O modulo [inewave](https://github.com/rjmalves/inewave) e a dependencia central do
`sintetizador-newave`. Ele fornece a modelagem de cada arquivo de entrada e saida do
modelo NEWAVE atraves do framework [cfinterface](https://github.com/rjmalves/cfi).

No `sintetizador-newave`, a dependencia do `inewave` e concentrada na classe `Deck`,
que fornece objetos nativos, DataFrames e arrays para as demais partes da aplicacao.

## Observacao sobre Documentacao

O conteudo do site de documentacao nao deve ser commitado manualmente no repositorio.
A publicacao e feita automaticamente pelos scripts de CI em qualquer modificacao no
branch `main`.

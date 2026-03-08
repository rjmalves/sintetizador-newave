# Changelog

Todas as mudanças notáveis neste projeto serão documentadas neste arquivo.

O formato é baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/),
e este projeto adere ao [Versionamento Semântico](https://semver.org/lang/pt-BR/).

## [Unreleased]

## [2.4.0]

### Changed

- Dependência `inewave` atualizada para >= 1.13.0 (cfinterface >= 1.9.1)
- API `set_version()` substituída por `read(path, version=...)` em conformidade com inewave v1.13

### Removed

- Suporte a Python 3.10 descontinuado. Versão mínima obrigatória: Python >= 3.11

## [2.3.0]

### Added

- Implementada síntese da operação da Geração de Usinas Não Simuladas (`GUNS_SBM.parquet` e `GUNS_SIN.parquet`)
- Adicionados novos dados para a síntese da execução: `VERSAO` e `TITULO`
- Adicionados novos dados para a síntese do sistema: `CVU`

### Changed

- Documentação de sínteses suportadas atualizada e padronizada com o [sintetizador-decomp](https://github.com/rjmalves/sintetizador-decomp) e o [sintetizador-dessem](https://github.com/rjmalves/sintetizador-dessem)

## [2.2.1]

### Fixed

- Correção na obtenção dos valores de volume armazenado em hm3 absolutos [#55](https://github.com/rjmalves/sintetizador-newave/issues/55)

## [2.2.0]

### Added

- Adição artificial de dados para o período pós-estudo em casos em que o modelo não gera estas informações [#52](https://github.com/rjmalves/sintetizador-newave/issues/52)

### Changed

- Pós-processamento das colunas nos arquivos de saída para eliminar informações desnecessárias [#50](https://github.com/rjmalves/sintetizador-newave/issues/50)
- Refatoração da síntese de política para formato compatível com a saída de cortes das LIBS. Existem duas sínteses: `CORTES_COEFICIENTES` e `CORTES_VARIAVEIS`.

### Fixed

- Melhor tratamento de erros no cálculo de limites, resultado em sínteses ilimitadas como contingência e geração de mensagens adequadas [#51](https://github.com/rjmalves/sintetizador-newave/issues/51)

## [2.1.2]

### Added

- Suporte ao versionamento de arquivos do NWLISTOP automaticamente quando ocorre mudança de formato

### Changed

- Concatenação dos arquivos de `ESTATISTICAS_*` e `METADADOS_*` com informações previamente existentes

### Fixed

- Correção na síntese da operação em casos com simulação final agregada

## [2.1.1]

### Fixed

- Correção na montagem das séries de cada ano simulado quando feita simulação final histórica [#47](https://github.com/rjmalves/sintetizador-newave/issues/47)

## [2.1.0]

### Added

- Suporte a versões >= 29.4 do modelo NEWAVE devido a renomeação de diversos arquivos de saída utilizados pelo sintetizador [#44](https://github.com/rjmalves/sintetizador-newave/issues/44)
- Implementada síntese do Custo Futuro (`CFU_SIN.parquet`) verificado no processo de simulação final [#45](https://github.com/rjmalves/sintetizador-newave/issues/45)
- Implementada síntese do Custo Total (`CTO_SIN.parquet`) obtido através da soma entre custo do estágio e custo futuro.

## [2.0.1]

### Fixed

- Fix no processamento de variáveis que só estão disponíveis no NWLISTOP por patamares e o valor médio dos patamares era calculado pelo sintetizador (`HLIQ`, `HJUS`).

## [2.0.0]

### Added

- Estatísticas calculadas a partir dos cenários de cada variável, para cada entidade, em um determinado estágio, passam a ser salvas em saídas específicas (`ESTATISTICAS_OPERACAO_UHE.parquet`, `ESTATISTICAS_CENARIOS_REE_BKW.parquet`, etc.)
- Uso do módulo [numba](https://numba.pydata.org/) como dependência opcional para aceleração de operações com DataFrames
- As sínteses agora produzem sempre como saída um arquivo de metadados, com informações sobre as sínteses que foram geradas (`METADADOS_OPERACAO.parquet`, `METADADOS_SISTEMA.parquet`, etc.) [#32](https://github.com/rjmalves/sintetizador-newave/issues/32)
- Implementado suporte para uso do caractere de `wildcard` (`*`) na especificação das sínteses desejadas via CLI [#33](https://github.com/rjmalves/sintetizador-newave/issues/33)
- Implementado o cálculo de limites para variáveis da síntese da operação que sejam limitadas inferior ou superiormente (colunas `limite_inferior` e `limite_superior`) [#23](https://github.com/rjmalves/sintetizador-newave/issues/23)
- Implementada síntese de Energia Armazenada por UHE, com cálculo feito na aplicação de síntese (`EARMI_UHE`, `EARMF_UHE`) [#37](https://github.com/rjmalves/sintetizador-newave/issues/37)
- Criação da abstração `Deck` que centraliza as conversões de formato e implementação de cálculos já realizados pelo modelo quando necessários para padronização do restante dos módulos de síntese [#40](https://github.com/rjmalves/sintetizador-newave/issues/40)

### Changed

- Refatoração dos processos de síntese, contemplando reuso de código e padronização de nomes de funções e variáveis
- Opção de exportação de saídas `PARQUET` não realiza mais a compressão em `gzip` automaticamente, adotando o `snappy` (padrão do Arrow). A extensão dos arquivos passa a ser apenas `.parquet`.
- Colunas do tipo `datetime` agora garante que a informação de fuso seja `UTC`, permitindo maior compatibilidade na leitura em outras implementações do Arrow. [#43](https://github.com/rjmalves/sintetizador-newave/issues/43)
- Colunas dos DataFrames de síntese padronizadas para `snake_case`
- Entidades passam a ser indexadas pelos seus códigos ao invés de nomes nos DataFrames das sínteses da operação e de cenários (`usina` -> `codigo_usina`, etc.). A síntese com opção `sistema` contem o mapeamento entre códigos e nomes.
- Substituída a divisão da síntese da operação utilizando agregação temporal (`EST` e `PAT`) pela inclusão sempre das colunas `patamar` e `duracao_patamar`, onde `patamar = 0` representa o valor médio do estágio [#21](https://github.com/rjmalves/sintetizador-newave/issues/21)
- Dados das sínteses de operação e cenários que sejam agrupados por entidades menores contém os códigos dos conjuntos que englobam estas entidades, permitindo agrupamentos arbitrários pelo usuário (ex. sínteses por UHE também contém colunas `codigo_ree` e `codigo_submercado`) [#22](https://github.com/rjmalves/sintetizador-newave/issues/22)
- Logging do processo de síntese melhorado e resumido, incluindo os tempos gastos em cada etapa do processo [#39](https://github.com/rjmalves/sintetizador-newave/issues/39)
- Diversas informações existentes no `pmo.dat` passaram a ser calculadas internamente para compatibilidade total com execuções do modelo que vão direto para a Simulação Final [#42](https://github.com/rjmalves/sintetizador-newave/issues/42)

### Removed

- Suporte a Python 3.8 descontinuado. Apenas versões de Python >= 3.10 são suportadas nos ambientes de CI e tem garantia de reprodutibilidade.
- Descontinuado o uso do `pylama` como linter para garantir padrões PEP de código devido à falta de suporte em Python >= 3.12. Adoção do [ruff](https://github.com/astral-sh/ruff) em substituição.
- Removidas sínteses específicas para violações de variáveis que tem seus limites superior e inferior calculados pela aplicação

## [1.2.0]

### Added

- Versão de compatibilidade com a séries de releases major `1.x`
- Síntese em formato compatível com `pyspark` para ingestão em ambiente analíticos

### Changed

- Última versão antes da reformulação do formato das sínteses (pré `2.0`)

### Fixed

- Correções diversas em variáveis da síntese da operação e cenários

## [1.1.0]

### Added

- Implementadas novas variáveis para síntese da operação: `VAGUA_UHE_EST`, `ENAAR_*_EST`, `ENAAF_*_EST`, `EARMI_*_EST`, `EARPI_*_EST`, `VARMI_*_EST`, `VARPI_UHE_EST`, `GHIDR_*_*`, `GHIDF_*_*`, `GTER_UTE_*`, `EDESR_*_EST`, `EVMIN_*_EST`, `EVMOR_*_EST`, `EEVAP_*_EST`, `VAFL_UHE_EST`, `VINC_UHE_EST`, `VDEF_UHE_*`, `HMON_UHE_EST`, `HJUS_UHE_PAT`, `HLIQ_UHE_PAT`, `VRET_UHE_EST`, `VDES_UHE_*`, `QRET_UHE_EST`, `QDES_UHE_*`
- Habilitado o paralelismo para todas as variáveis da operação, ao invés de apenas variáveis relacionadas a cada UHE

### Changed

- Compatibilização com `inewave` após a primeira major release (>= 1.4.0)
- Otimizado o código para aplicar modificações nos dataframes de maneira posicional, quando conveniente

## [1.0.0]

### Added

- Primeira major release
- Compatível com `inewave` até a versão 0.0.98 (pré 1ª major release)

[Unreleased]: https://github.com/rjmalves/sintetizador-newave/compare/v2.4.0...HEAD
[2.4.0]: https://github.com/rjmalves/sintetizador-newave/compare/v2.3.0...v2.4.0
[2.3.0]: https://github.com/rjmalves/sintetizador-newave/compare/v2.2.1...v2.3.0
[2.2.1]: https://github.com/rjmalves/sintetizador-newave/compare/v2.2.0...v2.2.1
[2.2.0]: https://github.com/rjmalves/sintetizador-newave/compare/v2.1.2...v2.2.0
[2.1.2]: https://github.com/rjmalves/sintetizador-newave/compare/v2.1.1...v2.1.2
[2.1.1]: https://github.com/rjmalves/sintetizador-newave/compare/v2.1.0...v2.1.1
[2.1.0]: https://github.com/rjmalves/sintetizador-newave/compare/v2.0.1...v2.1.0
[2.0.1]: https://github.com/rjmalves/sintetizador-newave/compare/v2.0.0...v2.0.1
[2.0.0]: https://github.com/rjmalves/sintetizador-newave/compare/v1.2.0...v2.0.0
[1.2.0]: https://github.com/rjmalves/sintetizador-newave/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/rjmalves/sintetizador-newave/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/rjmalves/sintetizador-newave/releases/tag/v1.0.0

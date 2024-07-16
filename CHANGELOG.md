# v2.0.0
- Suporte a Python 3.8 descontinuado. Apenas versões de Python >= 3.10 são suportadas nos ambientes de CI e tem garantia de reprodutibilidade.
- Refatoração dos processos de síntese, contemplando reuso de código e padronização de nomes de funções e variáveis
- Opção de exportação de saídas `PARQUET` não realiza mais a compressão em `gzip` automaticamente, adotando o `snappy` (padrão do Arrow). A extensão dos arquivos passa a ser apenas `.parquet`.
- Colunas do tipo `datetime` agora garante que a informação de fuso seja `UTC`, permitindo maior compatibilidade na leitura em outras implementações do Arrow. [#43](https://github.com/rjmalves/sintetizador-newave/issues/43)
- Colunas dos DataFrames de síntese padronizadas para `snake_case`
- Entidades passam a ser indexadas pelos seus códigos ao invés de nomes nos DataFrames das sínteses da operação e de cenários (`usina` -> `codigo_usina`, etc.). A síntese com opção `sistema` contem o mapeamento entre códigos e nomes.
- Estatísticas calculadas a partir dos cenários de cada variável, para cada entidade, em um determinado estágio, passam a ser salvas em saídas especíicas (`ESTATISTICAS_OPERACAO_UHE.parquet`, `ESTATISTICAS_CENARIOS_REE_BKW.parquet`, etc.)
- Uso do módulo (numba)[https://numba.pydata.org/] como dependência opcional para aceleração de operações com DataFrames
- Substituída a divisão da síntese da operação utilizando agregação temporal (`EST` e `PAT`) pela inclusão sempre das colunas `patamar` e `duracao_patamar`, onde `patamar = 0` representa o valor médio do estágio [#21](https://github.com/rjmalves/sintetizador-newave/issues/21)
- As sínteses agora produzem sempre como saída um arquivo de metadados, com informações sobre as sínteses que foram geradas (`METADADOS_OPERACAO.parquet`, `METADADOS_SISTEMA.parquet`, etc.) [#32](https://github.com/rjmalves/sintetizador-newave/issues/32)
- Implementado suporte para uso do caractere de `wildcard` (`*`) na especificação das sínteses desejadas via CLI [#33](https://github.com/rjmalves/sintetizador-newave/issues/33)
- Implementado o cálculo de limites para variáveis da síntese da operação que sejam limitadas inferior ou superiormente (colunas `limite_inferior` e `limite_superior`) [#23](https://github.com/rjmalves/sintetizador-newave/issues/23)
- Removidas sínteses específicas para violações de variáveis que tem seus limites superior e inferior calculados pela aplicação
- Dados das sínteses de operação e cenários que sejam agrupados por entidades menores contém os códigos dos conjuntos que englobam estas entidades, permitindo agrupamentos arbitrários pelo usuário (ex. sínteses por UHE também contém colunas `codigo_ree` e `codigo_submercado`) [#22](https://github.com/rjmalves/sintetizador-newave/issues/22)
- Implementada síntese de Energia Armazenada por UHE, com cálculo feito na aplicação de síntese (`EARMI_UHE`, `EARMF_UHE`) [#37](https://github.com/rjmalves/sintetizador-newave/issues/37)
- Logging do processo de síntese melhorado e resumido, incluindo os tempos gastos em cada etapa do processo [#39](https://github.com/rjmalves/sintetizador-newave/issues/39)
- Diversas informações existentes no `pmo.dat` passaram a ser calculadas internamente para compatibilidade total com execuções do modelo que vão direto para a Simulação Final [#42](https://github.com/rjmalves/sintetizador-newave/issues/42)
- Criação da abstração `Deck` que centraliza as conversões de formato e implementação de cálculos já realizados pelo modelo quando necessários para padronização do restante dos módulos de síntese [#40](https://github.com/rjmalves/sintetizador-newave/issues/40)


# v1.2.0 (v1-compat)

- Versão de compatibilidade com a séries de releases major `1.x`
- Última versão antes da reformulação do formato das sínteses (pré `2.0`)
- Síntese em formato compatível com `pyspark` para ingestão em ambiente analíticos
- Correções diversas em variáveis da síntese da operação e cenários

# v1.1.0

- Compatibilização com `inewave` após a primeira major release (>= 1.4.0)
- Implementadas novas variáveis para síntese da operação: `VAGUA_UHE_EST`, `ENAAR_*_EST`, `ENAAF_*_EST`, `EARMI_*_EST`, `EARPI_*_EST`, `VARMI_*_EST`, `VARPI_UHE_EST`, `GHIDR_*_*`, `GHIDF_*_*`, `GTER_UTE_*`, `EDESR_*_EST`, `EVMIN_*_EST`, `EVMOR_*_EST`, `EEVAP_*_EST`, `VAFL_UHE_EST`, `VINC_UHE_EST`, `VDEF_UHE_*`, `HMON_UHE_EST`, `HJUS_UHE_PAT`, `HLIQ_UHE_PAT`, `VRET_UHE_EST`, `VDES_UHE_*`, `QRET_UHE_EST`, `QDES_UHE_*`
- Habilitado o paralelismo para todas as variáveis da operação, ao invés de apenas variáveis relacionadas a cada UHE
- Otimizado o código para aplicar modificações nos dataframes de maneira posicional, quando conveniente

# v1.0.0

- Primeira major release
- Compatível com `inewave` até a versão 0.0.98 (pré 1ª major release)

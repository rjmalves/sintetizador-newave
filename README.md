# sintetizador-newave
Programa auxiliar para realizar a síntese de dados do programa NEWAVE em arquivos ou banco de dados.


## Modelo Unificado de Dados

O `sintetizador-newave` busca organizar as informações de entrada e saída do modelo NEWAVE em um modelo padronizado para lidar com os modelos do planejamento energético do SIN.

No momento são tratadas apenas informações de saída. Desta forma, foram criadas as categorias:

### Sistema

Informações da representação do sistema existente e alvo da otimização (TODO)

### Execução

Informações da execução do modelo, como ambiente escolhido, recursos computacionais disponíveis, convergência, tempo gasto, etc. (TODO)

### Cenários

Informações sobre os cenários visitados (gerados, fornecidos, processados, etc.) durante o processo de otimização. (TODO)

### Política

Informações sobre a política operativa construída (ou lida) pelo modelo (TODO)

### Operação

Informações da operação fornecida como saída pelo modelo. Estas informações são formadas a partir de três especificações: 

#### Variável


|          VARIÁVEL          |  MNEMÔNICO |
| -------------------------- | ---------- |
| Custo de Operação          |    COP     |
| Custo Marginal de Operação |    CMO     |
| Valor da Água              |    VAGUA   |
| Custo da Geração Térmica   |    CTER    |
| Energia Natural Afluente   |    ENA     |
| Energia Armazenada         |    EARM    |
| Energia Armazenada (%)     |    EARP    |
| Geração Hidráulica         |    GHID    |
| Geração Térmica            |    GTER    |
| Geração Eólica             |    GEOL    |
| Energia Vertida            |    EVER    |
| Vazão Afluente             |    QAFL    |
| Vazão Incremental          |    QINC    |
| Vazão Turbinada            |    QTUR    |
| Velocidade do Vento        |    VENTO   |
| Vazão Vertida              |    VVER    |
| Déficit                    |    DEF     |
| Intercâmbio                |    INT     |
| Volume Armazenado          |    VARM    |
| Volume Armazenado (%)      |    VARP    |
| Volume Vertido             |    VVER    |
| Volume Turbinado           |    VTUR    |


#### Agregação Espacial


|   AGERGAÇÃO ESPACIAL     |  MNEMÔNICO |
| ------------------------ | ---------- |
| Sistema Interligado      |     SIN    |
| Submercado               |     SBM    |
| Reservatório Equivalente |     REE    |
| Usina Hidroelétrica      |     UHE    |
| Usina Termelétrica       |     UTE    |
| Usina Eólica             |     UEE    |
| Par de Submercados       |     SBP    |


#### Agregação Temporal

|   AGERGAÇÃO TEMPORAL   |  MNEMÔNICO  |
| ---------------------- | ----------- |
| Mês (Estágio)          |     MES     |
| Patamar                |     PAT     |


Vale destacar que nem todas as combinações de mnemônicos estão disponíveis para o modelo NEWAVE. Até o momento as implementações são:

|          VARIÁVEL          | AGERGAÇÃO ESPACIAL | AGREGAÇÃO TEMPORAL |
| -------------------------- | ------------------ | ------------------ |
| COP                        | SIN                | MES                |
| CMO                        | SBM                | MES, PAT           |
| VAGUA                      | REE                | MES                |
| CTER                       | SBM, SIN           | MES                |
| ENA                        | REE, SBM, SIN      | MES                |
| EARM                       | REE, SBM, SIN      | MES                |
| EARP                       | REE, SBM, SIN      | MES                |
| GHID                       | UHE, REE, SBM, SIN | MES, PAT           |
| GTER                       | SBM, SIN           | MES                |
| GEOL                       | UEE, SBM, SIN      | MES, PAT           |
| EVER                       | REE, SBM, SIN      | MES                |
| QAFL                       | UHE                | MES                |
| QINC                       | UHE                | MES                |
| VENTO                      | UEE                | MES                |
| INT                        | SBP                | MES, PAT           |
| VARM                       | UHE                | MES                |
| VARP                       | UHE                | MES                |
| VVER                       | UHE                | MES                |
| VTUR                       | UHE                | MES                |


Exemplos de chaves de dados:
- EARP_SBM_MES
- VARP_UHE_MES
- GHID_UHE_PAT
- CMO_SBM_MES
- CMO_SBM_PAT


## Guia de Uso

Para usar o `sintetizador-newave`, ...
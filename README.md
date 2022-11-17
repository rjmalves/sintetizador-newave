# sintetizador-newave
Programa auxiliar para realizar a síntese de dados do programa NEWAVE em arquivos ou banco de dados.


## Modelo Unificado de Dados

O `sintetizador-newave` busca organizar as informações de entrada e saída do modelo NEWAVE em um modelo padronizado para lidar com os modelos do planejamento energético do SIN.

No momento são tratadas apenas informações de saída. Desta forma, foram criadas as categorias:

### Sistema

Informações da representação do sistema existente e alvo da otimização (TODO)

### Execução

Informações da execução do modelo, como ambiente escolhido, recursos computacionais disponíveis, convergência, tempo gasto, etc. 

|          VARIÁVEL          |     MNEMÔNICO     |
| -------------------------- | ----------------- |
| Composição de Custos       |       CUSTOS      |
| Tempo de Execução          |       TEMPO       |
| Convergência               |   CONVERGENCIA    |

Para síntese da informações da execução, as chaves de dados a serem sintetizados contém apenas os nomes das variáveis.

### Cenários

Informações sobre os cenários visitados (gerados, fornecidos, processados, etc.) durante o processo de otimização. (TODO)

### Política

Informações sobre a política operativa construída (ou lida) pelo modelo (TODO)

### Operação

Informações da operação fornecida como saída pelo modelo. Estas informações são formadas a partir de três especificações: 

#### Variável


|                      VARIÁVEL                     |  MNEMÔNICO |
| ------------------------------------------------- | ---------- |
| Custo de Operação                                 |  COP       |
| Custo Futuro                                      |  CFU       |
| Custo Marginal de Operação                        |  CMO       |
| Valor da Água                                     |  VAGUA     |
| Custo da Geração Térmica                          |  CTER      |
| Energia Natural Afluente Absoluta                 |  ENAA      |
| Energia Natural Afluente MLT                      |  ENAM      |
| Energia Armazenada Inicial                        |  EARMI     |
| Energia Armazenada Inicial (%)                    |  EARPI     |
| Energia Armazenada Final                          |  EARMF     |
| Energia Armazenada Final (%)                      |  EARPF     |
| Geração Hidráulica                                |  GHID      |
| Geração Térmica                                   |  GTER      |
| Geração Eólica                                    |  GEOL      |
| Energia Vertida                                   |  EVER      |
| Energia Vertida Turbinável                        |  EVERT     |
| Energia Vertida Não-Turbinável                    |  EVERNT    |
| Energia Vertida em Reservatórios                  |  EVERR     |
| Energia Vertida Turbinável em Reservatórios       |  EVERRT    |
| Energia Vertida Não-Turbinável em Reservatórios   |  EVERRNT   |
| Energia Vertida em Fio d'Água                     |  EVERF     |
| Energia Vertida Turbinável em Fio d'Água          |  EVERFT    |
| Energia Vertida Não-Turbinável em Fio d'Água      |  EVERFNT   |
| Vazão Afluente                                    |  QAFL      |
| Vazão Defluente                                   |  QDEF      |
| Vazão Incremental                                 |  QINC      |
| Vazão Turbinada                                   |  QTUR      |
| Velocidade do Vento                               |  VENTO     |
| Vazão Vertida                                     |  VVER      |
| Déficit                                           |  DEF       |
| Intercâmbio                                       |  INT       |
| Volume Armazenado Inicial                         |  VARMI     |
| Volume Armazenado Inicial (%)                     |  VARPI     |
| Volume Armazenado Final                           |  VARMF     |
| Volume Armazenado Final (%)                       |  VARPF     |
| Volume Vertido                                    |  VVER      |
| Volume Turbinado                                  |  VTUR      |


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
| Estágio                |     EST     |
| Patamar                |     PAT     |


Vale destacar que nem todas as combinações de mnemônicos estão disponíveis para o modelo NEWAVE. Até o momento as implementações são:

|          VARIÁVEL          | AGERGAÇÃO ESPACIAL | AGREGAÇÃO TEMPORAL |
| -------------------------- | ------------------ | ------------------ |
| COP                        | SIN                | EST                |
| CFU                        |                    |                    |
| CMO                        | SBM                | EST, PAT           |
| VAGUA                      | REE                | EST                |
| CTER                       | SBM, SIN           | EST                |
| ENAA                       | REE, SBM, SIN      | EST                |
| ENAM                       |                    |                    |
| EARMI                      |                    |                    |
| EARPI                      |                    |                    |
| EARMF                      | REE, SBM, SIN      | EST                |
| EARPF                      | REE, SBM, SIN      | EST                |
| GHID                       | UHE, REE, SBM, SIN | EST, PAT           |
| GTER                       | SBM, SIN           | EST                |
| GEOL                       | UEE, SBM, SIN      | EST, PAT           |
| EVERT                      | REE, SBM, SIN      | EST                |
| EVERNT                     |                    |                    |
| QAFL                       | UHE                | EST                |
| QDEF                       |                    |                    |
| QINC                       | UHE                | EST                |
| QTUR                       |                    |                    |
| VENTO                      | UEE                | EST                |
| INT                        | SBP                | EST, PAT           |
| VARMI                      |                    |                    |
| VARPI                      |                    |                    |
| VARMF                      | UHE                | EST                |
| VARPF                      | UHE                | EST                |
| VVER                       | UHE                | EST                |
| VTUR                       | UHE                | EST                |
| MER                        |                    |                    |
| DEF                        |                    |                    |


Exemplos de chaves de dados:
- EARPF_SBM_EST
- VARPF_UHE_EST
- GHID_UHE_PAT
- CMO_SBM_EST
- CMO_SBM_PAT


## Guia de Uso

Para usar o `sintetizador-newave`, ...
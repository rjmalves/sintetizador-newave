# sintetizador-newave
Programa auxiliar para realizar a síntese de dados do programa NEWAVE em arquivos de texto ou banco de dados.


|   AGERGAÇÃO ESPACIAL     |  MNEMÔNICO |
| ------------------------ | ---------- |
| Sistema Interligado      |     SIN    |
| Submercado               |     SBM    |
| Reservatório Equivalente |     REE    |
| Usina Hidroelétrica      |     UHE    |
| Usina Termelétrica       |     UTE    |
| Usina Eólica             |     UEE    |
| Par de Submercados       |     SBP    |


|   AGERGAÇÃO TEMPORAL   |  MNEMÔNICO  |
| ---------------------- | ----------- |
| Mês (Estágio)          |     MES     |
| Patamar                |     PAT     |


|          VARIÁVEL          | AGERGAÇÃO ESPACIAL | AGREGAÇÃO TEMPORAL |  MNEMÔNICO |
| -------------------------- | ------------------ | ------------------ | ---------- |
| Volume Armazenado          | UHE                | MES                | VARM       |
| Volume Armazenado (%)      | UHE                | MES                | VARP       |
| Energia Armazenada         | REE, SBM, SIN      | MES                | EARM       |
| Energia Armazenada (%)     | REE, SBM, SIN      | MES                | EARP       |
| Vazão Afluente             | UHE                | MES                | VAFL       |
| Vazão Incremental          | UHE                | MES                | VINC       |
| Energia Natural Afluente   | REE, SBM, SIN      | ...                | ENA        |
| Velocidade do Vento        | UEE                |                    | VEN        |
| Geração Térmica            | UTE, SBM, SIN      |                    | GTER       |
| Geração Hidráulica         | UHE, REE, SBM, SIN |                    | GHID       |
| Geração Eólica             | UEE, SBM, SIN      |                    | GEOL       |
| Vazão Vertida              | UHE                |                    | VVER       |
| Energia Vertida            | UHE, REE, SBM, SIN |                    | EVER       |
| Déficit                    | SBM, SIN           |                    | DEF        |
| Custo de Operação          | SIN                |                    | COP        |
| Custo Marginal de Operação | SBM                |                    | CMO        |
| Intercâmbio                | SBP                |                    | INT        |


Exemplos de chaves de dados:
EARP_SBM_EST
VARP_UHE_EST
GHID_UHE_PAT
CMO_SBM_EST
CMO_SBM_PAT

Chaves de dados na PoC:
EARP_SIN_EST
EARP_SBM_EST
EARP_REE_EST

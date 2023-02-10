.. _model:

Modelo Unificado de Dados
############################

O `sintetizador-newave` busca organizar as informações de entrada e saída do modelo NEWAVE em um modelo padronizado para lidar com os modelos do planejamento energético do SIN.

Desta forma, foram criadas as categorias:


Sistema
********

Informações da representação do sistema existente e alvo da otimização.

.. list-table:: Dados do Sistema
   :widths: 50 10
   :header-rows: 1

   * - VARIÁVEL
     - `MNEMÔNICO`
   * - Estágios
     - `EST`
   * - Patamares
     - `PAT`
   * - Submercados
     - `SBM`
   * - Reservatórios Equivalentes de Energia
     - `REE`
   * - Parques Eólicos Equivalentes
     - `PEE`
   * - Usina Termoelétrica
     - `UTE`
   * - Usina Hidroelétrica
     - `UHE`

Execução
********

Informações da execução do modelo, como ambiente escolhido, recursos computacionais disponíveis, convergência, tempo gasto, etc. 

.. list-table:: Dados da Execução
   :widths: 50 10
   :header-rows: 1

   * - VARIÁVEL
     - MNEMÔNICO
   * - Composição de Custos
     - `CUSTOS`
   * - Tempo de Execução
     - `TEMPO`
   * - Convergência
     - `CONVERGENCIA`
   * - Recursos Computacionais do Job
     - `RECURSOS_JOB`
   * - Recursos Computacionais do Cluster
     - `RECURSOS_CLUSTER`

Os mnemônicos `RECURSOS_JOB` e `RECURSOS_CLUSTER` dependem de arquivos que não são gerados automaticamente pelo modelo NEWAVE,
e sim por outras ferramentas adicionais. Portanto, não devem ser utilizados em ambientes recentemente configurados.

Cenários
*********

Informações sobre os cenários visitados (gerados, fornecidos, processados, etc.) durante o processo de otimização. (TODO)

Política
*********

Informações sobre a política operativa construída (ou lida) pelo modelo (TODO)

Operação
*********

Informações da operação fornecida como saída pelo modelo. Estas informações são formadas a partir de três especificações:

Variável
=========

A variável informa a grandeza que é modelada e fornecida como saída da operação pelo modelo.

.. list-table:: Variáveis da Operação
   :widths: 50 10
   :header-rows: 1

   * - VARIÁVEL
     - MNEMÔNICO
   * - Custo de Operação (Presente)
     - `COP`
   * - Custo Futuro
     - `CFU`
   * - Custo Marginal de Operação
     - `CMO`
   * - Valor da Água
     - `VAGUA`
   * - Custo da Geração Térmica
     - `CTER`
   * - Energia Natural Afluente Absoluta
     - `ENAA`
   * - Energia Natural Afluente (% MLT)
     - `ENAM`
   * - Energia Armazenada Inicial
     - `EARMI`
   * - Energia Armazenada Inicial (%)
     - `EARPI`
   * - Energia Armazenada Final
     - `EARMF`
   * - Energia Armazenada Final (%)
     - `EARPF`
   * - Geração Hidráulica
     - `GHID`
   * - Geração Térmica
     - `GTER`
   * - Geração Eólica
     - `GEOL`
   * - Energia Vertida
     - `EVER`
   * - Energia Vertida Turbinável
     - `EVERT`
   * - Energia Vertida Não-Turbinável
     - `EVERNT`
   * - Energia Vertida em Reservatórios
     - `EVERR`
   * - Energia Vertida Turbinável em Reservatórios
     - `EVERRT`
   * - Energia Vertida Não-Turbinável em Reservatórios
     - `EVERRNT`
   * - Energia Vertida em Fio d'Água
     - `EVERF`
   * - Energia Vertida Turbinável em Fio d'Água
     - `EVERFT`
   * - Energia Vertida Não-Turbinável em Fio d'Água
     - `EVERFNT`
   * - Vazão Afluente
     - `QAFL`
   * - Vazão Defluente
     - `QDEF`
   * - Vazão Incremental
     - `QINC`
   * - Vazão Turbinada
     - `QTUR`
   * - Vazão Vertida
     - `QVER`
   * - Velocidade do Vento
     - `VENTO`
   * - Mercado de Energia
     - `MER`
   * - Déficit
     - `DEF`
   * - Intercâmbio
     - `INT`
   * - Volume Armazenado Inicial
     - `VARMI`
   * - Volume Armazenado Inicial (%)
     - `VARPI`
   * - Volume Armazenado Final
     - `VARMF`
   * - Volume Armazenado Final (%)
     - `VARPF`
   * - Volume Vertido
     - `VVER`
   * - Volume Turbinado
     - `VTUR`

Agregação Espacial
===================

A agregação espacial informa o nível de agregação da variável em questão
em relação ao conjunto de elementos do sistema.

.. list-table:: Possíveis Agregações Espaciais
   :widths: 50 10
   :header-rows: 1

   * - AGREGAÇÂO
     - MNEMÔNICO
   * - Sistema Interligado
     - `SIN`
   * - Submercado
     - `SBM`
   * - Reservatório Equivalente
     - `REE`
   * - Usina Hidroelétrica
     - `UHE`
   * - Usina Termelétrica
     - `UTE`
   * - Parque Eólico Equivalente
     - `PEE`
   * - Par de Submercados
     - `SBP`


Agregação Temporal
===================

A agregação espacial informa o nível de agregação da variável em questão em relação
à discretização temporal (médio diário, semanal, mensal, por patamar, etc.).

.. list-table:: Possíveis Agregações Temporais
   :widths: 50 10
   :header-rows: 1

   * - AGREGAÇÂO
     - MNEMÔNICO
   * - Estágio
     - `EST`
   * - Patamar
     - `PAT`


Estado do Desenvolvimento
***************************

Todas as variáveis das categorias `Sistema`, `Execução`, `Cenários` e `Política` que são listadas
e estão presentes no modelo NEWAVE, estão disponíveis para uso no sintetizador.

Já para a categoria de operação, nem todas as combinações de agregações espaciais, temporais e variáveis
fazem sentido, ou especialmente são modeladas ou possíveis de se obter no NEWAVE. Desta forma,
o estado do desenvolvimento é listado a seguir, onde se encontram as combinações de sínteses da operação
que estão disponíveis no modelo.

.. list-table:: Sínteses da Operação Existentes
   :widths: 50 10 10
   :header-rows: 1

   * - VARIÁVEL
     - AGREGAÇÃO ESPACIAL
     - AGREGAÇÃO TEMPORAL
   * - `COP`
     - `SIN`
     - `EST`
   * - `CFU`
     - 
     - 
   * - `CMO`
     - `SBM`
     - `EST`, `PAT`
   * - `VAGUA`
     - `REE`
     - `EST`
   * - `CTER`
     - `SIN`, `SBM`
     - `EST`
   * - `ENAA`
     - `SIN`, `SBM`, `REE`
     - `EST`
   * - `ENAM`
     - 
     - 
   * - `EARMI`
     - 
     - 
   * - `EARPI`
     - 
     - 
   * - `EARMF`
     - `SIN`, `SBM`, `REE`
     - `EST`
   * - `EARPF`
     - `SIN`, `SBM`, `REE`
     - `EST`
   * - `EVER`
     - `SIN`, `SBM`, `REE`
     - `EST`
   * - `EVERF`
     - `SIN`, `SBM`, `REE`
     - `EST`
   * - `EVERR`
     - `SIN`, `SBM`, `REE`
     - `EST`
   * - `EVERT`
     - 
     - 
   * - `EVERNT`
     - 
     - 
   * - `EVERFT`
     - `SIN`, `SBM`, `REE`
     - `EST`
   * - `GHID`
     - `SIN`, `SBM`, `REE`, `UHE`
     - `EST`, `PAT`
   * - `GTER`
     - `SIN`, `SBM`
     - `EST`, `PAT`
   * - `GEOL`
     - `SIN`, `SBM`, `PEE`
     - `EST`, `PAT`
   * - `QAFL`
     - `UHE`
     - `EST`
   * - `QDEF`
     - `UHE`
     - `EST`, `PAT`
   * - `QINC`
     - `UHE`
     - `EST`
   * - `QTUR`
     - `SIN`
     - `EST`, `PAT`
   * - `QVER`
     - `SIN`
     - `EST`, `PAT`
   * - `VENTO`
     - `PEE`
     - `EST`
   * - `INT`
     - `SBP`
     - `EST`, `PAT`
   * - `VARMI`
     - 
     - 
   * - `VARPI`
     - 
     - 
   * - `VARMF`
     - `SIN`, `SBM`, `REE`, `UHE`
     - `EST`
   * - `VARPF`
     - `UHE`
     - `EST`
   * - `VVER`
     - `UHE`
     - `EST`, `PAT`
   * - `VTUR`
     - `UHE`
     - `EST`, `PAT`
   * - `MER`
     - 
     - 
   * - `DEF`
     - `SIN`, `SBM`
     - `EST`, `PAT`

São exemplos de chaves de dados válidas para as sínteses da operação `EARPF_SBM_EST`, `VARPF_UHE_EST`, `GHID_UHE_PAT`, `CMO_SBM_EST`, dentre outras.
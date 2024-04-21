.. _modelo:

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
   * - Patamares
     - `PAT`
   * - Submercados
     - `SBM`
   * - Reservatórios Equivalentes de Energia
     - `REE`
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

Cenários
*********

Informações sobre os cenários visitados (gerados, fornecidos, processados, etc.) durante o processo de otimização. Estas informações são formadas a partir de três especificações:


Variável
=========

A variável informa a grandeza do cenário que foi gerado, fornecido ou processado pelo modelo.

.. list-table:: Variáveis dos Cenários
   :widths: 50 10
   :header-rows: 1

   * - VARIÁVEL
     - MNEMÔNICO
   * - Energia Natural Afluente (MWmes)
     - `ENAA`
   * - Vazão Incremental (m3/s)
     - `QINC`


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



Etapa
======

A etapa diz o momento em que o valor de cenário foi utilizado.

.. list-table:: Possíveis Etapas
   :widths: 50 10
   :header-rows: 1

   * - ETAPA
     - MNEMÔNICO
   * - Forward
     - `FOR`
   * - Backward
     - `BKW`
   * - Simulação Final
     - `SF`

Política
*********

Informações sobre a política operativa construída (ou lida) pelo modelo.

.. list-table:: Variáveis da Política
   :widths: 50 10
   :header-rows: 1

   * - VARIÁVEL
     - MNEMÔNICO
   * - Cortes de Benders Calculados
     - `CORTES`
   * - Estados Visitados na Construção dos Cortes
     - `ESTADOS`
   * - Cortes de Benders Recebidos
     - `FCF`


Operação
*********

Informações da operação fornecida como saída pelo modelo. Assim como os dados de cenários, estas informações são formadas a partir de duas especificações:

Variável
=========

A variável informa a grandeza que é modelada e fornecida como saída da operação pelo modelo.

.. list-table:: Variáveis da Operação
   :widths: 50 10
   :header-rows: 1

   * - VARIÁVEL
     - MNEMÔNICO
   * - Cota de Jusante (m)
     - `HJUS`
   * - Cota de Montante (m)
     - `HMON`
   * - Custo de Operação (Presente) (10^6 R$)
     - `COP`
   * - Custo Futuro (10^6 R$)
     - `CFU`
   * - Custo Marginal de Operação (R$/MWh)
     - `CMO`
   * - Custo da Geração Térmica (10^6 R$)
     - `CTER`
   * - Déficit (MWmes)
     - `DEF`
   * - Energia Natural Afluente Absoluta (MWmes)
     - `ENAA`
   * - Energia Natural Afluente Absoluta em Fio d'Água  (MWmes)
     - `ENAAF`
   * - Energia Natural Afluente Absoluta em Reservatórios  (MWmes)
     - `ENAAR`
   * - Energia Armazenada Inicial (MWmes)
     - `EARMI`
   * - Energia Armazenada Inicial (%)
     - `EARPI`
   * - Energia Armazenada Final (MWmes)
     - `EARMF`
   * - Energia Armazenada Final (%)
     - `EARPF`
   * - Energia Vertida (MWmes)
     - `EVER`
   * - Energia Vertida Turbinável (MWmes)
     - `EVERT`
   * - Energia Vertida Não-Turbinável (MWmes)
     - `EVERNT`
   * - Energia Vertida em Reservatórios (MWmes)
     - `EVERR`
   * - Energia Vertida Turbinável em Reservatórios (MWmes)
     - `EVERRT`
   * - Energia Vertida Não-Turbinável em Reservatórios (MWmes)
     - `EVERRNT`
   * - Energia Vertida em Fio d'Água (MWmes)
     - `EVERF`
   * - Energia Vertida Turbinável em Fio d'Água (MWmes)
     - `EVERFT`
   * - Energia Vertida Não-Turbinável em Fio d'Água (MWmes)
     - `EVERFNT`
   * - Energia Desviada em Fio d'Água (MWmes)
     - `EDESF`
   * - Energia Desviada em Reservatórios (MWmes)
     - `EDESR`
   * - Energia Evaporada (MWmes)
     - `EEVAP`
   * - Energia de Defluência Mínima (MWmes)
     - `EVMIN`
   * - Energia de Enchimento de Volume Morto (MWmes)
     - `EVMOR`
   * - Geração Hidráulica (MWmes)
     - `GHID`
   * - Geração Hidráulica em Fio d'Água (MWmes)
     - `GHIDF`
   * - Geração Hidráulica em Reservatórios (MWmes)
     - `GHIDR`
   * - Geração Térmica (MWmes)
     - `GTER`
   * - Geração Eólica (MWmes)
     - `GEOL`
   * - Intercâmbio (MWmes)
     - `INT`
   * - Mercado de Energia (MWmes)
     - `MER`
   * - Mercado de Energia Líquido (MWmes)
     - `MERL`
   * - Queda Líquida (m)
     - `HLIQ`
   * - Valor da Água (R$/hm3 - UHE ou R$/MWmes - REE)
     - `VAGUA`
   * - Vazão Afluente (m3/s)
     - `QAFL`
   * - Vazão Defluente (m3/s)
     - `QDEF`
   * - Vazão Desviada (m3/s)
     - `QDES`
   * - Vazão Incremental (m3/s)
     - `QINC`
   * - Vazão Retirada (m3/s)
     - `QRET`
   * - Vazão Turbinada (m3/s)
     - `QTUR`
   * - Vazão Vertida (m3/s)
     - `QVER`
   * - Violação de FPHA (MWmes)
     - `VFPHA`
   * - Volume Armazenado Inicial (hm3)
     - `VARMI`
   * - Volume Armazenado Inicial (%)
     - `VARPI`
   * - Volume Armazenado Final (hm3)
     - `VARMF`
   * - Volume Armazenado Final (%)
     - `VARPF`
   * - Volume Afluente (hm3)
     - `VAFL`
   * - Volume Defluente (hm3)
     - `VDEF`
   * - Volume Desviado (hm3)
     - `VDES`
   * - Volume Incremental (hm3)
     - `VINC`
   * - Volume Retirado (hm3)
     - `VRET`
   * - Volume Turbinado (hm3)
     - `VTUR`
   * - Volume Vertido (hm3)
     - `VVER`

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
   * - Par de Submercados
     - `SBP`


Estado do Desenvolvimento
***************************

Todas as variáveis das categorias `Sistema`, `Execução` e `Política` que são listadas
e estão presentes no modelo NEWAVE, estão disponíveis para uso no sintetizador.

Já para as categorias de cenários e operação, nem todas as combinações de agregações espaciais, temporais e variáveis
fazem sentido, ou especialmente são modeladas ou possíveis de se obter no NEWAVE. Desta forma,
o estado do desenvolvimento é listado a seguir, onde se encontram as combinações de sínteses da operação
que estão disponíveis no modelo.


.. list-table:: Sínteses de Cenários Existentes
   :widths: 50 10 10
   :header-rows: 1

   * - VARIÁVEL
     - AGREGAÇÃO ESPACIAL
     - ETAPA
   * - `ENAA`
     - `REE`, `SBM`, `SIN`
     - `FOR`, `BKW`, `SF`
   * - `QINC`
     - `UHE`, `REE`, `SBM`, `SIN`
     - `FOR`, `BKW`, `SF`

.. list-table:: Sínteses da Operação Existentes
   :widths: 50 10 10
   :header-rows: 1

   * - VARIÁVEL
     - AGREGAÇÃO ESPACIAL
   * - `HJUS`
     - `UHE`
   * - `HMON`
     - `UHE`
   * - `COP`
     - `SIN`
   * - `CFU`
     - 
     - 
   * - `CMO`
     - `SBM`
   * - `CTER`
     - `SIN`, `SBM`
   * - `DEF`
     - `SIN`, `SBM`
   * - `ENAA`
     - `SIN`, `SBM`, `REE`
   * - `EARMI`
     - `SIN`, `SBM`, `REE`
   * - `EARPI`
     - `SIN`, `SBM`, `REE`
   * - `EARMF`
     - `SIN`, `SBM`, `REE`
   * - `EARPF`
     - `SIN`, `SBM`, `REE`
   * - `EVER`
     - `SIN`, `SBM`, `REE`
   * - `EVERF`
     - `SIN`, `SBM`, `REE`
   * - `EVERR`
     - `SIN`, `SBM`, `REE`
   * - `EVERT`
     - 
     - 
   * - `EVERNT`
     - 
     - 
   * - `EVERFT`
     - `SIN`, `SBM`, `REE`
   * - `GHID`
     - `SIN`, `SBM`, `REE`, `UHE`
   * - `GTER`
     - `SIN`, `SBM`
   * - `GEOL`
     - `SIN`, `SBM`, `PEE`
   * - `INT`
     - `SBP`
   * - `MER`
     - 
     - 
   * - `MERL`
     - `SIN`, `SBM`
   * - `HLIQ`
     - `UHE`
     - `PAT`
   * - `VAGUA`
     - `REE`, `UHE`
   * - `QAFL`
     - `UHE`
   * - `QDEF`
     - `UHE`
   * - `QDES`
     - `UHE`
   * - `QINC`
     - `UHE`
   * - `QRET`
     - `UHE`
   * - `QTUR`
     - `SIN`
   * - `QVER`
     - `SIN`
   * - `VFPHA`
     - `SIN`, `SBM`, `REE`, `UHE`
   * - `VARMI`
     - `SIN`, `SBM`, `REE`, `UHE`
   * - `VARPI`
     - `UHE`
   * - `VARMF`
     - `SIN`, `SBM`, `REE`, `UHE`
   * - `VARPF`
     - `UHE`
   * - `VAFL`
     - `UHE`
   * - `VDEF`
     - `UHE`
   * - `VINC`
     - `UHE`
   * - `VRET`
     - `UHE`
   * - `VTUR`
     - `UHE`
   * - `VVER`
     - `UHE`


São exemplos de elementos de dados válidos para as sínteses da operação `EARPF_SBM`, `VARPF_UHE`, `GHID_UHE`, `CMO_SBM`, dentre outras.
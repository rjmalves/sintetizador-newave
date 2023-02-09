.. _model:

Modelo Unificado de Dados
==========================

O `sintetizador-newave` busca organizar as informações de entrada e saída do modelo NEWAVE em um modelo padronizado para lidar com os modelos do planejamento energético do SIN.

No momento são tratadas apenas informações de saída. Desta forma, foram criadas as categorias:


Sistema
--------

Informações da representação do sistema existente e alvo da otimização.

Execução
---------

Informações da execução do modelo, como ambiente escolhido, recursos computacionais disponíveis, convergência, tempo gasto, etc. 

Cenários
---------

Informações sobre os cenários visitados (gerados, fornecidos, processados, etc.) durante o processo de otimização. (TODO)

Política
---------

Informações sobre a política operativa construída (ou lida) pelo modelo (TODO)

Operação
---------

Informações da operação fornecida como saída pelo modelo. Estas informações são formadas a partir de três especificações:
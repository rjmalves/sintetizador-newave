from enum import Enum


class Unit(Enum):
    RS = "R$"
    RS_mes_h = "(R$*mês)/h"
    RS_MWh = "R$/MWh"
    RS_mes_hm3_MWh = "(R$*mês)/(hm3*h)"
    MWmes = "MWmes"
    hm3 = "hm3"

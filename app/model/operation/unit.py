from enum import Enum


class Unit(Enum):
    RS = "R$"
    MiRS = "10^6 R$"
    RS_MWh = "R$/MWh"
    RS_hm3 = "R$/hm3"
    MWmes = "MWmes"
    MWmed = "MWmed"
    MWh = "MWh"
    MW = "MW"
    hm3 = "hm3"
    hm3_modif = "'h'"
    m3s = "m3/s"
    ms = "m/s"
    m = "m"
    perc = "%"
    perc_modif = "'%'"

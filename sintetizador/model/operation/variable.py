from enum import Enum
from typing import Dict


class Variable(Enum):
    CUSTO_MARGINAL_OPERACAO = "CMO"
    VALOR_AGUA = "VAGUA"
    VALOR_AGUA_INCREMENTAL = "VAGUAI"
    CUSTO_GERACAO_TERMICA = "CTER"
    CUSTO_DEFICIT = "CDEF"
    CUSTO_OPERACAO = "COP"
    CUSTO_FUTURO = "CFU"
    ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_RESERVATORIO = "ENAAR"
    ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_FIO = "ENAAF"
    ENERGIA_NATURAL_AFLUENTE_ABSOLUTA = "ENAA"
    ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL = "EARMI"
    ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL = "EARPI"
    ENERGIA_ARMAZENADA_ABSOLUTA_FINAL = "EARMF"
    ENERGIA_ARMAZENADA_PERCENTUAL_FINAL = "EARPF"
    GERACAO_HIDRAULICA_RESERVATORIO = "GHIDR"
    GERACAO_HIDRAULICA_FIO = "GHIDF"
    GERACAO_HIDRAULICA = "GHID"
    COTA_MONTANTE = "HMON"
    COTA_JUSANTE = "HJUS"
    QUEDA_LIQUIDA = "HLIQ"
    GERACAO_TERMICA = "GTER"
    GERACAO_EOLICA = "GEOL"
    ENERGIA_VERTIDA = "EVER"
    ENERGIA_VERTIDA_TURBINAVEL = "EVERT"
    ENERGIA_VERTIDA_NAO_TURBINAVEL = "EVERNT"
    ENERGIA_VERTIDA_RESERV = "EVERR"
    ENERGIA_VERTIDA_RESERV_TURBINAVEL = "EVERRT"
    ENERGIA_VERTIDA_RESERV_NAO_TURBINAVEL = "EVERRNT"
    ENERGIA_VERTIDA_FIO = "EVERF"
    ENERGIA_VERTIDA_FIO_TURBINAVEL = "EVERFT"
    ENERGIA_VERTIDA_FIO_NAO_TURBINAVEL = "EVERFNT"
    ENERGIA_DESVIO_RESERVATORIO = "EDESR"
    ENERGIA_DESVIO_FIO = "EDESF"
    META_ENERGIA_DEFLUENCIA_MINIMA = "MEVMIN"
    ENERGIA_DEFLUENCIA_MINIMA = "EVMIN"
    ENERGIA_VOLUME_MORTO = "EVMOR"
    ENERGIA_EVAPORACAO = "EEVAP"
    VAZAO_AFLUENTE = "QAFL"
    VAZAO_DEFLUENTE = "QDEF"
    VAZAO_INCREMENTAL = "QINC"
    VAZAO_TURBINADA = "QTUR"
    VAZAO_VERTIDA = "QVER"
    VAZAO_RETIRADA = "QRET"
    VAZAO_DESVIADA = "QDES"
    VELOCIDADE_VENTO = "VENTO"
    VOLUME_ARMAZENADO_ABSOLUTO_INICIAL = "VARMI"
    VOLUME_ARMAZENADO_PERCENTUAL_INICIAL = "VARPI"
    VOLUME_ARMAZENADO_ABSOLUTO_FINAL = "VARMF"
    VOLUME_ARMAZENADO_PERCENTUAL_FINAL = "VARPF"
    VOLUME_AFLUENTE = "VAFL"
    VOLUME_INCREMENTAL = "VINC"
    VOLUME_DEFLUENTE = "VDEF"
    VOLUME_VERTIDO = "VVER"
    VOLUME_TURBINADO = "VTUR"
    VOLUME_RETIRADO = "VRET"
    VOLUME_DESVIADO = "VDES"
    VOLUME_EVAPORADO = "VEVP"
    INTERCAMBIO = "INT"
    MERCADO = "MER"
    MERCADO_LIQUIDO = "MERL"
    USINAS_NAO_SIMULADAS = "UNSI"
    DEFICIT = "DEF"
    EXCESSO = "EXC"
    VIOLACAO_GERACAO_HIDRAULICA_MINIMA = "VGHMIN"
    VIOLACAO_DEFLUENCIA_MINIMA = "VDEFMIN"
    VIOLACAO_DEFLUENCIA_MAXIMA = "VDEFMAX"
    VIOLACAO_TURBINAMENTO_MINIMO = "VTURMIN"
    VIOLACAO_TURBINAMENTO_MAXIMO = "VTURMAX"
    VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA = "VEVMIN"
    VIOLACAO_VMINOP = "VVMINOP"
    VIOLACAO_FPHA = "VFPHA"
    VIOLACAO_POSITIVA_EVAPORACAO = "VPOSEVAP"
    VIOLACAO_NEGATIVA_EVAPORACAO = "VNEGEVAP"
    VIOLACAO_EVAPORACAO = "VEVAP"
    CORTE_GERACAO_EOLICA = "VEOL"

    def __repr__(self) -> str:
        return self.value

    @classmethod
    def factory(cls, val: str) -> "Variable":
        for v in cls:
            if v.value == val:
                return v
        return cls.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL

    @property
    def short_name(self):
        SHORT_NAMES: Dict[str, str] = {
            "CMO": "CMO",
            "VAGUA": "VAGUA",
            "VAGUAI": "VAGUAI",
            "CTER": "CTER",
            "CDEF": "CDEF",
            "COP": "COP",
            "CFU": "CFU",
            "ENAAR": "ENAAR",
            "ENAAF": "ENAAF",
            "ENAA": "ENAA",
            "EARMI": "EARMI",
            "EARPI": "EARPI",
            "EARMF": "EARMF",
            "EARPF": "EARPF",
            "GHIDR": "GHIDR",
            "GHIDF": "GHIDF",
            "GHID": "GHID",
            "HMON": "HMON",
            "HJUS": "HJUS",
            "HLIQ": "HLIQ",
            "GTER": "GTER",
            "GEOL": "GEOL",
            "EVER": "EVER",
            "EVERT": "EVERT",
            "EVERNT": "EVERNT",
            "EVERR": "EVERR",
            "EVERRT": "EVERRT",
            "EVERRNT": "EVERRNT",
            "EVERF": "EVERF",
            "EVERFT": "EVERFT",
            "EVERFNT": "EVERFNT",
            "EDESR": "EDESR",
            "EDESF": "EDESF",
            "MEVMIN": "MEVMIN",
            "EVMIN": "EVMIN",
            "EVMOR": "EVMOR",
            "EEVAP": "EEVAP",
            "QAFL": "QAFL",
            "QINC": "QINC",
            "QDEF": "QDEF",
            "QTUR": "QTUR",
            "QVER": "QVER",
            "QRET": "QRET",
            "QDES": "QDES",
            "VENTO": "VENTO",
            "VARMI": "VARMI",
            "VARPI": "VARPI",
            "VARMF": "VARMF",
            "VARPF": "VARPF",
            "VAFL": "VAFL",
            "VINC": "VINC",
            "VDEF": "VDEF",
            "VVER": "VVER",
            "VTUR": "VTUR",
            "VRET": "VRET",
            "VDES": "VDES",
            "VEVP": "VEVP",
            "INT": "INT",
            "MER": "MER",
            "MERL": "MERL",
            "UNSI": "UNSI",
            "DEF": "DEF",
            "EXC": "EXC",
            "VGHMIN": "VGHMIN",
            "VDEFMIN": "VDEFMIN",
            "VDEFMAX": "VDEFMAX",
            "VTURMIN": "VTURMIN",
            "VTURMAX": "VTURMAX",
            "VEVMIN": "VEVMIN",
            "VVMINOP": "VVMINOP",
            "VFPHA": "VFPHA",
            "VPOSEVAP": "VPOSEVAP",
            "VNEGEVAP": "VNEGEVAP",
            "VEVAP": "VEVAP",
            "VEOL": "VEOL",
        }
        return SHORT_NAMES.get(self.value)

    @property
    def long_name(self):
        LONG_NAMES: Dict[str, str] = {
            "CMO": "Custo Marginal de Operação",
            "VAGUA": "Valor da Água",
            "VAGUAI": "Valor da Água Incremental",
            "CTER": "Custo de Geração Térmica",
            "CDEF": "Custo de Déficit",
            "COP": "Custo de Operação",
            "CFU": "Custo Futuro",
            "ENAAR": "Energia Natural Afluente Absoluta em Reservatórios",
            "ENAAF": "Energia Natural Afluente Absoluta em Fio d'Água",
            "ENAA": "Energia Natural Afluente Absoluta",
            "EARMI": "Energia Armazenada Absoluta Inicial",
            "EARPI": "Energia Armazenada Percentual Inicial",
            "EARMF": "Energia Armazenada Absoluta Final",
            "EARPF": "Energia Armazenada Percentual Final",
            "GHIDR": "Geração Hidráulica em Reservatórios",
            "GHIDF": "Geração Hidráulica em Fio d'Água",
            "GHID": "Geração Hidráulica",
            "HMON": "Cota de Montante",
            "HJUS": "Cota de Jusante",
            "HLIQ": "Queda Líquida",
            "GTER": "Geração Térmica",
            "GEOL": "Geração Eólica",
            "EVER": "Energia Vertida",
            "EVERT": "Energia Vertida Turbinável",
            "EVERNT": "Energia Vertida Não-Turbinável",
            "EVERR": "Energia Vertida em Reservatórios",
            "EVERRT": "Energia Vertida Turbinável em Reservatórios",
            "EVERRNT": "Energia Vertida Não-Turbinável em Reservatórios",
            "EVERF": "Energia Vertida em Fio d'Água",
            "EVERFT": "Energia Vertida Turbinável em Fio d'Água",
            "EVERFNT": "Energia Vertida Não-Turbinável em Fio d'Água",
            "EDESR": "Energia Desviada em Reservatórios",
            "EDESF": "Energia Desviada em Fio d'Água",
            "MEVMIN": "Meta de Energia de Defluência Mínima",
            "EVMIN": "Energia de Defluência Mínima",
            "EVMOR": "Energia de Enchimento de Volume Morto",
            "EEVAP": "Energia de Evaporação",
            "QAFL": "Vazão Afluente",
            "QINC": "Vazão Incremental",
            "QDEF": "Vazão Defluente",
            "QTUR": "Vazão Turbinada",
            "QVER": "Vazão Vertida",
            "QRET": "Vazão Retirada",
            "QDES": "Vazão Desviada",
            "VENTO": "Velocidade do Vento",
            "VARMI": "Volume Armazenado Absoluto Inicial",
            "VARPI": "Volume Armazenado Percentual Inicial",
            "VARMF": "Volume Armazenado Absoluto Final",
            "VARPF": "Volume Armazenado Percentual Final",
            "VAFL": "Volume Afluente",
            "VINC": "Volume Incremental",
            "VDEF": "Volume Defluente",
            "VVER": "Volume Vertido",
            "VTUR": "Volume Turbinado",
            "VRET": "Volume Retirado",
            "VDES": "Volume Desviado",
            "VEVP": "Volume Evaporado",
            "INT": "Intercâmbio de Energia",
            "MER": "Mercado de Energia",
            "MERL": "Mercado de Energia Líquido",
            "UNSI": "Geração de Usinas Não Simuladas",
            "DEF": "Déficit",
            "EXC": "Excesso de Energia",
            "VGHMIN": "Violação de Geração Hidráulica Mínima",
            "VDEFMIN": "Violação de Defluência Mínima",
            "VDEFMAX": "Violação de Defluência Máxima",
            "VTURMIN": "Violação de Turbinamento Mínimo",
            "VTURMAX": "Violação de Turbinamento Máximo",
            "VEVMIN": "Violação de Energia de Defluência Mínima",
            "VVMINOP": "Violação de Volume Mínimo Operativo",
            "VFPHA": "Violação de FPHA",
            "VPOSEVAP": "Violação Positiva de Evaporação",
            "VNEGEVAP": "Violação Negativa de Evaporação",
            "VEVAP": "Violação de Evaporação",
            "VEOL": "Corte de Geração Eólica",
        }
        return LONG_NAMES.get(self.value)

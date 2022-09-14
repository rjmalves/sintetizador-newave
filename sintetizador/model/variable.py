from enum import Enum


class Variable(Enum):
    CUSTO_MARGINAL_OPERACAO = "CMO"
    VALOR_AGUA = "VAGUA"
    CUSTO_GERACAO_TERMICA = "CTER"
    CUSTO_OPERACAO = "COP"
    ENERGIA_NATURAL_AFLUENTE = "ENA"
    ENERGIA_ARMAZENADA_ABSOLUTA = "EARM"
    ENERGIA_ARMAZENADA_PERCENTUAL = "EARP"
    GERACAO_HIDRAULICA = "GHID"
    GERACAO_TERMICA = "GTER"
    ENERGIA_VERTIDA = "EVER"
    VAZAO_AFLUENTE = "QAFL"
    VAZAO_TURBINADA = "QTUR"
    VOLUME_ARMAZENADO_ABSOLUTO = "VARM"
    VOLUME_ARMAZENADO_PERCENTUAL = "VARP"

    @classmethod
    def factory(cls, val: str) -> "Variable":
        for v in cls:
            if v.value == val:
                return v
        return cls.ENERGIA_ARMAZENADA_PERCENTUAL

    def __repr__(self):
        return self.value

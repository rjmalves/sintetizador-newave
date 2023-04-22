from typing import Callable, Dict, List
import pandas as pd
import numpy as np
import logging
from inewave.config import MESES_DF
from datetime import datetime
from dateutil.relativedelta import relativedelta

from sintetizador.services.unitofwork import AbstractUnitOfWork
from sintetizador.utils.log import Log
from sintetizador.model.system.variable import Variable
from sintetizador.model.system.systemsynthesis import SystemSynthesis


FATOR_HM3_M3S = 1.0 / 2.63
HORAS_MES_NW = 730.0


class SystemSynthetizer:
    IDENTIFICATION_COLUMNS = [
        "dataInicio",
        "dataFim",
        "estagio",
        "submercado",
        "submercadoDe",
        "submercadoPara",
        "ree",
        "usina",
        "patamar",
    ]

    DEFAULT_SYSTEM_SYNTHESIS_ARGS: List[str] = [
        "EST",
        "PAT",
        "SBM",
        "REE",
        "UTE",
        "UHE",
    ]

    @classmethod
    def _default_args(cls) -> List[SystemSynthesis]:
        return [
            SystemSynthesis.factory(a)
            for a in cls.DEFAULT_SYSTEM_SYNTHESIS_ARGS
        ]

    @classmethod
    def _process_variable_arguments(
        cls,
        args: List[str],
    ) -> List[SystemSynthesis]:
        args_data = [SystemSynthesis.factory(c) for c in args]
        for i, a in enumerate(args_data):
            if a is None:
                Log.log(f"Erro no argumento fornecido: {args[i]}")
                return []
        return args_data

    @classmethod
    def filter_valid_variables(
        cls, variables: List[SystemSynthesis], uow: AbstractUnitOfWork
    ) -> List[SystemSynthesis]:
        with uow:
            dger = uow.files.get_dger()
            ree = uow.files.get_ree()
        valid_variables: List[SystemSynthesis] = []
        indiv = ree.rees["Mês Fim Individualizado"].isna().sum() == 0
        eolica = dger.considera_geracao_eolica != 0
        cls.logger.info(f"Caso com geração de cenários de eólica: {eolica}")
        cls.logger.info(f"Caso com modelagem híbrida: {indiv}")
        for v in variables:
            if v.variable in [Variable.PEE] and not eolica:
                continue
            valid_variables.append(v)
        cls.logger.info(f"Variáveis: {valid_variables}")
        return valid_variables

    @classmethod
    def _resolve(
        cls, synthesis: SystemSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        RULES: Dict[Variable, Callable] = {
            Variable.EST: cls.__resolve_EST,
            Variable.PAT: cls.__resolve_PAT,
            Variable.SBM: cls.__resolve_SBM,
            Variable.REE: cls.__resolve_REE,
            Variable.UTE: cls.__resolve_UTE,
            Variable.UHE: cls.__resolve_UHE,
            Variable.PEE: cls.__resolve_PEE,
        }
        return RULES[synthesis.variable](uow)

    @classmethod
    def __resolve_EST(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            dger = uow.files.get_dger()
        n_anos = dger.num_anos_estudo
        mes_inicial = dger.mes_inicio_estudo
        ano_inicial = dger.ano_inicio_estudo
        datas_iniciais = pd.date_range(
            datetime(year=ano_inicial, month=mes_inicial, day=1),
            datetime(year=ano_inicial + n_anos - 1, month=12, day=1),
            freq="MS",
        ).tolist()
        datas_finais = [d + relativedelta(months=1) for d in datas_iniciais]
        return pd.DataFrame(
            data={
                "idEstagio": list(range(1, len(datas_iniciais) + 1)),
                "dataInicio": datas_iniciais,
                "dataFim": datas_finais,
            }
        )

    @classmethod
    def __resolve_PAT(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            dger = uow.files.get_dger()
            pat = uow.files.get_patamar()
        meses_pre = dger.mes_inicio_estudo - dger.mes_inicio_pre_estudo
        estagios = (
            np.array(range(1, dger.num_anos_estudo * len(MESES_DF) + 1))
            - meses_pre
        )[meses_pre:]
        estagios = np.array(
            [
                list(
                    range(12 * i + 1, 12 * (i + 1) + 1)
                    for i in range(dger.num_anos_estudo)
                )
            ]
        )
        estagios = np.tile(estagios, pat.numero_patamares).flatten()
        duracao = pat.duracao_mensal_patamares
        patamares = np.array(list(range(1, pat.numero_patamares + 1)))
        pats = np.tile(
            np.repeat(patamares, len(MESES_DF)), dger.num_anos_estudo
        ).flatten()
        horas = HORAS_MES_NW * (duracao[MESES_DF].to_numpy().flatten())
        df = pd.DataFrame(
            data={"idEstagio": estagios, "patamar": pats, "duracao": horas}
        )
        # Filtra estágios pré-estudo
        df = df.loc[df["idEstagio"] > meses_pre]
        df = df.reset_index(drop=True)
        df["idEstagio"] -= df["idEstagio"].min() - 1
        return df

    @classmethod
    def __resolve_SBM(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            sistema = uow.files.get_sistema()
        df = sistema.custo_deficit[["Num. Subsistema", "Nome"]]
        df = df.rename(columns={"Num. Subsistema": "id", "Nome": "nome"})
        return df

    @classmethod
    def __resolve_REE(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            ree = uow.files.get_ree()
        df = ree.rees[["Número", "Nome", "Submercado"]]
        df = df.rename(
            columns={
                "Número": "id",
                "Submercado": "idSubmercado",
                "Nome": "nome",
            }
        )
        return df[["id", "idSubmercado", "nome"]]

    @classmethod
    def __resolve_UTE(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            conft = uow.files.get_conft()

        df = conft.usinas[["Número", "Nome", "Subsistema"]]
        df = df.rename(
            columns={
                "Número": "id",
                "Subsistema": "idSubmercado",
                "Nome": "nome",
            }
        )
        return df[["id", "idSubmercado", "nome"]]

    @classmethod
    def __resolve_UHE(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            confhd = uow.files.get_confhd()

        df = confhd.usinas[
            ["Número", "Nome", "Posto", "REE", "Volume Inicial"]
        ]
        df = df.rename(
            columns={
                "Número": "id",
                "REE": "idREE",
                "Nome": "nome",
                "Posto": "posto",
                "Volume Inicial": "volumeInicial",
            }
        )
        return df[["id", "idREE", "nome", "posto", "volumeInicial"]]

    @classmethod
    def __resolve_PEE(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        with uow:
            eolica = uow.files.get_eolicacadastro()

        pees = eolica.pee_cad()
        codigos = [p.codigo_pee for p in pees]
        nomes = [p.nome_pee for p in pees]
        df = pd.DataFrame(data={"id": codigos, "nome": nomes})
        return df

    @classmethod
    def synthetize(cls, variables: List[str], uow: AbstractUnitOfWork):
        cls.logger = logging.getLogger()
        try:
            if len(variables) == 0:
                variables = SystemSynthetizer._default_args()
            else:
                variables = SystemSynthetizer._process_variable_arguments(
                    variables
                )
        except Exception as e:
            cls.logger.error(str(e))
            valid_synthesis = []
        valid_synthesis = SystemSynthetizer.filter_valid_variables(
            variables, uow
        )
        for s in valid_synthesis:
            filename = str(s)
            cls.logger.info(f"Realizando síntese de {filename}")
            df = cls._resolve(s, uow)
            with uow:
                uow.export.synthetize_df(df, filename)

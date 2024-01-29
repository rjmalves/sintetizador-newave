from typing import Callable, Dict, List, Type, TypeVar, Optional
import pandas as pd  # type: ignore
import numpy as np  # type: ignore
import logging
from inewave.config import MESES_DF
from inewave.newave import Dger, Ree, Confhd, Conft, Sistema
from inewave.libs.modelos.eolica import RegistroPEECadastro
from datetime import datetime
from dateutil.relativedelta import relativedelta  # type: ignore

from sintetizador.services.unitofwork import AbstractUnitOfWork
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

    T = TypeVar("T")

    logger: Optional[logging.Logger] = None

    @classmethod
    def _default_args(cls) -> List[SystemSynthesis]:
        args = [
            SystemSynthesis.factory(a)
            for a in cls.DEFAULT_SYSTEM_SYNTHESIS_ARGS
        ]
        return [arg for arg in args if arg is not None]

    @classmethod
    def _process_variable_arguments(
        cls,
        args: List[str],
    ) -> List[SystemSynthesis]:
        args_data = [SystemSynthesis.factory(c) for c in args]
        valid_args = [arg for arg in args_data if arg is not None]
        return valid_args

    @classmethod
    def _validate_data(cls, data, type: Type[T], msg: str = "dados") -> T:
        if not isinstance(data, type):
            if cls.logger is not None:
                cls.logger.error(f"Erro na leitura de {msg}")
            raise RuntimeError()
        return data

    @classmethod
    def _get_dger(cls, uow: AbstractUnitOfWork) -> Dger:
        with uow:
            dger = uow.files.get_dger()
            if dger is None:
                raise RuntimeError(
                    "Erro no processamento do dger.dat para"
                    + " síntese do sistema"
                )
            return dger

    @classmethod
    def _get_ree(cls, uow: AbstractUnitOfWork) -> Ree:
        with uow:
            ree = uow.files.get_ree()
            if ree is None:
                raise RuntimeError(
                    "Erro no processamento do ree.dat para"
                    + " síntese do sistema"
                )
            return ree

    @classmethod
    def _get_confhd(cls, uow: AbstractUnitOfWork) -> Confhd:
        with uow:
            confhd = uow.files.get_confhd()
            if confhd is None:
                raise RuntimeError(
                    "Erro no processamento do confhd.dat para"
                    + " síntese do sistema"
                )
            return confhd

    @classmethod
    def _get_conft(cls, uow: AbstractUnitOfWork) -> Conft:
        with uow:
            conft = uow.files.get_conft()
            if conft is None:
                raise RuntimeError(
                    "Erro no processamento do conft.dat para"
                    + " síntese do sistema"
                )
            return conft

    @classmethod
    def _get_sistema(cls, uow: AbstractUnitOfWork) -> Sistema:
        with uow:
            sist = uow.files.get_sistema()
            if sist is None:
                if cls.logger is not None:
                    cls.logger.error(
                        "Erro no processamento do sistema.dat para"
                        + " síntese dos cenários"
                    )
                raise RuntimeError()
            return sist

    @classmethod
    def filter_valid_variables(
        cls, variables: List[SystemSynthesis], uow: AbstractUnitOfWork
    ) -> List[SystemSynthesis]:
        dger = cls._get_dger(uow)
        ree = cls._get_ree(uow)

        rees = cls._validate_data(ree.rees, pd.DataFrame, "REEs")
        geracao_eolica = cls._validate_data(
            dger.considera_geracao_eolica, int, "dger"
        )

        valid_variables: List[SystemSynthesis] = []
        indiv = rees["mes_fim_individualizado"].isna().sum() == 0
        eolica = geracao_eolica != 0
        if cls.logger is not None:
            cls.logger.info(
                f"Caso com geração de cenários de eólica: {eolica}"
            )
            cls.logger.info(f"Caso com modelagem híbrida: {indiv}")
        for v in variables:
            if v.variable in [Variable.PEE] and not eolica:
                continue
            valid_variables.append(v)
        if cls.logger is not None:
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
        dger = cls._get_dger(uow)
        ano_inicial = cls._validate_data(
            dger.ano_inicio_estudo,
            int,
            "dger",
        )
        mes_inicial = cls._validate_data(
            dger.mes_inicio_estudo,
            int,
            "dger",
        )
        n_anos = cls._validate_data(
            dger.num_anos_estudo,
            int,
            "dger",
        )
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
        dger = cls._get_dger(uow)
        if dger is None:
            raise RuntimeError(
                "Erro no processamento do dger.dat para"
                + " síntese do sistema"
            )
        pat = uow.files.get_patamar()
        if pat is None:
            raise RuntimeError(
                "Erro no processamento do patamar.dat para"
                + " síntese do sistema"
            )
        num_patamares = cls._validate_data(
            pat.numero_patamares,
            int,
            "patamares",
        )

        duracao_patamares = cls._validate_data(
            pat.duracao_mensal_patamares,
            pd.DataFrame,
            "patamares",
        )
        mes_inicio = cls._validate_data(
            dger.mes_inicio_estudo,
            int,
            "dger",
        )
        mes_inicio_pre = cls._validate_data(
            dger.mes_inicio_pre_estudo,
            int,
            "dger",
        )
        anos_estudo = cls._validate_data(
            dger.num_anos_estudo,
            int,
            "dger",
        )

        meses_pre = mes_inicio - mes_inicio_pre
        estagios = (
            np.array(range(1, anos_estudo * len(MESES_DF) + 1)) - meses_pre
        )[meses_pre:]
        estagios = np.array(
            [
                list(
                    range(12 * i + 1, 12 * (i + 1) + 1)
                    for i in range(anos_estudo)
                )
            ]
        )
        estagios = np.tile(estagios, num_patamares).flatten()
        patamares = np.array(list(range(1, num_patamares + 1)))
        pats = np.tile(
            np.repeat(patamares, len(MESES_DF)), anos_estudo
        ).flatten()
        horas = HORAS_MES_NW * (
            duracao_patamares[MESES_DF].to_numpy().flatten()
        )
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
        arq_sistema = cls._get_sistema(uow)
        if arq_sistema is None:
            raise RuntimeError(
                "Erro no processamento do sistema.dat para"
                + " síntese do sistema"
            )
        sistema = cls._validate_data(
            arq_sistema.custo_deficit,
            pd.DataFrame,
            "submercados",
        )
        df = sistema[["codigo_submercado", "nome_submercado"]]
        df = df.rename(
            columns={"codigo_submercado": "id", "nome_submercado": "nome"}
        )
        return df

    @classmethod
    def __resolve_REE(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        arq_ree = cls._get_ree(uow)
        if arq_ree is None:
            raise RuntimeError(
                "Erro no processamento do ree.dat para" + " síntese do sistema"
            )
        rees = cls._validate_data(arq_ree.rees, pd.DataFrame, "REEs")
        df = rees[["codigo", "nome", "submercado"]]
        df = df.rename(
            columns={
                "codigo": "id",
                "submercado": "idSubmercado",
                "nome": "nome",
            }
        )
        return df[["id", "idSubmercado", "nome"]]

    @classmethod
    def __resolve_UTE(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        arq_conft = cls._get_conft(uow)
        conft = cls._validate_data(arq_conft.usinas, pd.DataFrame, "UTEs")

        df = conft[["codigo_usina", "nome_usina", "submercado"]]
        df = df.rename(
            columns={
                "codigo_usina": "id",
                "submercado": "idSubmercado",
                "nome_usina": "nome",
            }
        )
        return df[["id", "idSubmercado", "nome"]]

    @classmethod
    def __resolve_UHE(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        arq_confhd = uow.files.get_confhd()
        if arq_confhd is None:
            raise RuntimeError(
                "Erro no processamento do confhd.dat para"
                + " síntese do sistema"
            )
        confhd = cls._validate_data(arq_confhd.usinas, pd.DataFrame, "UHEs")

        df = confhd[
            [
                "codigo_usina",
                "nome_usina",
                "posto",
                "ree",
                "volume_inicial_percentual",
            ]
        ]
        df = df.rename(
            columns={
                "codigo_usina": "id",
                "ree": "idREE",
                "nome_usina": "nome",
                "posto": "posto",
                "volume_inicial_percentual": "volumeInicial",
            }
        )
        return df[["id", "idREE", "nome", "posto", "volumeInicial"]]

    @classmethod
    def __resolve_PEE(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        eolica = uow.files.get_eolica()
        if eolica is None:
            raise RuntimeError(
                "Erro no processamento do eolica-cadastro.csv para"
                + " síntese do sistema"
            )

        pees = eolica.pee_cad()
        if isinstance(pees, list):
            codigos = [p.codigo_pee for p in pees]
            nomes = [p.nome_pee for p in pees]
        elif isinstance(pees, RegistroPEECadastro):
            codigos = [pees.codigo_pee]
            nomes = [pees.nome_pee]
        else:
            if cls.logger is not None:
                cls.logger.error("Erro na leitura de PEEs")
                raise RuntimeError()
        df = pd.DataFrame(data={"id": codigos, "nome": nomes})
        return df

    @classmethod
    def synthetize(cls, variables: List[str], uow: AbstractUnitOfWork):
        cls.logger = logging.getLogger("main")
        try:
            if len(variables) == 0:
                synthesis_variables = SystemSynthetizer._default_args()
            else:
                synthesis_variables = (
                    SystemSynthetizer._process_variable_arguments(variables)
                )
            valid_synthesis = SystemSynthetizer.filter_valid_variables(
                synthesis_variables, uow
            )
            for s in valid_synthesis:
                filename = str(s)
                cls.logger.info(f"Realizando síntese de {filename}")
                df = cls._resolve(s, uow)
                with uow:
                    uow.export.synthetize_df(df, filename)
        except Exception as e:
            cls.logger.error(str(e))

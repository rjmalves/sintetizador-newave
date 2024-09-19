from dataclasses import dataclass
from typing import Optional

from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.unit import Unit
from app.model.operation.variable import Variable


@dataclass
class OperationSynthesis:
    variable: Variable
    spatial_resolution: SpatialResolution

    def __repr__(self) -> str:
        return "_".join([
            self.variable.value,
            self.spatial_resolution.value,
        ])

    def __hash__(self) -> int:
        return hash(
            f"{self.variable.value}_" + f"{self.spatial_resolution.value}_"
        )

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, OperationSynthesis):
            return False
        else:
            return all([
                self.variable == o.variable,
                self.spatial_resolution == o.spatial_resolution,
            ])

    @classmethod
    def factory(cls, synthesis: str) -> Optional["OperationSynthesis"]:
        data = synthesis.split("_")
        if len(data) != 2:
            return None
        else:
            return cls(
                Variable.factory(data[0]),
                SpatialResolution.factory(data[1]),
            )


SUPPORTED_SYNTHESIS: list[str] = [
    "CMO_SBM",
    "VAGUA_REE",
    "VAGUA_UHE",
    "VAGUAI_UHE",
    "CTER_SBM",
    "CTER_SIN",
    "COP_SIN",
    "ENAA_REE",
    "ENAA_SBM",
    "ENAA_SIN",
    "ENAAR_REE",
    "ENAAR_SBM",
    "ENAAR_SIN",
    "ENAAF_REE",
    "ENAAF_SBM",
    "ENAAF_SIN",
    "EARPI_REE",
    "EARPI_SBM",
    "EARPI_SIN",
    "EARPF_REE",
    "EARPF_SBM",
    "EARPF_SIN",
    "EARMI_REE",
    "EARMI_SBM",
    "EARMI_SIN",
    "EARMF_REE",
    "EARMF_SBM",
    "EARMF_SIN",
    "GHIDR_REE",
    "GHIDR_SBM",
    "GHIDR_SIN",
    "GHID_REE",
    "GHID_SBM",
    "GHID_SIN",
    "GHIDF_REE",
    "GHIDF_SBM",
    "GHIDF_SIN",
    "GTER_SBM",
    "GTER_SIN",
    "GTER_UTE",
    "EVER_REE",
    "EVER_SBM",
    "EVER_SIN",
    "EVERR_REE",
    "EVERR_SBM",
    "EVERR_SIN",
    "EVERF_REE",
    "EVERF_SBM",
    "EVERF_SIN",
    "EVERFT_REE",
    "EVERFT_SBM",
    "EVERFT_SIN",
    "EDESR_REE",
    "EDESR_SBM",
    "EDESR_SIN",
    "EDESF_REE",
    "EDESF_SBM",
    "EDESF_SIN",
    "EVMIN_REE",
    "EVMIN_SBM",
    "EVMIN_SIN",
    "EVMOR_REE",
    "EVMOR_SBM",
    "EVMOR_SIN",
    "EEVAP_REE",
    "EEVAP_SBM",
    "EEVAP_SIN",
    "VGHMIN_REE",
    "VGHMIN_SBM",
    "VGHMIN_SIN",
    "VTUR_UHE",
    "VVER_UHE",
    "QTUR_UHE",
    "QVER_UHE",
    "QAFL_UHE",
    "QINC_UHE",
    "VAFL_UHE",
    "VINC_UHE",
    "QDEF_UHE",
    "VDEF_UHE",
    "VRET_UHE",
    "VDES_UHE",
    "QRET_UHE",
    "QDES_UHE",
    "VTUR_REE",
    "VVER_REE",
    "QTUR_REE",
    "QVER_REE",
    "QAFL_REE",
    "QINC_REE",
    "VAFL_REE",
    "VINC_REE",
    "QDEF_REE",
    "VDEF_REE",
    "VRET_REE",
    "VDES_REE",
    "QRET_REE",
    "QDES_REE",
    "VTUR_SBM",
    "VVER_SBM",
    "QTUR_SBM",
    "QVER_SBM",
    "QAFL_SBM",
    "QINC_SBM",
    "VAFL_SBM",
    "VINC_SBM",
    "QDEF_SBM",
    "VDEF_SBM",
    "VRET_SBM",
    "VDES_SBM",
    "QRET_SBM",
    "QDES_SBM",
    "VTUR_SIN",
    "VVER_SIN",
    "QTUR_SIN",
    "QVER_SIN",
    "QAFL_SIN",
    "QINC_SIN",
    "VAFL_SIN",
    "VINC_SIN",
    "QDEF_SIN",
    "VDEF_SIN",
    "VRET_SIN",
    "VDES_SIN",
    "QRET_SIN",
    "QDES_SIN",
    "VARMI_UHE",
    "VARMI_REE",
    "VARMI_SBM",
    "VARMI_SIN",
    "VARMF_UHE",
    "VARMF_REE",
    "VARMF_SBM",
    "VARMF_SIN",
    "VARPI_UHE",
    "VARPF_UHE",
    "GHID_UHE",
    "VENTO_PEE",
    "GEOL_PEE",
    "GEOL_SBM",
    "GEOL_SIN",
    "INT_SBP",
    "DEF_SBM",
    "DEF_SIN",
    "EXC_SBM",
    "EXC_SIN",
    "CDEF_SBM",
    "CDEF_SIN",
    "MERL_SBM",
    "MERL_SIN",
    "VEOL_SBM",
    "VGHMIN_UHE",
    "VFPHA_UHE",
    "VFPHA_REE",
    "VFPHA_SBM",
    "VFPHA_SIN",
    "HMON_UHE",
    "HJUS_UHE",
    "HLIQ_UHE",
    "VEVP_UHE",
    "VEVP_REE",
    "VEVP_SBM",
    "VEVP_SIN",
    "VEVAP_UHE",
    "VEVAP_REE",
    "VEVAP_SBM",
    "VEVAP_SIN",
]

SYNTHESIS_DEPENDENCIES: dict[OperationSynthesis, list[OperationSynthesis]] = {
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_RESERV,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_FIO,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ),
    ],
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_RESERV,
            SpatialResolution.SUBMERCADO,
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_FIO,
            SpatialResolution.SUBMERCADO,
        ),
    ],
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_RESERV,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ),
        OperationSynthesis(
            Variable.ENERGIA_VERTIDA_FIO,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ),
    ],
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        )
    ],
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        )
    ],
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.SUBMERCADO,
        )
    ],
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            SpatialResolution.SUBMERCADO,
        )
    ],
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        )
    ],
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        )
    ],
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
        SpatialResolution.USINA_HIDROELETRICA,
    ): [
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        )
    ],
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
        SpatialResolution.USINA_HIDROELETRICA,
    ): [
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        )
    ],
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.USINA_HIDROELETRICA,
        )
    ],
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.USINA_HIDROELETRICA,
        )
    ],
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.USINA_HIDROELETRICA,
        )
    ],
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        )
    ],
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        )
    ],
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        )
    ],
    OperationSynthesis(
        Variable.ENERGIA_DEFLUENCIA_MINIMA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.META_ENERGIA_DEFLUENCIA_MINIMA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        ),
    ],
    OperationSynthesis(
        Variable.ENERGIA_DEFLUENCIA_MINIMA,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.META_ENERGIA_DEFLUENCIA_MINIMA,
            SpatialResolution.SUBMERCADO,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
            SpatialResolution.SUBMERCADO,
        ),
    ],
    OperationSynthesis(
        Variable.ENERGIA_DEFLUENCIA_MINIMA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.META_ENERGIA_DEFLUENCIA_MINIMA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ),
    ],
    OperationSynthesis(
        Variable.VAZAO_VERTIDA,
        SpatialResolution.USINA_HIDROELETRICA,
    ): [
        OperationSynthesis(
            Variable.VOLUME_VERTIDO,
            SpatialResolution.USINA_HIDROELETRICA,
        )
    ],
    OperationSynthesis(
        Variable.VAZAO_TURBINADA,
        SpatialResolution.USINA_HIDROELETRICA,
    ): [
        OperationSynthesis(
            Variable.VOLUME_TURBINADO,
            SpatialResolution.USINA_HIDROELETRICA,
        )
    ],
    OperationSynthesis(
        Variable.VAZAO_DESVIADA,
        SpatialResolution.USINA_HIDROELETRICA,
    ): [
        OperationSynthesis(
            Variable.VOLUME_DESVIADO,
            SpatialResolution.USINA_HIDROELETRICA,
        )
    ],
    OperationSynthesis(
        Variable.VAZAO_RETIRADA,
        SpatialResolution.USINA_HIDROELETRICA,
    ): [
        OperationSynthesis(
            Variable.VOLUME_RETIRADO,
            SpatialResolution.USINA_HIDROELETRICA,
        )
    ],
    OperationSynthesis(
        Variable.VOLUME_AFLUENTE,
        SpatialResolution.USINA_HIDROELETRICA,
    ): [
        OperationSynthesis(
            Variable.VAZAO_AFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        )
    ],
    OperationSynthesis(
        Variable.VOLUME_INCREMENTAL,
        SpatialResolution.USINA_HIDROELETRICA,
    ): [
        OperationSynthesis(
            Variable.VAZAO_INCREMENTAL,
            SpatialResolution.USINA_HIDROELETRICA,
        )
    ],
    OperationSynthesis(
        Variable.VAZAO_DEFLUENTE,
        SpatialResolution.USINA_HIDROELETRICA,
    ): [
        OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
        OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_DEFLUENTE,
        SpatialResolution.USINA_HIDROELETRICA,
    ): [
        OperationSynthesis(
            Variable.VOLUME_TURBINADO,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
        OperationSynthesis(
            Variable.VOLUME_VERTIDO,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VIOLACAO_FPHA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.VIOLACAO_FPHA,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VIOLACAO_FPHA,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.VIOLACAO_FPHA,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VIOLACAO_FPHA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.VIOLACAO_FPHA,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_AFLUENTE,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.VOLUME_AFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_AFLUENTE,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.VOLUME_AFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_AFLUENTE,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.VOLUME_AFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_INCREMENTAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.VOLUME_INCREMENTAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_INCREMENTAL,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.VOLUME_INCREMENTAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_INCREMENTAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.VOLUME_INCREMENTAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_DEFLUENTE,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.VOLUME_DEFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_DEFLUENTE,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.VOLUME_DEFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_DEFLUENTE,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.VOLUME_DEFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_VERTIDO,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.VOLUME_VERTIDO,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_VERTIDO,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.VOLUME_VERTIDO,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_VERTIDO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.VOLUME_VERTIDO,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_TURBINADO,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.VOLUME_TURBINADO,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_TURBINADO,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.VOLUME_TURBINADO,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_TURBINADO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.VOLUME_TURBINADO,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_RETIRADO,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.VOLUME_RETIRADO,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_RETIRADO,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.VOLUME_RETIRADO,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_RETIRADO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.VOLUME_RETIRADO,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_DESVIADO,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.VOLUME_DESVIADO,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_DESVIADO,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.VOLUME_DESVIADO,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_DESVIADO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.VOLUME_DESVIADO,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VAZAO_AFLUENTE,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.VAZAO_AFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
        OperationSynthesis(
            Variable.VOLUME_AFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VAZAO_AFLUENTE,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.VAZAO_AFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
        OperationSynthesis(
            Variable.VOLUME_AFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VAZAO_AFLUENTE,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.VAZAO_AFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
        OperationSynthesis(
            Variable.VOLUME_AFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VAZAO_INCREMENTAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.VAZAO_INCREMENTAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
        OperationSynthesis(
            Variable.VOLUME_INCREMENTAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VAZAO_INCREMENTAL,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.VAZAO_INCREMENTAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
        OperationSynthesis(
            Variable.VOLUME_INCREMENTAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VAZAO_INCREMENTAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.VAZAO_INCREMENTAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
        OperationSynthesis(
            Variable.VOLUME_INCREMENTAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VAZAO_DEFLUENTE,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.VAZAO_DEFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VAZAO_DEFLUENTE,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.VAZAO_DEFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VAZAO_DEFLUENTE,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.VAZAO_DEFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VAZAO_VERTIDA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VAZAO_VERTIDA,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VAZAO_VERTIDA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VAZAO_TURBINADA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VAZAO_TURBINADA,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VAZAO_TURBINADA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VAZAO_RETIRADA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.VAZAO_RETIRADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VAZAO_RETIRADA,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.VAZAO_RETIRADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VAZAO_RETIRADA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.VAZAO_RETIRADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VAZAO_DESVIADA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.VAZAO_DESVIADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VAZAO_DESVIADA,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.VAZAO_DESVIADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VAZAO_DESVIADA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.VAZAO_DESVIADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VIOLACAO_EVAPORACAO,
        SpatialResolution.USINA_HIDROELETRICA,
    ): [
        OperationSynthesis(
            Variable.VIOLACAO_POSITIVA_EVAPORACAO,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
        OperationSynthesis(
            Variable.VIOLACAO_NEGATIVA_EVAPORACAO,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VIOLACAO_EVAPORACAO,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.VIOLACAO_EVAPORACAO,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VIOLACAO_EVAPORACAO,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.VIOLACAO_EVAPORACAO,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VIOLACAO_EVAPORACAO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.VIOLACAO_EVAPORACAO,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VIOLACAO_POSITIVA_EVAPORACAO,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.VIOLACAO_POSITIVA_EVAPORACAO,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VIOLACAO_POSITIVA_EVAPORACAO,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.VIOLACAO_POSITIVA_EVAPORACAO,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VIOLACAO_POSITIVA_EVAPORACAO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.VIOLACAO_POSITIVA_EVAPORACAO,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VIOLACAO_NEGATIVA_EVAPORACAO,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.VIOLACAO_NEGATIVA_EVAPORACAO,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VIOLACAO_NEGATIVA_EVAPORACAO,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.VIOLACAO_NEGATIVA_EVAPORACAO,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VIOLACAO_NEGATIVA_EVAPORACAO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.VIOLACAO_NEGATIVA_EVAPORACAO,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
        SpatialResolution.USINA_HIDROELETRICA,
    ): [
        OperationSynthesis(
            Variable.QUEDA_LIQUIDA,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
        SpatialResolution.USINA_HIDROELETRICA,
    ): [
        OperationSynthesis(
            Variable.QUEDA_LIQUIDA,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ),
    ],
    OperationSynthesis(
        Variable.VOLUME_EVAPORADO,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): [
        OperationSynthesis(
            Variable.VOLUME_EVAPORADO,
            SpatialResolution.USINA_HIDROELETRICA,
        )
    ],
    OperationSynthesis(
        Variable.VOLUME_EVAPORADO,
        SpatialResolution.SUBMERCADO,
    ): [
        OperationSynthesis(
            Variable.VOLUME_EVAPORADO,
            SpatialResolution.USINA_HIDROELETRICA,
        )
    ],
    OperationSynthesis(
        Variable.VOLUME_EVAPORADO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): [
        OperationSynthesis(
            Variable.VOLUME_EVAPORADO,
            SpatialResolution.USINA_HIDROELETRICA,
        )
    ],
}

UNITS: dict[OperationSynthesis, Unit] = {
    OperationSynthesis(
        Variable.CUSTO_MARGINAL_OPERACAO, SpatialResolution.SUBMERCADO
    ): Unit.RS_MWh,
    OperationSynthesis(
        Variable.VALOR_AGUA, SpatialResolution.RESERVATORIO_EQUIVALENTE
    ): Unit.RS_MWh,
    OperationSynthesis(
        Variable.VALOR_AGUA, SpatialResolution.USINA_HIDROELETRICA
    ): Unit.RS_hm3,
    OperationSynthesis(
        Variable.VALOR_AGUA_INCREMENTAL,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.RS_hm3,
    OperationSynthesis(
        Variable.CUSTO_GERACAO_TERMICA,
        SpatialResolution.SUBMERCADO,
    ): Unit.MiRS,
    OperationSynthesis(
        Variable.CUSTO_GERACAO_TERMICA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MiRS,
    OperationSynthesis(
        Variable.CUSTO_OPERACAO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MiRS,
    OperationSynthesis(
        Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_RESERVATORIO,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_RESERVATORIO,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_RESERVATORIO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_FIO,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_FIO,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_NATURAL_AFLUENTE_ABSOLUTA_FIO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.perc,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
        SpatialResolution.SUBMERCADO,
    ): Unit.perc,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_PERCENTUAL_INICIAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.perc,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.perc,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
        SpatialResolution.SUBMERCADO,
    ): Unit.perc,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_PERCENTUAL_FINAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.perc,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA_INICIAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.GERACAO_HIDRAULICA_RESERVATORIO,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.GERACAO_HIDRAULICA_RESERVATORIO,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.GERACAO_HIDRAULICA_RESERVATORIO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.GERACAO_HIDRAULICA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.GERACAO_HIDRAULICA,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.GERACAO_HIDRAULICA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.GERACAO_HIDRAULICA_FIO,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.GERACAO_HIDRAULICA_FIO,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.GERACAO_HIDRAULICA_FIO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.GERACAO_TERMICA,
        SpatialResolution.USINA_TERMELETRICA,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.GERACAO_TERMICA,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.GERACAO_TERMICA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA_RESERV,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA_RESERV,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA_RESERV,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA_FIO,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA_FIO,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA_FIO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA_FIO_TURBINAVEL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA_FIO_TURBINAVEL,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_VERTIDA_FIO_TURBINAVEL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_DESVIO_RESERVATORIO,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_DESVIO_RESERVATORIO,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_DESVIO_RESERVATORIO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_DESVIO_FIO,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_DESVIO_FIO,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_DESVIO_FIO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_DEFLUENCIA_MINIMA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_DEFLUENCIA_MINIMA,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_DEFLUENCIA_MINIMA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_VOLUME_MORTO,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_VOLUME_MORTO,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_VOLUME_MORTO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_EVAPORACAO,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_EVAPORACAO,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.ENERGIA_EVAPORACAO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.VIOLACAO_GERACAO_HIDRAULICA_MINIMA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.VIOLACAO_GERACAO_HIDRAULICA_MINIMA,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.VIOLACAO_GERACAO_HIDRAULICA_MINIMA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.VOLUME_TURBINADO,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_TURBINADO,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_TURBINADO,
        SpatialResolution.SUBMERCADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_TURBINADO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_VERTIDO,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_VERTIDO,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_VERTIDO,
        SpatialResolution.SUBMERCADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_VERTIDO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VAZAO_TURBINADA,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_TURBINADA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_TURBINADA,
        SpatialResolution.SUBMERCADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_TURBINADA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_VERTIDA,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_VERTIDA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_VERTIDA,
        SpatialResolution.SUBMERCADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_VERTIDA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_AFLUENTE,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_AFLUENTE,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_AFLUENTE,
        SpatialResolution.SUBMERCADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_AFLUENTE,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_INCREMENTAL,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_INCREMENTAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_INCREMENTAL,
        SpatialResolution.SUBMERCADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_INCREMENTAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VOLUME_AFLUENTE,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_AFLUENTE,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_AFLUENTE,
        SpatialResolution.SUBMERCADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_AFLUENTE,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_INCREMENTAL,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_INCREMENTAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_INCREMENTAL,
        SpatialResolution.SUBMERCADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_INCREMENTAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VAZAO_DEFLUENTE,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_DEFLUENTE,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_DEFLUENTE,
        SpatialResolution.SUBMERCADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_DEFLUENTE,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VOLUME_DEFLUENTE,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_DEFLUENTE,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_DEFLUENTE,
        SpatialResolution.SUBMERCADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_DEFLUENTE,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VAZAO_RETIRADA,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_RETIRADA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_RETIRADA,
        SpatialResolution.SUBMERCADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_RETIRADA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VOLUME_RETIRADO,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_RETIRADO,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_RETIRADO,
        SpatialResolution.SUBMERCADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_RETIRADO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VAZAO_DESVIADA,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_DESVIADA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_DESVIADA,
        SpatialResolution.SUBMERCADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_DESVIADA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VOLUME_DESVIADO,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_DESVIADO,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_DESVIADO,
        SpatialResolution.SUBMERCADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_DESVIADO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
        SpatialResolution.SUBMERCADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
        SpatialResolution.SUBMERCADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.perc,
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.perc,
    OperationSynthesis(
        Variable.GERACAO_HIDRAULICA,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.VELOCIDADE_VENTO,
        SpatialResolution.PARQUE_EOLICO_EQUIVALENTE,
    ): Unit.ms,
    OperationSynthesis(
        Variable.GERACAO_EOLICA,
        SpatialResolution.PARQUE_EOLICO_EQUIVALENTE,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.GERACAO_EOLICA,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.GERACAO_EOLICA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.INTERCAMBIO,
        SpatialResolution.PAR_SUBMERCADOS,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.DEFICIT,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.DEFICIT,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.EXCESSO,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.EXCESSO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.CUSTO_DEFICIT,
        SpatialResolution.SUBMERCADO,
    ): Unit.MiRS,
    OperationSynthesis(
        Variable.CUSTO_DEFICIT,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MiRS,
    OperationSynthesis(
        Variable.MERCADO_LIQUIDO,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.MERCADO_LIQUIDO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.CORTE_GERACAO_EOLICA,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.VIOLACAO_GERACAO_HIDRAULICA_MINIMA,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.VIOLACAO_FPHA,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.VIOLACAO_FPHA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.VIOLACAO_FPHA,
        SpatialResolution.SUBMERCADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.VIOLACAO_FPHA,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWmes,
    OperationSynthesis(
        Variable.COTA_MONTANTE,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.m,
    OperationSynthesis(
        Variable.COTA_JUSANTE,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.m,
    OperationSynthesis(
        Variable.QUEDA_LIQUIDA,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.m,
    OperationSynthesis(
        Variable.VOLUME_EVAPORADO,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_EVAPORADO,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_EVAPORADO,
        SpatialResolution.SUBMERCADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_EVAPORADO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VIOLACAO_EVAPORACAO,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VIOLACAO_EVAPORACAO,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VIOLACAO_EVAPORACAO,
        SpatialResolution.SUBMERCADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VIOLACAO_EVAPORACAO,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.hm3,
}

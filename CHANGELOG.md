# v1.2.0 (v1-compat)

- Versão de compatibilidade com a séries de releases major `1.x`
- Última versão antes da reformulação do formato das sínteses (pré `2.0`)
- Síntese em formato compatível com `pyspark` para ingestão em ambiente analíticos
- Correções diversas em variáveis da síntese da operação e cenários

# v1.1.0

- Compatibilização com `inewave` após a primeira major release (>= 1.4.0)
- Implementadas novas variáveis para síntese da operação: `VAGUA_UHE_EST`, `ENAAR_*_EST`, `ENAAF_*_EST`, `EARMI_*_EST`, `EARPI_*_EST`, `VARMI_*_EST`, `VARPI_UHE_EST`, `GHIDR_*_*`, `GHIDF_*_*`, `GTER_UTE_*`, `EDESR_*_EST`, `EVMIN_*_EST`, `EVMOR_*_EST`, `EEVAP_*_EST`, `VAFL_UHE_EST`, `VINC_UHE_EST`, `VDEF_UHE_*`, `HMON_UHE_EST`, `HJUS_UHE_PAT`, `HLIQ_UHE_PAT`, `VRET_UHE_EST`, `VDES_UHE_*`, `QRET_UHE_EST`, `QDES_UHE_*`
- Habilitado o paralelismo para todas as variáveis da operação, ao invés de apenas variáveis relacionadas a cada UHE
- Otimizado o código para aplicar modificações nos dataframes de maneira posicional, quando conveniente

# v1.0.0

- Primeira major release
- Compatível com `inewave` até a versão 0.0.98 (pré 1ª major release)

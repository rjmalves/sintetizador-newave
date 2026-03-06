import logging
import os
import pathlib
from abc import ABC, abstractmethod
from typing import Dict, Type

import pandas as pd  # type: ignore
import polars as pl
import pyarrow as pa  # type: ignore
import pyarrow.parquet as pq  # type: ignore

from app.utils.tz import enforce_utc

logger = logging.getLogger(__name__)


class AbstractExportRepository(ABC):
    @abstractmethod
    def read_df(self, filename: str) -> pd.DataFrame | None:
        pass

    @abstractmethod
    def synthetize_df(self, df: pd.DataFrame, filename: str) -> bool:
        pass

    def synthetize_pl(self, df: pl.DataFrame, filename: str) -> bool:
        """Default implementation: convert to pandas and use existing path."""
        return self.synthetize_df(df.to_pandas(), filename)


class ParquetExportRepository(AbstractExportRepository):
    def __init__(self, path: str):
        self.__path = path

    @property
    def path(self) -> pathlib.Path:
        return pathlib.Path(self.__path)

    def read_df(self, filename: str) -> pd.DataFrame | None:
        arq = self.path.joinpath(filename + ".parquet")
        if os.path.isfile(arq):
            return pd.read_parquet(arq)
        return None

    def synthetize_df(self, df: pd.DataFrame, filename: str) -> bool:
        pq.write_table(
            pa.Table.from_pandas(enforce_utc(df)),
            self.path.joinpath(filename + ".parquet"),
            write_statistics=False,
            flavor="spark",
            coerce_timestamps="ms",
            allow_truncated_timestamps=True,
        )
        return True

    def synthetize_pl(self, df: pl.DataFrame, filename: str) -> bool:
        """Write Parquet from a Polars DataFrame via PyArrow, bypassing pandas.

        Enforces UTC timezone on all timezone-naive datetime columns before
        writing.  Converts Polars → Arrow → PyArrow table (with pandas metadata
        for round-trip compatibility) and writes with the same ``pq.write_table``
        options used by ``synthetize_df``, producing schema-compatible output
        whose datetime columns are restored as ``datetime64[ns, UTC]`` when
        read back with ``pd.read_parquet``.

        The pandas conversion happens on the already-materialised Arrow table
        (not directly on the Polars frame), which avoids the Polars-level
        pandas conversion in the hot path.

        Falls back to the pandas path (via ``synthetize_df``) if the primary
        write fails.
        """
        # Enforce UTC on timezone-naive datetime columns
        for col_name in df.columns:
            dtype = df[col_name].dtype
            if isinstance(dtype, pl.Datetime) and dtype.time_zone is None:
                df = df.with_columns(
                    pl.col(col_name).dt.replace_time_zone("UTC")
                )
        try:
            # Polars → Arrow (zero-copy where possible).
            # Arrow → pandas is needed only to generate the pandas Parquet
            # metadata that instructs pd.read_parquet to restore UTC dtype.
            arrow_table = pa.Table.from_pandas(df.to_arrow().to_pandas())
            pq.write_table(
                arrow_table,
                self.path.joinpath(filename + ".parquet"),
                write_statistics=False,
                flavor="spark",
                coerce_timestamps="ms",
                allow_truncated_timestamps=True,
            )
            return True
        except Exception:
            logger.warning(
                "synthetize_pl failed for %s; falling back to pandas path",
                filename,
                exc_info=True,
            )
            return self.synthetize_df(df.to_pandas(), filename)


class CSVExportRepository(AbstractExportRepository):
    def __init__(self, path: str):
        self.__path = path

    @property
    def path(self) -> pathlib.Path:
        return pathlib.Path(self.__path)

    def read_df(self, filename: str) -> pd.DataFrame | None:
        arq = self.path.joinpath(filename + ".csv")
        if os.path.isfile(arq):
            return pd.read_csv(arq)
        return None

    def synthetize_df(self, df: pd.DataFrame, filename: str) -> bool:
        enforce_utc(df).to_csv(
            self.path.joinpath(filename + ".csv"), index=False
        )
        return True


class TestExportRepository(AbstractExportRepository):
    def __init__(self, path: str):
        self.__path = path

    @property
    def path(self) -> pathlib.Path:
        return pathlib.Path(self.__path)

    def read_df(self, filename: str) -> pd.DataFrame | None:
        return None

    def synthetize_df(self, df: pd.DataFrame, filename: str) -> bool:
        return df


def factory(kind: str, *args, **kwargs) -> AbstractExportRepository:
    mapping: Dict[str, Type[AbstractExportRepository]] = {
        "PARQUET": ParquetExportRepository,
        "CSV": CSVExportRepository,
        "TEST": TestExportRepository,
    }
    kind = kind.upper()
    if kind not in mapping:
        raise ValueError(f"Formato de síntese: {kind} não suportado")
    return mapping[kind](*args, **kwargs)

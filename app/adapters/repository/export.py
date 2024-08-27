import pathlib
from abc import ABC, abstractmethod
from typing import Dict, Type

import pandas as pd  # type: ignore
import polars as pl

from app.utils.tz import enforce_utc


class AbstractExportRepository(ABC):
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def synthetize_df(self, df: pd.DataFrame, filename: str) -> bool:
        pass


class ParquetExportRepository(AbstractExportRepository):
    def __init__(self, path: str):
        self.__path = path

    @property
    def path(self) -> pathlib.Path:
        return pathlib.Path(self.__path)

    def synthetize_df(self, df: pl.DataFrame, filename: str) -> bool:
        df.write_parquet(
            self.path.joinpath(filename + ".parquet"),
            compression="snappy",
            use_pyarrow=True,
            pyarrow_options={
                "coerce_timestamps": "ms",
                "write_statistics": False,
                "flavor": "spark",
                "allow_truncated_timestamps": True,
            },
        )
        # pq.write_table(
        #     pa.Table.from_pandas(enforce_utc(df)),
        #     self.path.joinpath(filename + ".parquet"),
        #     write_statistics=False,
        #     flavor="spark",
        #     coerce_timestamps="ms",
        #     allow_truncated_timestamps=True,
        # )
        return True


class CSVExportRepository(AbstractExportRepository):
    def __init__(self, path: str):
        self.__path = path

    @property
    def path(self) -> pathlib.Path:
        return pathlib.Path(self.__path)

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

    def synthetize_df(self, df: pd.DataFrame, filename: str) -> bool:
        return df


def factory(kind: str, *args, **kwargs) -> AbstractExportRepository:
    mapping: Dict[str, Type[AbstractExportRepository]] = {
        "PARQUET": ParquetExportRepository,
        "CSV": CSVExportRepository,
        "TEST": TestExportRepository,
    }
    kind = kind.upper()
    if kind not in mapping.keys():
        msg = f"Formato de síntese: {kind} não suportado"
        raise ValueError(msg)
    return mapping.get(kind, ParquetExportRepository)(*args, **kwargs)

import pandas as pd  # type: ignore


def enforce_utc(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
        df[col] = df[col].dt.tz_localize(tz="UTC")
    return df

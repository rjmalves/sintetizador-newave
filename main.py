from sintetizador.app import app
from dotenv import load_dotenv
import os
import pathlib

BASEDIR = pathlib.Path().resolve()
os.environ["APP_INSTALLDIR"] = os.path.dirname(os.path.abspath(__file__))
load_dotenv(
    pathlib.Path(os.getenv("APP_INSTALLDIR")).joinpath("sintese.cfg"),
    override=True,
)
os.environ["APP_BASEDIR"] = str(BASEDIR)
load_dotenv(BASEDIR.joinpath("sintese.cfg"), override=True)

if __name__ == "__main__":
    app()

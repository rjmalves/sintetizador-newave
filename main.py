from sintetizador.app import app
from sintetizador.utils.log import Log
import os
import pathlib


def main():
    BASEDIR = pathlib.Path().resolve()
    os.environ["APP_BASEDIR"] = str(BASEDIR)
    Log.configure_logging(BASEDIR)
    app()


if __name__ == "__main__":
    main()

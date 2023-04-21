from sintetizador.app import app
import os
import pathlib


def main():
    os.environ["APP_INSTALLDIR"] = os.path.dirname(os.path.abspath(__file__))
    BASEDIR = pathlib.Path().resolve()
    os.environ["APP_BASEDIR"] = str(BASEDIR)
    app()


if __name__ == "__main__":
    main()

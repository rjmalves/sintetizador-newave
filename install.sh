#! /bin/bash

VERSION="1.0.0"
DATE="12/09/2022"

echo "Instalação da aplicação sintetizador-newave"
echo "Gerência de Metodologias e Modelos Energéticos - PEM / ONS"
echo "Versão ${VERSION} - ${DATE}"

# Checks if Python 3 is installed
command -v python3 >/dev/null 2>&1
PYTHON3_INSTALLED=$?
if [ $PYTHON3_INSTALLED -ne 0 ]; then
    echo "Python 3 não foi encontrado!"
    exit $PYTHON3_INSTALLED
else
    echo "Instalação do python3 encontrada..."
fi

# Creates installation directory in /tmp
echo "Criando diretório de instalação..." 
USERINSTALLDIR=/home/pem/rotinas
INSTALLDIR=${USERINSTALLDIR}/sintetizador-newave
[ ! -d $INSTALLDIR ] && mkdir -p $INSTALLDIR

# Copies necessary files
echo "Copiando arquivos necessários..."
cp -r sintetizador/ $INSTALLDIR
cp main.py $INSTALLDIR
cp requirements.txt $INSTALLDIR
cp sintetizador-newave $INSTALLDIR
cp sintese.cfg $INSTALLDIR

# Creates venv is not exists
if [ ! -d $INSTALLDIR/venv ]; then
    echo "Criando ambiente virtual..."
    python3 -m venv $INSTALLDIR/venv
else
    echo "Ambiente virtual já existente..."
fi

# Checks if venv was created succesfully
if [ ! -f $INSTALLDIR/venv/bin/activate ]; then
    echo "Falha na criação do ambiente virtual"
    echo "Limpando arquivos da instalação..."
    rm -r $INSTALLDIR
    exit 2
else
    echo "Ambiente virtual criado com sucesso"
fi

# Activates venv and installs requirements
CURDIR=$(pwd)
cd $INSTALLDIR
echo "Ativando ambiente virtual..."
source venv/bin/activate
VENV_ACTIVATE=$?
if [ $VENV_ACTIVATE -ne 0 ]; then
    echo "Não foi possível ativar o ambiente virtual"
    exit $VENV_ACTIVATE
else
    echo "Instalando dependências..."
    pip install -r requirements.txt
fi

# Copies the executable to a folder in the system's PATH
EXECPATH=/usr/bin/sintetizador-newave
echo "Copiando executável para ${EXECPATH}" 
cp sintetizador-newave $EXECPATH

# Deactivates venv
echo "Finalizando instalação..."
deactivate
cd $CURDIR
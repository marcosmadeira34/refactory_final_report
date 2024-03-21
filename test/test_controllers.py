from scripts.controllers import FinalReport
import pandas as pd
from unittest.mock import MagicMock, patch
from scripts.controllers import FinalReport
import tempfile

host_postgres = 'postgresql://postgres:123456789@localhost:5432/postgres'
instance = FinalReport(host_postgres)


def test_padronizar_nome_colunas():    
    df = pd.DataFrame({'Nome do$Cliente': ['joao', 'maria', 'jose'], 'Idade do Cliente': [20, 30, 40]})
    instance.padronizar_nomes_colunas(df)
    expected_columns = ['nome_do_cliente', 'idade_do_cliente']
    assert list(df.columns) == expected_columns


def test_formatar_cnpj():
    # cnpj sem formatação
    cnpj = '12345678000190'
    # chamada da função e armazenamento do retorno
    cnpj_formatado = instance.formatar_cnpj(cnpj)
    expected_cnpj = '12.345.678/0001-90'
    assert cnpj_formatado == expected_cnpj


def test_corrigir_valor_faturamento():
    # valor original como string
    valor = '10.001.037,16'
    # chamada da função e armazenamento do retorno
    valor_corrigido = instance.corrigir_valor_faturamento(valor)
    # valor esperado
    expected_valor = 10001037.16
    # teste de igualdade
    assert valor_corrigido == expected_valor



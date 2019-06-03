#!/usr/bin/python

# Ambiente Spark
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import regexp_extract

# Outras bibliotecas
import glob
import pandas as pd
import re
import click

@click.command()
@click.option('--limit', default=1, help="Número de registros a processar..")
def run(limit):
    # Configura algumas coisas...
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    spark = SparkSession(sc)

    # Limpa a tela antes de começasr
    click.clear()

    click.echo("--> Análise de dados: Python + Spark.")

    # Lê os arquicos .gz no diretório
    raw_data_files = glob.glob('*.gz')
    # Imprime na tela os arquivos
    click.echo("Foram encontrados os arquivos *.gz: %s" % str(raw_data_files))

    # Para rodar o script para os dois arquivos
    base_df = spark.read.text(raw_data_files) #-> Essa linha roda os dois arquivos

    # Expressões regulares para os Hosts
    host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'

    # Expressões regulares para os times
    ts_pattern = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'

    # Expressões regulares para os métodos
    method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'

    # Expressões regulares para o status
    status_pattern = r'\s(\d{3})\s'

    # Expressões regulares para o content_size
    content_size_pattern = r'\s(\d+)$'

    # Junta todos os dados em um dataframe
    click.echo("Criando dataset...")
    logs_df = base_df.select(regexp_extract('value', host_pattern, 1).alias('host'),
         regexp_extract('value', ts_pattern, 1).alias('timestamp'),
         regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
         regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
         regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
         regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
         regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'))

    click.echo("A quantidade de hosts distintos: %s" % (logs_df.select("host").distinct().count()))

# Executa o script
if __name__ == '__main__':
    run()

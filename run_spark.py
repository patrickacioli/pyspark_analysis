#!/usr/bin/python

# Ambiente Spark
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import col
from pyspark.sql import functions as F

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

    click.echo("########################################")
    click.echo("### Análise de dados: Python + Spark ###")
    click.echo("########################################")

    # Lê os arquicos .gz no diretório
    raw_files = glob.glob('*.gz')
    # Imprime na tela os arquivos
    click.echo("Arquivos do dataset : %s" % str(raw_files))

    # Para rodar o script para os dois arquivos
    df = spark.read.text(raw_files) #-> Essa linha roda os dois arquivos

    # Expressões regulares para os Hosts
    host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'

    # Expressões regulares para os times
    timestamp_pattern = r'\[(\d{2}/\w{3}/\d{4})'

    # Expressões regulares para os métodos
    method_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'

    # Expressões regulares para o status
    status_pattern = r'\s(\d{3})\s'

    # Expressões regulares para o content_size
    bytes_pattern = r'\s(\d+)$'

    # Junta todos os dados em um dataframe
    click.echo("Criando dataset...")
    logs = df.select(regexp_extract('value', host_pattern, 1).alias('host'),
         regexp_extract('value', timestamp_pattern, 1).alias('timestamp'),
         regexp_extract('value', method_pattern, 1).alias('method'),
         regexp_extract('value', method_pattern, 2).alias('endpoint'),
         regexp_extract('value', method_pattern, 3).alias('protocol'),
         regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
         regexp_extract('value', bytes_pattern, 1).cast('integer').alias('content_size'))

    # Errors 404
    logs_404 = logs.select("status").where(logs.status == 404)
    # Hosts distintos
    distinct_hosts = logs.select("host").distinct()
    # Top5 erros 404
    top5 = logs.select("endpoint").where(logs.status == 404).groupby("endpoint").count().sort(col("count").desc())
    # Agrupamento por dia
    by_day = logs.select(["status", "timestamp"]).where(logs.status == 404).groupby("timestamp").count().sort(col("timestamp").asc())
    # total de bytes retornados
    logs = logs.na.fill({'content_size': 0}) # Existem colunas com valores null, então preencha com zeros
    total_bytes = logs.groupBy().agg(F.sum("content_size")).collect()[0][0]


    ###
    ## Imprime as respostas
    ###
    click.echo("1 - Quantidade de hosts distintos: %s" % (distinct_hosts.count()))
    click.echo("2 - Errors 404: %s" % (logs_404.count()))
    click.echo("3 - Top5 errors 404: ")
    top5.show(5)
    click.echo("4 - Errors 404 por dia: ")
    by_day.show(60)
    click.echo("5 - O total de bytes retornados foram: %s" % total_bytes)

# Executa o script
if __name__ == '__main__':
    run()

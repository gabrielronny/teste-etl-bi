from pyspark import SparkConf
from pyspark.sql import SparkSession

import utils 

from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col
from pyspark.sql.functions import split
from pyspark.sql.functions import monotonically_increasing_id
import pandas as pd

conf = SparkConf()
conf.setAppName('etl-bi-neoway')
conf.set('spark.mongodb.read.connection.uri', 'mongodb+srv://etl-neoway:pip20po0@mongo-teste.1xrdbcs.mongodb.net/?retryWrites=true&w=majority')
conf.set('spark.mongodb.write.connection.uri', 'mongodb+srv://etl-neoway:pip20po0@mongo-teste.1xrdbcs.mongodb.net/?retryWrites=true&w=majority')
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.2,com.microsoft.azure:spark-mssql-connector_2.12:1.2.0')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.InstanceProfileCredentialsProvider')

spark = SparkSession.builder.config(conf=conf).getOrCreate()

def carga_empresas_azure():
    df_empresas = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[0].get("paths_origem_bases")}/empresas.csv')

    # Realizando a carga da base empresas no SQL Server Azure

    df_empresas.write.mode("overwrite") \
        .format("jdbc") \
        .option('url', f'{utils.getCredenciais()[0].get("url_db_azure")}') \
        .option("dbtable", 'empresas') \
        .option("user", f'{utils.getCredenciais()[0].get("user")}') \
        .option("password", f'{utils.getCredenciais()[0].get("pass")}') \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

    # Removendo dataset memória
    df_empresas.unpersist()
    
    print('Carga realizada')


def extract_empresas_azure():   

    df_empresas_azure = spark.read.format("jdbc") \
        .option('url', f'{utils.getCredenciais()[0].get("url_db_azure")}') \
        .option("dbtable", 'empresas') \
        .option("user", f'{utils.getCredenciais()[0].get("user")}') \
        .option("password", f'{utils.getCredenciais()[0].get("pass")}') \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()
     

    df_empresas_azure_pd = df_empresas_azure.toPandas()
    df_empresas_azure_pd.to_csv(f"{utils.getPaths()[1].get('extract')}/empresas.csv", sep=";", index=False)

def extract_cnae():
    df_cnaes = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[0].get("paths_origem_bases")}/CNAE.csv')

    df_cnaes = df_cnaes.toPandas()
        
    df_cnaes.to_csv(f"{utils.getPaths()[1].get('extract')}/cnae.csv", sep=";", index=False)

def extract_empresas_calc():
    df_empresas_calculos = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[0].get("paths_origem_bases")}/empresasCalc.csv')

    df_empresas_calculos = df_empresas_calculos.toPandas()
        
    df_empresas_calculos.to_csv(f"{utils.getPaths()[1].get('extract')}/empresas_calculos.csv", sep=";", index=False)

def extract_setores_e_ramos_de_atividades():
    df_setores_e_ramos_de_atividades = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[0].get("paths_origem_bases")}/Setores e Ramos de Atividade.csv')
    
    df_setores_e_ramos_de_atividades = df_setores_e_ramos_de_atividades.toPandas()
        
    df_setores_e_ramos_de_atividades.to_csv(f"{utils.getPaths()[1].get('extract')}/setores_e_ramos_de_atividades.csv", sep=";", index=False)


## Tranformando arquivos

def transform_setores_e_ramos_de_atividades():

    df_setores_e_ramos_de_atividades_ld = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[1].get("extract")}/setores_e_ramos_de_atividades.csv')

    df_setores_e_ramos_de_atividades_ld.createOrReplaceTempView('temp_setores_e_ramos_de_atividades')

    df_setores_tratado = spark.sql("""

        SELECT
            case 
                when a.divisao_2_digitos_cnae = 46 then 'ATACADO'
                when a.divisao_2_digitos_cnae in (29, 30) then 'INDUSTRIA AUTOMOTIVA'
                when a.divisao_2_digitos_cnae in (64, 65, 66) then 'BENS DE CAPITAL'
                when a.divisao_2_digitos_cnae in (10, 11, 12, 14, 15, 16, 31) then 'BENS DE CONSUMO'
                when a.divisao_2_digitos_cnae = 32 then 'DIVERSOS'
                when a.divisao_2_digitos_cnae in (26, 27) then 'ELETROELETRONICOS'
                when a.divisao_2_digitos_cnae = 35 then 'ENERGIA'
                when a.divisao_2_digitos_cnae = 21 then 'FARMACEUTICA'
                when a.divisao_2_digitos_cnae in (41, 42, 43) then 'INDUSTRIA DA CONSTRUCAO'
                when a.divisao_2_digitos_cnae in (62, 63) then 'INDUSTRIA DIGITAL'
                when a.divisao_2_digitos_cnae in (5, 6, 7, 8, 9) then 'MINERACAO'
                when a.divisao_2_digitos_cnae in (17, 18) then 'PAPEL E CELULOSE'
                when a.divisao_2_digitos_cnae in (1, 2, 3) then 'PRODUTOS DE AGROPECUARIA'
                when a.divisao_2_digitos_cnae in (19, 20, 22, 23) then 'QUIMICA-PETROQUIMICA'
                when a.divisao_2_digitos_cnae in (24, 25, 28) then 'SIDERURGICA-METALURGIA'
                when a.divisao_2_digitos_cnae in (58, 59, 60, 61) then 'TELECOM'
                when a.divisao_2_digitos_cnae = 13 then 'TEXTEIS'
                when a.divisao_2_digitos_cnae in (49, 50, 51, 52, 53) then 'TRANSPORTE'
                when a.divisao_2_digitos_cnae in (45, 47) then 'VAREJO'
                when a.divisao_2_digitos_cnae in (36, 37, 38, 39) then 'SERVICOS DE SANEAMENTO BASICO'
                when a.divisao_2_digitos_cnae in (55, 56) then 'SERVICOS DE ALOJAMENTO/ALIMENTACAO'
                when a.divisao_2_digitos_cnae in (68, 77, 80, 78, 79, 81, 82) then 'SERVICOS ADMINISTRATIVOS'
                when a.divisao_2_digitos_cnae in (69, 70, 95, 71, 72, 73, 74, 75) then 'SERVICOS PROFISSIONAIS E TECNICOS'
                when a.divisao_2_digitos_cnae = 85 then 'SERVICOS DE EDUCACAO'
                when a.divisao_2_digitos_cnae in (86, 87, 88) then 'SERVICOS DE SAUDE'
                when a.divisao_2_digitos_cnae in (33, 90, 91, 92, 93, 94, 96, 97, 99, 84) then 'SERVICOS DIVERSOS'
            end as ramo_de_atividade,
            a.divisoes,
            a.divisao_2_digitos_cnae
        FROM (
            SELECT 
                `Ramo de Atividade`                           as ramo_de_atividade,
                `Divisões`                                    as divisoes,
                cast(`(DIVISÃO - 2digitos CNAE)` as bigint)   as divisao_2_digitos_cnae,
                F4,
                F5,
                F6,
                F7
            FROM temp_setores_e_ramos_de_atividades
        ) as a
    """).toPandas()

    df_setores_tratado.to_csv(f"{utils.getPaths()[1].get('transform')}/setores_e_ramos_de_atividades.csv", sep=";", index=False)

    spark.catalog.dropTempView("temp_setores_e_ramos_de_atividades")
    
def transform_empresas_calculo():

    df_empresas_calculo_ld = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[1].get("extract")}/empresas_calculos.csv')

    df_empresas_calculo_ld.createOrReplaceTempView('temp_empresas_calculo')

    df_empresas_calculo_tratado = spark.sql("""
         SELECT 
            a.cod_cnpj,
            a.latitude,
            a.longitude,
            a.qtd_total_veiculos_antt,
            a.qtd_total_veiculos_leves,
            a.qtd_total_veiculos_pesados,
            case 
                when (qtd_total_veiculos_leves + qtd_total_veiculos_pesados) >= 0 and (qtd_total_veiculos_leves + qtd_total_veiculos_pesados) <= 5   then '0 a 5'
                when (qtd_total_veiculos_leves + qtd_total_veiculos_pesados) >= 6 and (qtd_total_veiculos_leves + qtd_total_veiculos_pesados) <= 10  then '6 a 10'
                when (qtd_total_veiculos_leves + qtd_total_veiculos_pesados) >= 11 and (qtd_total_veiculos_leves + qtd_total_veiculos_pesados) <= 15 then '11 a 15'
                when (qtd_total_veiculos_leves + qtd_total_veiculos_pesados) >= 16 and (qtd_total_veiculos_leves + qtd_total_veiculos_pesados) <= 30 then '16 a 30'
                when (qtd_total_veiculos_leves + qtd_total_veiculos_pesados) >= 31 and (qtd_total_veiculos_leves + qtd_total_veiculos_pesados) <= 50 then '31 a 50'
                when (qtd_total_veiculos_leves + qtd_total_veiculos_pesados) > 50 then '> 50'
            end as faixa_qtde_veiculos,
            a.flag_optante_simei,
            a.saude_tributaria,
            a.nivel_atividade
        FROM (
            SELECT 
                cast(`cd_cnpj` as string)                                         as cod_cnpj,
                `vl_latitude`                                                     as latitude,
                `vl_longitude`                                                    as longitude,
                cast(coalesce(`vl_total_veiculos_antt`, 0) as bigint)             as qtd_total_veiculos_antt,
                cast(coalesce(`vl_total_veiculos_leves`, 0) as bigint)            as qtd_total_veiculos_leves,
                cast(coalesce(`vl_total_veiculos_pesados`, 0)  as bigint)         as qtd_total_veiculos_pesados,
                cast(coalesce(`qt_filial`, 0) as bigint)                          as qtd_filial,
                case
                    when `fl_optante_simei` = 1 then 'SIM'
                    when `fl_optante_simei` = 0 then 'NÃO'
                end as flag_optante_simei,
                coalesce(`de_saude_tributaria`, 'SEM INFORMACAO')                 as saude_tributaria,
                `de_nivel_atividade`                                              as nivel_atividade
            FROM temp_empresas_calculo  
        ) as a
    """).toPandas()

  
    df_empresas_calculo_tratado.to_csv(f"{utils.getPaths()[1].get('transform')}/empresas_calculos.csv", sep=";", index=False)

    spark.catalog.dropTempView("temp_empresas_calculo")


def transform_cnae():

    df_cnae = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[1].get("extract")}/cnae.csv')

    df_cnae.createOrReplaceTempView('temp_cnae')
    
    df_cnae_tratado = spark.sql("""
        select
            cd_cnpj,
            split(de_ramo_atividade,  '~@~')[0] as de_ramo_atividade_principal,
            split(cd_ramo_atividade,  '~@~')[0] as cod_ramo_atividade_principal,
            cd_ramo_atividade,
            nu_ordem,
            de_ramo_atividade
        from temp_cnae
    """).toPandas()

    df_cnae_tratado.to_csv(f"{utils.getPaths()[1].get('transform')}/cnae.csv", sep=";", index=False)

    spark.catalog.dropTempView("temp_cnae")

def transform_empresas():

    df_empresas = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[1].get("extract")}/empresas.csv')

    df_empresas.createOrReplaceTempView('temp_empresas')

    df_empresas_tratado = spark.sql("""
        SELECT 
            cd_cnpj,
            case 
                when fl_matriz = 1 then 'SIM'
                when fl_matriz = 0 then 'NÃO'
            end as flag_matriz,
            dt_abertura,
            nm_razao_social,
            cd_natureza_juridica,
            de_natureza_juridica,
            coalesce(nm_logradouro, 'SEM INFORMACAO') as nm_logradouro,
            coalesce(cd_cep, 'SEM INFORMACAO') as cd_cep,
            coalesce(nm_bairro, 'SEM INFORMACAO') as nm_bairro,
            coalesce(nm_municipio, 'SEM INFORMACAO') as nm_municipio,
            coalesce(sg_uf, 'SEM INFORMACAO') as sg_uf, 
            de_situacao,
            de_classif_natureza_juridica
        FROM temp_empresas
    """).toPandas()

    df_empresas_tratado.to_csv(f"{utils.getPaths()[1].get('transform')}/empresas.csv", sep=";", index=False)

    spark.catalog.dropTempView("temp_empresas")



def criar_dimensoes():

    df_empresas = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[1].get("transform")}/empresas.csv')

    df_empresas.createOrReplaceTempView('temp_empresas')

    df_empresas_calculo = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[1].get("transform")}/empresas_calculos.csv')

    df_empresas_calculo.createOrReplaceTempView('temp_empresas_calculos')

    df_setores_e_ramos_atividades = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[1].get("transform")}/setores_e_ramos_de_atividades.csv')

    df_setores_e_ramos_atividades.createOrReplaceTempView('temp_setores_ramos_atividades')

    df_cnae = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[1].get("transform")}/cnae.csv')

    df_cnae.createOrReplaceTempView('temp_cnae')


    # Criando a dim UF
    dim_uf = spark.sql("""
        SELECT
            ROW_NUMBER() OVER (ORDER BY sg_uf) as id_uf,
            `sg_uf` as desc_uf
        FROM temp_empresas
        GROUP BY sg_uf
    """).toPandas()

    dim_uf.to_csv(f"{utils.getPaths()[1].get('load')}/dim_uf.csv", sep=";", index=False)


    # Criando a dim Bairro
    dim_bairro = spark.sql("""
        SELECT
            ROW_NUMBER() OVER (ORDER BY nm_bairro) as id_bairro,
            `nm_bairro` as desc_bairro
        FROM temp_empresas
        GROUP BY nm_bairro
    """).toPandas()

    dim_bairro.to_csv(f"{utils.getPaths()[1].get('load')}/dim_bairro.csv", sep=";", index=False)


    # Criando a dim Bairro
    dim_municipio = spark.sql("""
        SELECT
            ROW_NUMBER() OVER (ORDER BY nm_municipio) as id_municipio,
            `nm_municipio` as desc_municipio
        FROM temp_empresas
        GROUP BY nm_municipio
    """).toPandas()

    dim_municipio.to_csv(f"{utils.getPaths()[1].get('load')}/dim_municipio.csv", sep=";", index=False)

    # Criando a dim logradouro
    dim_logradouro = spark.sql("""
        SELECT
            ROW_NUMBER() OVER (ORDER BY nm_logradouro) as id_logradouro,
            `nm_logradouro` as desc_logradouro
        FROM temp_empresas
        GROUP BY desc_logradouro
    """).toPandas()

    dim_logradouro.to_csv(f"{utils.getPaths()[1].get('load')}/dim_logradouro.csv", sep=";", index=False)

    # Criando a dim Natureza Juridica
    dim_natureza_juridica = spark.sql("""
        SELECT
            ROW_NUMBER() OVER (ORDER BY de_natureza_juridica) as id_natureza_juridica,
            `de_natureza_juridica` as desc_natureza_juridica,
            `cd_natureza_juridica` as cod_natureza_juridica
        FROM temp_empresas
        GROUP BY de_natureza_juridica, cod_natureza_juridica
    """).toPandas()

    dim_natureza_juridica.to_csv(f"{utils.getPaths()[1].get('load')}/dim_natureza_juridica.csv", sep=";", index=False)


    # Criando a dim Classificacao Natureza Juridica
    dim_natureza_class_juridica = spark.sql("""
        SELECT
            ROW_NUMBER() OVER (ORDER BY de_classif_natureza_juridica) as id_classificacao_natureza_juridica,
            `de_classif_natureza_juridica` as desc_classificacao_natureza_juridica
        FROM temp_empresas
        GROUP BY de_classif_natureza_juridica
    """).toPandas()

    dim_natureza_class_juridica.to_csv(f"{utils.getPaths()[1].get('load')}/dim_natureza_classificacao_juridica.csv", sep=";", index=False)


    # Criando a dim Natureza Juridica
    dim_natureza_class_juridica = spark.sql("""
        SELECT
            ROW_NUMBER() OVER (ORDER BY de_classif_natureza_juridica) as id_classificacao_natureza_juridica,
            `de_classif_natureza_juridica` as desc_classificacao_natureza_juridica
        FROM temp_empresas
        GROUP BY de_classif_natureza_juridica
    """).toPandas()

    dim_natureza_class_juridica.to_csv(f"{utils.getPaths()[1].get('load')}/dim_natureza_classificacao_juridica.csv", sep=";", index=False)

    # Criando a dim Situacao
    dim_natureza_class_juridica = spark.sql("""
        SELECT
            ROW_NUMBER() OVER (ORDER BY de_situacao) as id_situacao,
            `de_situacao` as desc_situacao
        FROM temp_empresas
        GROUP BY de_situacao
    """).toPandas()

    dim_natureza_class_juridica.to_csv(f"{utils.getPaths()[1].get('load')}/dim_situacao.csv", sep=";", index=False)

    # Criando a dim Saude Tributária
    dim_saude_tributaria = spark.sql("""
        SELECT
            ROW_NUMBER() OVER (ORDER BY saude_tributaria) as id_saude_tributaria,
            `saude_tributaria` as desc_saude_tributaria
        FROM temp_empresas_calculos
        GROUP BY saude_tributaria
    """).toPandas()

    dim_saude_tributaria.to_csv(f"{utils.getPaths()[1].get('load')}/dim_saude_tributaria.csv", sep=";", index=False)

    # Criando a dim Faixa Quantidade Veículos
    dim_faixa_qtde_veiculos = spark.sql("""
        SELECT
            ROW_NUMBER() OVER (ORDER BY faixa_qtde_veiculos) as id_faixa_qtde_veiculos,
            faixa_qtde_veiculos
        FROM temp_empresas_calculos
        GROUP BY faixa_qtde_veiculos
    """).toPandas()

    dim_faixa_qtde_veiculos.to_csv(f"{utils.getPaths()[1].get('load')}/dim_faixa_qtde_veiculos.csv", sep=";", index=False)

    # Criando a dim Nivel atividade
    dim_faixa_nivel_atividade = spark.sql("""
        SELECT
            ROW_NUMBER() OVER (ORDER BY nivel_atividade) as id_nivel_atividade,
            nivel_atividade
        FROM temp_empresas_calculos
        GROUP BY nivel_atividade
    """).toPandas()

    dim_faixa_nivel_atividade.to_csv(f"{utils.getPaths()[1].get('load')}/dim_nivel_atividade.csv", sep=";", index=False)


    # Criando a dim Setor e
    dim_setor = spark.sql("""
        SELECT
            ROW_NUMBER() OVER (ORDER BY ramo_de_atividade) as id_setor,
            ramo_de_atividade as desc_setor
        FROM temp_setores_ramos_atividades
        GROUP BY ramo_de_atividade
    """).toPandas()

    dim_setor.to_csv(f"{utils.getPaths()[1].get('load')}/dim_setor.csv", sep=";", index=False)

    # Criando a dim Ramo
    dim_ramo = spark.sql("""
        SELECT
            ROW_NUMBER() OVER (ORDER BY divisoes) as id_ramo,
            `divisoes` as desc_ramo,
            `divisao_2_digitos_cnae` as digito_cnae
        FROM temp_setores_ramos_atividades
        GROUP BY divisoes, divisao_2_digitos_cnae
    """).toPandas()

    dim_ramo.to_csv(f"{utils.getPaths()[1].get('load')}/dim_ramo.csv", sep=";", index=False)

    # Criando a dim CNAE Principal
    dim_ramo = spark.sql("""
        SELECT
            ROW_NUMBER() OVER (ORDER BY de_ramo_atividade_principal) as id_cnae_principal,
            `de_ramo_atividade_principal` as desc_cnae_principal
        FROM temp_cnae
        GROUP BY desc_cnae_principal
    """).toPandas()

    dim_ramo.to_csv(f"{utils.getPaths()[1].get('load')}/dim_cnae_principal.csv", sep=";", index=False)

    spark.catalog.dropTempView("temp_empresas")
    spark.catalog.dropTempView("temp_empresas_calculo")
    spark.catalog.dropTempView("temp_setores_ramos_atividades")
    spark.catalog.dropTempView("temp_cnae")

def criar_fato():

    df_empresas = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[1].get("transform")}/empresas.csv')

    df_empresas.createOrReplaceTempView('temp_empresas')

    df_empresas_calculo = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[1].get("transform")}/empresas_calculos.csv')

    df_empresas_calculo.createOrReplaceTempView('temp_empresas_calculos')

    df_setores_e_ramos_atividades = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[1].get("transform")}/setores_e_ramos_de_atividades.csv')

    df_setores_e_ramos_atividades.createOrReplaceTempView('temp_setores_ramos_atividades')

    df_cnae = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[1].get("transform")}/cnae.csv')

    df_cnae.createOrReplaceTempView('temp_cnae')

    df_dim_uf = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[1].get("load")}/dim_uf.csv')

    df_dim_uf.createOrReplaceTempView('temp_dim_uf')

    df_dim_bairro = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[1].get("load")}/dim_bairro.csv')

    df_dim_bairro.createOrReplaceTempView('temp_dim_bairro')

    df_dim_logradouro = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[1].get("load")}/dim_logradouro.csv')

    df_dim_logradouro.createOrReplaceTempView('temp_dim_logradouro')

    df_dim_municipio = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[1].get("load")}/dim_municipio.csv')

    df_dim_municipio.createOrReplaceTempView('temp_dim_municipio')

    df_dim_natureza_class_juridica = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[1].get("load")}/dim_natureza_classificacao_juridica.csv')

    df_dim_natureza_class_juridica.createOrReplaceTempView('temp_dim_nat_calss_juridica')

    df_dim_natureza_juridica = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[1].get("load")}/dim_natureza_juridica.csv')

    df_dim_natureza_juridica.createOrReplaceTempView('temp_dim_nat_juridica')

    df_dim_situacao = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[1].get("load")}/dim_situacao.csv')

    df_dim_situacao.createOrReplaceTempView('temp_dim_situacao')

    df_dim_faixa_qtde_veiculos = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[1].get("load")}/dim_faixa_qtde_veiculos.csv')

    df_dim_faixa_qtde_veiculos.createOrReplaceTempView('temp_dim_faixa_qtde_veiculos')

    df_dim_saude_tributaria = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[1].get("load")}/dim_saude_tributaria.csv')

    df_dim_saude_tributaria.createOrReplaceTempView('temp_dim_saude_tributaria')

    df_dim_nivel_atividade = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[1].get("load")}/dim_nivel_atividade.csv')

    df_dim_nivel_atividade.createOrReplaceTempView('temp_dim_nivel_atividade')
    
    df_dim_cnae_principal = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[1].get("load")}/dim_cnae_principal.csv')

    df_dim_cnae_principal.createOrReplaceTempView('temp_dim_cnae_principal')

    df_dim_setor = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[1].get("load")}/dim_setor.csv')

    df_dim_setor.createOrReplaceTempView('temp_dim_setor')

    df_cnae_principal = spark.sql("""
        SELECT 
            cn.cd_cnpj,
            cnp.desc_cnae_principal,
            cnp.id_cnae_principal,
            substring(cn.cod_ramo_atividade_principal, 1, 2),
            rset.ramo_de_atividade,
            set.id_setor
        FROM temp_cnae as cn
        LEFT JOIN temp_dim_cnae_principal cnp
            ON cn.de_ramo_atividade_principal = cnp.desc_cnae_principal
        LEFT JOIN temp_setores_ramos_atividades as rset
            ON substring(cn.cod_ramo_atividade_principal, 1, 2) = rset.divisao_2_digitos_cnae
        LEFT JOIN temp_dim_setor as set
            ON rset.ramo_de_atividade = set.desc_setor
    """).createOrReplaceTempView('temp_setor')

    df_fato_empresa = spark.sql("""
        SELECT
            emp.cd_cnpj,
            emp.flag_matriz,
            emp.dt_abertura,
            emp.nm_razao_social,
            nj.id_natureza_juridica,
            ncj.id_classificacao_natureza_juridica,
            uf.id_uf,
            ba.id_bairro,
            mu.id_municipio,
            lg.id_logradouro,
            sit.id_situacao,
            emp_calc.longitude,
            emp_calc.latitude,
            emp_calc.qtd_total_veiculos_antt,
            emp_calc.qtd_total_veiculos_leves,
            emp_calc.qtd_total_veiculos_pesados,
            emp_calc.qtd_total_veiculos_pesados,
            fx_veic.id_faixa_qtde_veiculos,
            emp_calc.flag_optante_simei,
            st.id_saude_tributaria,
            na.id_nivel_atividade,
            set.id_setor
        FROM temp_empresas as emp
        LEFT JOIN temp_dim_nat_juridica as nj
            ON nj.desc_natureza_juridica = emp.de_natureza_juridica
        LEFT JOIN temp_dim_nat_calss_juridica as ncj
            ON ncj.desc_classificacao_natureza_juridica = emp.de_classif_natureza_juridica 
        LEFT JOIN temp_dim_uf as uf
            ON uf.desc_uf = emp.sg_uf
        LEFT JOIN temp_dim_bairro as ba
            ON ba.desc_bairro = emp.nm_bairro
        LEFT JOIN temp_dim_municipio as mu
            ON mu.desc_municipio = emp.nm_municipio
        LEFT JOIN temp_dim_logradouro as lg
            ON lg.desc_logradouro = emp.nm_logradouro
        LEFT JOIN temp_dim_situacao as sit
            ON sit.desc_situacao = emp.de_situacao
        LEFT JOIN temp_empresas_calculos as emp_calc
            ON emp_calc.cod_cnpj = emp.cd_cnpj
        LEFT JOIN temp_dim_faixa_qtde_veiculos as fx_veic
            ON fx_veic.faixa_qtde_veiculos = emp_calc.faixa_qtde_veiculos
        LEFT JOIN temp_dim_saude_tributaria as st
            ON st.desc_saude_tributaria = emp_calc.saude_tributaria
        LEFT JOIN temp_dim_nivel_atividade na
            ON na.nivel_atividade = emp_calc.nivel_atividade
        LEFT JOIN temp_setor as set
            ON set.cd_cnpj = emp.cd_cnpj
    """).toPandas()

    df_fato_empresa.to_csv(f"{utils.getPaths()[1].get('load')}/fato_empresa.csv", sep=";", index=False)

if __name__ == '__main__':
    carga_empresas_azure()
    extract_empresas_azure()
    extract_cnae()
    extract_empresas_calc()
    extract_setores_e_ramos_de_atividades()
    transform_setores_e_ramos_de_atividades()
    transform_empresas_calculo()
    transform_cnae()
    transform_empresas()
    criar_dimensoes()
    criar_fato()

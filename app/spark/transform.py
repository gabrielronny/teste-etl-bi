import utils 

def transform_setores_e_ramos_de_atividades(spark):

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

    print('Transformação Setores e ramo de atividades realizada')
    
def transform_empresas_calculo(spark):

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

    print('Transformação Empresas Calculos realizada')

def transform_cnae(spark):

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

    print('Transformação CNAE realizada')

def transform_empresas(spark):
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


    print('Transformação Empresas realizada')
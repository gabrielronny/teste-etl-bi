import utils 

def criar_dimensoes(spark):

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

    print('Criação das Dimensoes realizada')

def criar_fato(spark):

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

    df_dim_ramo = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[1].get("load")}/dim_ramo.csv')

    df_dim_ramo.createOrReplaceTempView('temp_dim_ramo')

    df_cnae_principal = spark.sql("""
        SELECT 
            cn.cd_cnpj,
            cnp.desc_cnae_principal,
            cnp.id_cnae_principal,
            substring(cn.cod_ramo_atividade_principal, 1, 2),
            rset.ramo_de_atividade,
            ramo.id_ramo,
            set.id_setor
        FROM temp_cnae as cn
        LEFT JOIN temp_dim_cnae_principal cnp
            ON cn.de_ramo_atividade_principal = cnp.desc_cnae_principal
        LEFT JOIN temp_setores_ramos_atividades as rset
            ON substring(cn.cod_ramo_atividade_principal, 1, 2) = rset.divisao_2_digitos_cnae
        LEFT JOIN temp_dim_ramo as ramo
            ON substring(cn.cod_ramo_atividade_principal, 1, 2) = ramo.digito_cnae
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
            set.id_setor,
            set.id_cnae_principal,
            set.id_ramo
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

    print('Criação da Fato realizada')
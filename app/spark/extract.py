import utils 

def carga_empresas_azure(spark):
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


def extract_empresas_azure(spark):   

    df_empresas_azure = spark.read.format("jdbc") \
        .option('url', f'{utils.getCredenciais()[0].get("url_db_azure")}') \
        .option("dbtable", 'empresas') \
        .option("user", f'{utils.getCredenciais()[0].get("user")}') \
        .option("password", f'{utils.getCredenciais()[0].get("pass")}') \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()

    print('Extração Empresas na Azure realizada')
     

    df_empresas_azure_pd = df_empresas_azure.toPandas()
    df_empresas_azure_pd.to_csv(f"{utils.getPaths()[1].get('extract')}/empresas.csv", sep=";", index=False)

def extract_cnae(spark):
    df_cnaes = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[0].get("paths_origem_bases")}/CNAE.csv')

    df_cnaes = df_cnaes.toPandas()
        
    df_cnaes.to_csv(f"{utils.getPaths()[1].get('extract')}/cnae.csv", sep=";", index=False)

    print('Extração CNAE realizada')

def extract_empresas_calc(spark):
    df_empresas_calculos = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[0].get("paths_origem_bases")}/empresasCalc.csv')

    df_empresas_calculos = df_empresas_calculos.toPandas()
        
    df_empresas_calculos.to_csv(f"{utils.getPaths()[1].get('extract')}/empresas_calculos.csv", sep=";", index=False)

    print('Extração Empresas Calculos realizada')

def extract_setores_e_ramos_de_atividades(spark):
    df_setores_e_ramos_de_atividades = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getPaths()[0].get("default")}/{utils.getPaths()[0].get("paths_origem_bases")}/Setores e Ramos de Atividade.csv')
    
    df_setores_e_ramos_de_atividades = df_setores_e_ramos_de_atividades.toPandas()
        
    df_setores_e_ramos_de_atividades.to_csv(f"{utils.getPaths()[1].get('extract')}/setores_e_ramos_de_atividades.csv", sep=";", index=False)

    print('Extração Setores e ramo de atividades realizada')
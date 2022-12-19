from typing import Sequence, Mapping, Any


def getCredenciais():
    CREDENCIAIS: Sequence[Mapping[str, Any]] = [
        {
            'url_db_azure' : 'jdbc:sqlserver://sql-projetos.database.windows.net:1433;database=sqldb-projetos',
            'user' : 'admin-projetos',
            'pass': 'pip20po0*'
        }
    ]
    
    return CREDENCIAIS

def getPaths():
    PATHS: Sequence[Mapping[str, Any]] = [
        {
            'default' : '/home/ec2-user/teste_bi_neoway',
            'paths_origem_bases': 'files/spreadsheets'
        }, 
        {
            'extract'   : 'loads/extract',
            'transform' : 'loads/transform',
            'load'      : 'loads/dataCloud'
        }
    ]
    
    return PATHS
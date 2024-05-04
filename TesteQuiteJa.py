import os
import csv
import sqlite3
import zipfile
from datetime import datetime

# //////////////////////////////////////////////// PASTAS ////////////////////////////////////////////////
# Caminho aonde meu arquivi esta para ser descompactado 
arquivo_zip = '/home/possamai/Downloads/dados.zip'

# Diretório de destino para onde vou mandar o arquivo descompactado
diretorio_destino = '/home/possamai/Downloads/teste'



# //////////////////////////////////////////////// FUNÇOES DESCOMPACTAR ////////////////////////////////////////////////
def descompactar_zip(arquivo_zip, diretorio_destino):
    with zipfile.ZipFile(arquivo_zip, 'r') as zip_ref:
        zip_ref.extractall(diretorio_destino)

descompactar_zip(arquivo_zip, diretorio_destino)

# Caminho para o arquivo origem-dados.csv
caminho_origem_dados = os.path.join(diretorio_destino, 'origem-dados.csv')
caminho_tipos        = os.path.join(diretorio_destino, 'tipos.csv')

# Lista para armazenar os dados do arquivo origem-dados.csv , tipos.csv , dadosCRITICOS e dadosNOMES
origem_dados = []
tipos        = []
dados_criticos = []
dados_nome_tipo = []

# Dicionário para mapear os tipos pelo ID
tipos_dict = {}

# ////////////////////////////// LEITURA E MANIPULÇÃO DE ARQUIVOS (origem-dados.csv e tipos.csv) ///////////////////////////////////
# Leitura do arquivo tipos.csv
with open(caminho_tipos, newline='') as arquivo_tipos:
    leitor_tipos = csv.reader(arquivo_tipos)
    next(leitor_tipos)  # Ignora a primeira linha (cabeçalho)
    for linha in leitor_tipos:
        tipo_id = int(linha[0])  # campo 1 é o ID do tipo
        tipos_dict[tipo_id] = linha[1]  # Mapeando tipos pelo ID        

for linha in tipos:
    print(linha)


# Leitura do arquivo origem-dados.csv
with open(caminho_origem_dados, newline='') as arquivo_origem:
    leitor_origem = csv.reader(arquivo_origem)
    leitor_csv    = csv.DictReader(arquivo_origem) 

    for linha in leitor_csv:
        origem_dados.append(linha)

        tipo_id = int(linha['tipo'])
        if tipo_id in tipos_dict:
            linha['nome_tipo'] = tipos_dict[tipo_id]  # Adicionando o nome do tipo
           

        if linha['status'].upper() == 'CRITICO': # Verifica o campo CRITICO
            dados_criticos.append(linha)
            # print(linha)

# Ordenar os dados filtrados pelo campo "created_at"
dados_criticos_ordenados = sorted(origem_dados, key=lambda x: datetime.strptime(x['created_at'], '%Y-%m-%d %H:%M:%S'))

# Imprimir os dados filtrados e ordenados
print("Dados Identificados como CRÍTICOS (Ordenados por 'created_at'):")
for dado in dados_criticos_ordenados:
    print(dado)


# /////////////////////////////////////// ARQUIVO SQL ////////////////////////////////////////////

# Caminho para o arquivo SQL de saída
caminho_sql = os.path.join(diretorio_destino, 'insert-dados.sql')

# Nome da tabela e nome das colunas
nome_tabela = 'dados_finais'
nomes_colunas = ['created_at', 'product_code', 'customer_code', 'status', 'tipo', 'nome_tipo']

# Abrir o arquivo SQL para escrita
with open(caminho_sql, 'w') as arquivo_sql:
    # Inserir o cabeçalho do insert
    arquivo_sql.write(f"INSERT INTO {nome_tabela} ({', '.join(nomes_colunas)}) VALUES\n")

    # Escrever os inserts para cada linha de dados críticos
    for dado in dados_criticos_ordenados:
        valores = [f"'{dado[coluna]}'" if isinstance(dado[coluna], str) else str(dado[coluna]) for coluna in nomes_colunas]
        linha_insert = f"({', '.join(valores)}),\n"
        arquivo_sql.write(linha_insert)

    # Remover a última vírgula e adicionar o ponto e vírgula final
    arquivo_sql.seek(arquivo_sql.tell() - 2, os.SEEK_SET)
    arquivo_sql.write(";\n")

print(f"Arquivo SQL criado em: {caminho_sql}")

# ////////////////////////////////////////////// BANCO ////////////////////////////////////////////////////
# - com base na estrutura desta tabela, monte uma query que retorne, por dia, a quantidade de itens agrupadas pelo tipo;

# Criar uma conexão com o banco 
conn = sqlite3.connect('dados.db')

# Criar uma tabela chamada dados_finais 
conn.execute('''
CREATE TABLE IF NOT EXISTS dados_finais (
    id INTEGER PRIMARY KEY,
    created_at TEXT,
    product_code TEXT,
    customer_code TEXT,
    status TEXT,
    tipo INTEGER,
    nome_tipo TEXT
)
''')

# Inserir os dados críticos no banco de dados
for dado in dados_criticos_ordenados:
    conn.execute('''
    INSERT INTO dados_finais (created_at, product_code, customer_code, status, tipo, nome_tipo)
    VALUES (?, ?, ?, ?, ?, ?)
    ''', (dado['created_at'], dado['product_code'], dado['customer_code'], dado['status'], dado['tipo'], dado['nome_tipo']))

# Commit das alterações no banco de dados
conn.commit()

# Montar a query SQL para obter a quantidade de itens agrupados por dia e tipo
query = '''
SELECT DATE(created_at) AS data, tipo, nome_tipo, COUNT(*) AS quantidade
FROM dados_finais
GROUP BY DATE(created_at), tipo, nome_tipo
ORDER BY data, tipo, nome_tipo
'''

# Executar a query 
resultado = conn.execute(query)

# Imprimir o resultado
print("Quantidade de itens agrupados por dia e tipo:")
for row in resultado:
    print(row)

# Fechar a conexão com o banco de dados
conn.close()


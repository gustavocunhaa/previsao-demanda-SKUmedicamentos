import pandas as pd
import os

# O objetivo desse script é realizar a limpeza dos dados coletados, deixando uma base única final pronta para a previsão das séries temporais
# São definidas funções para abrir os arquivos, transformar e salvar os novos arquivos criados
# Os métodos que define o pipeline são funções compilando as diferentes funções criadas, formando o fluxo de tratamento dos dados
# A pasta trusted tem os dados já filtrados, tanto por munipio quanto por colunas de interesse (Fonte: https://dados.gov.br/dados/conjuntos-dados/venda-de-medicamentos-controlados-e-antimicrobianos---medicamentos-industrializados)
# A pasta refined tem o dataframe final unido, pronto para a análise

# Define os diretorios das pastas de dados
rootPath = os.getcwd()
dataPath = os.path.join(rootPath, 'data')
dataRawPath = os.path.join(dataPath, 'raw')
dataTrustedPath = os.path.join(dataPath, 'trusted')
dataRefinedPath = os.path.join(dataPath, 'refined')

# ----------- FUNÇÕES QUE CARREGAM E SALVAM OS DADOS -----------
# Abre o CSV podendo selecionar o separador com o enconding
def open_csv(folder, csv, sep=',', enconding='utf-8'):
    csv_path = os.path.join(folder, csv)
    data = pd.read_csv(csv_path, sep=sep, encoding=enconding, low_memory=False)
    return data

# Salva um arquivo csv na pasta desejada
def save_csv(dataframe, folder, csv, sep):
    csv_path = os.path.join(folder, csv)
    dataframe.to_csv(csv_path, sep=sep, index=False)
    return print(f"Arquivo {csv} salvo na pasta -> {folder}")

# ----------- FUNÇÕES QUE TRANSFORMAM OS DADOS -----------
# Seleciona a cidade de escolha
def query_mun(dataframe, municipio):
    dataframe_query = dataframe.query(f"MUNICIPIO_VENDA == '{municipio}'")
    return dataframe_query

# Seleciona as colunas que fazem sentido para o projeto
def query_columns(dataframe, columns):
    dataframe_query = dataframe[columns]
    return dataframe_query

# Cria os indexes de tempo e o campo chave para id do medicamento
# O campo id do medicamento ficou definido como o principio ativo + apresentacao, para termos de fato os diferentes produtos distintos com as infos fornecidas
def create_index_ts(dataframe):
    dataframe['DATA_EOM_VENDA'] = pd.to_datetime(dataframe['MES_VENDA'].map(str)  + '/' + dataframe['ANO_VENDA'].map(str))
    dataframe['KEY_SKU_MEDICAMENTO'] = dataframe['PRINCIPIO_ATIVO'].map(str)  + ' ' + dataframe['DESCRICAO_APRESENTACAO'].map(str)
    time_series = dataframe.groupby(['KEY_SKU_MEDICAMENTO'])['DATA_EOM_VENDA'].max()
    df = pd.DataFrame(dataframe.groupby(['KEY_SKU_MEDICAMENTO'])['QTD_VENDIDA'].sum())
    df['DATA'] = time_series
    df = df.reset_index()
    return df

# ----------- FUNÇÕES QUE DEFINEM O PIPELINE -----------

def pipeline_raw_trusted(municipio_escolha, colunas_interesse):
    print("\n")
    print("Iniciado o pipline de dados: raw - trusted")
    print("\n")
    list_files_raw = os.listdir(dataRawPath)

    for csv_file in list_files_raw:
        df_raw = open_csv(dataRawPath, csv_file, sep=';', enconding='latin-1')
        df_trusted = query_mun(df_raw, municipio_escolha)
        df_trusted = query_columns(df_trusted, colunas_interesse)
        csv_clean_name = 'CLEAN-' + csv_file
        save_csv(df_trusted, dataTrustedPath, csv_clean_name, sep=';')
    
    return print("Fim do pipeline de dados: raw - trusted")

def pipeline_trusted_refined():
    print("\n")
    print("Iniciando o pipeline de dados: trusted - refined")
    print("\n")
    list_files_trusted = os.listdir(dataTrustedPath)

    df_refined = pd.DataFrame() # Dataframe vazio que ira receber os valores de todos os arquivos limpos
    for csv_file in list_files_trusted:
        df_trusted = open_csv(dataTrustedPath, csv_file , sep=';')
        df_process = create_index_ts(df_trusted)
        df_refined = pd.concat([df_refined, df_process])
        print(f"Arquivo {csv_file} unido no dataframe final")
   
    csv_refined_name = 'DF_REFINED_EDA_INDUSTRIALIZADOS.csv'
    save_csv(df_refined, dataRefinedPath, csv_refined_name, sep=';')

    return print("Fim do pipeline de dados: trusted - refined")

# ----------- EXECUÇÃO DO PIPELINE -----------

municipio_escolha = 'BELO HORIZONTE'
colunas_interesse = ['ANO_VENDA', 'MES_VENDA', 'PRINCIPIO_ATIVO', 'DESCRICAO_APRESENTACAO', 'QTD_VENDIDA']

pipeline_raw_trusted(municipio_escolha, colunas_interesse)
pipeline_trusted_refined()
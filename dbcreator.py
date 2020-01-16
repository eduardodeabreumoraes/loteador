#Importa as Bibliotecas que serão utilizadas
import pandas as pd
import os
import numpy as np

#Define o prefixo que identificará o ativo.
prefixo = input('Qual o prefixo (DOL, DI1, etc) do ativo de interesse: ')

#Pergunta em qual pasta estão os arquivos zip que serão utilizados para criar a base de dados.
pasta = input('Nome da pasta onde estão os arquivos ZIP: ')

#Define o número de buckets por dia.
N = int(input('Número de buckets por dia: '))

#Cria uma lista com os nomes das colunas dos arquivos (Coluna1, Coluna2, etc. conforme o arquivo passo a passo enviado pelo João Eduardo)
colunas = ['Coluna' + str(n) for n in range(1,19)]

#Cria uma lista com as colunas que são do interesse do usuário.
colunas_de_interesse = ['Coluna1', 'Coluna2', 'Coluna4', 'Coluna5', 'Coluna6', 'Coluna11']

#Cria uma lista com os arquivos na pasta. É importante que na pasta estejam somente os arquivos ZIP que serão utilizados.
files = os.listdir(pasta)

#Ordena alfabeticamente a lista de arquivos, para que fiquem na ordem cronológica.
files.sort()

#Define o nome do Banco de Dados
nome_database = (prefixo + '_' + str(N) + '_database.csv')

#Cria o arquivo onde serão escritos os resultados (Banco de Dados)
fd = open(nome_database,'a')
fd.write('Data')
fd.write(',')
fd.write('Bucket_Número')
fd.write(',')
fd.write('VPIN')
fd.write(',')
#fd.write('Última Oferta Venda')
#fd.write(',')
#fd.write('Primeira Oferta Compra')
#fd.write(',')
fd.write('Bid Ask Spread')
fd.write(',')
fd.write('Quantidade negociada')
fd.write(',')
fd.write('Volume Negociado (em dinheiro)')
fd.write(',')
fd.write('Limite máximo de volume no bucket.')
fd.write(',')
fd.write('Variância do Preço')
fd.write('\n')
fd.close()

for file in files: #Passa por cada um dos arquivos ZIP da pasta.
    df = pd.read_csv(pasta + '/' + file, sep=';', names=colunas, skiprows=1) #Entra em cada ZIP, lê o arquivo (seperado por ';'), ignora a primeira linha (dentificação do arquivo), e transforma em um pandas dataframe.
    data = df.iloc[0,0] #Salva a data daquele dia (arquivo).
    df = df[:-1] #Deleta a última linha do dataframe (presenta o total de linhas do arquivo).
    df = df[colunas_de_interesse] #Deleta as colunas que não são de interesse do usuário.
    df.Coluna2 = df.Coluna2.str.rstrip() #Deleta os espaços em branco à direita do ticker na Coluna2.
    df = df[(df.Coluna2.str.contains(prefixo)) & (df.Coluna2.str.len() == 6)] #Esse comando deleta todas as linhas que não são do ativo de interesse (determinado em função do prefixo) e cujo ticker possua mais ou menos de 6 caracteres.
    df.insert(column = 'Volume_Negociado', value = df.Coluna4 * df.Coluna5, loc=(len(df.columns))) #Cria uma nova coluna com o volume negociado em cada operação.
    df.Coluna11 = df.Coluna11.replace(1, -1) #Se é compra vai ser -1 ao invés de 1.
    df.Coluna11 = df.Coluna11.replace(2, 1) #Altera 2 por 1 na coluna que indica se a operação foi compra ou venda. Dessa forma, -1 significa compra e +1 significa venda. 
    volume_negociado_dia = df.Volume_Negociado.sum() #Calcula o volume negociado no dia inteiro (arquivo inteiro).
    volume_bucket_dia = volume_negociado_dia / N #Calcula o volume de cada bucket do dia.
    agrupador = [] #Declara a lista que trará os critérios para agrupar os buckets no groupby adiante.
    grupo_numero = 1 #Declara a variável que vai estabelecer o número do grupo (bucket) começando por 1.
    soma_atual = 0 #Declara a variável que vai ser utilizada para conferir o volume em cada bucket.
    for i in df.Volume_Negociado.values:
        soma_atual += i
        if grupo_numero < N: #Confere se não está no último bucket (único que pode estourar o limite)
            if soma_atual > volume_bucket_dia: #Se ultrapassar o valor do limite máximo do bucket.
                soma_atual = i #Já inclui o valor do FOR atual no próximo bucket.
                grupo_numero += 1 #Passa para o próximo bucket
                agrupador.append(grupo_numero)
            else: #Se não ultrapassar o valor do limite máximo do bucket.
                agrupador.append(grupo_numero)
        else:
            agrupador.append(grupo_numero)
    buckets = df.groupby(agrupador) #Cria os buckets, cada um com até o valor do volume de cada bucket (1/N avos do volume do dia), exceto o último que ultrapassará o limite (será o bucket de ajuste).   
    for tuple in buckets:#Passa por cada uma das tuples formadas na linha anterior.
        bucket_number, df_bucket = tuple #Separa o que é o número do bucket e o que é o dataframe do bucket.
        vpin = (np.abs((df_bucket.Coluna11 * df_bucket.Volume_Negociado).sum())) / (N * (df_bucket.Volume_Negociado.sum())) #Calcula o VPIN.
        lista_de_precos = (df_bucket.Coluna11 * df_bucket.Coluna4).to_list()
        ultima_oferta_venda = np.nan
        for i in range(len(lista_de_precos)-1, -1, -1):
            if lista_de_precos[i] > 0:
                ultima_oferta_venda = lista_de_precos[i] #Obtém o último valor positivo (oferta de venda).
                break
        primeira_oferta_compra = np.nan 
        for i in lista_de_precos:
            if i < 0:
                primeira_oferta_compra = i #Obtém o primeiro valor negativo (oferta de compra).
                break
        bid_ask_spread = ultima_oferta_venda + primeira_oferta_compra
        quantidade_negociada = df_bucket.Coluna5.sum() #Soma as quantidades negociadas em cada linha do bucket.
        volume_negociado = df_bucket.Volume_Negociado.sum()
        variancia_preco = df_bucket.Coluna4.var() #Calcula a variância dos preços, sem diferenciar se são preços de compra e de venda.
        #Escreve os resultados no banco de dados.
        fd = open(nome_database,'a')
        fd.write(data)
        fd.write(',')
        fd.write(str(bucket_number))
        fd.write(',')
        fd.write(str(vpin))
        fd.write(',')
        #fd.write(str(ultima_oferta_venda))
        #fd.write(',')
        #fd.write(str(primeira_oferta_compra))
        #fd.write(',')
        fd.write(str(bid_ask_spread))
        fd.write(',')
        fd.write(str(quantidade_negociada))
        fd.write(',')
        fd.write(str(volume_negociado))
        fd.write(',')
        fd.write(str(volume_bucket_dia))
        fd.write(',')
        fd.write(str(variancia_preco))
        fd.write('\n')
        fd.close()
    print(data)
    

from bs4 import BeautifulSoup
from datetime import date, datetime, timedelta
from pandas.tseries.offsets import MonthEnd
from zipfile import ZipFile
import json
import numpy as np
import os
import pandas as pd
import re
import requests as req
import sqlite3

from sqlalchemy import values

dbname = 'CVM_Dados_Financeiros.db'

conn = sqlite3.connect(dbname)

data_hoje = datetime.today().strftime('%Y-%m-%d')

def download(url: str, dest_folder: str):
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)  # create folder if it does not exist

    filename = url.split('/')[-1].replace(" ", "_")  # be careful with file names
    file_path = os.path.join(dest_folder, filename)

    r = req.get(url, stream=True)
    if r.ok:
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 8):
                if chunk:
                    f.write(chunk)
                    f.flush()
                    os.fsync(f.fileno())
    else:  # HTTP status code 4XX/5XX
        print("Download failed: status code {}\n{}".format(r.status_code, r.text))


def download_arquivos_CVM(tipo):

    # Acessa site CVM para verificar arquivos anuais para download


    URL_CVM = f'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/{tipo}/DADOS/'

    data_ultimo_download = '2020-05-10'


    try:

        resp = req.get(URL_CVM)

    except Exception as e:

        print(e)
        


    bs = BeautifulSoup(resp.text, 'html.parser')

    tab = bs.find_all('table', {'id': 'indexlist'})

    # Lista armazena todos os arquivos listados no site
    lista = []
    for i in tab:
        class_indexcolname = i.findChildren('td', 'indexcolname')
        class_indexcollastmod = i.findChildren('td', 'indexcollastmod')
        for j, z in enumerate(class_indexcolname):
            if class_indexcolname[j].text.startswith(f'{tipo.lower()}_cia_aberta'):
                lista.append([class_indexcolname[j].text.strip(), class_indexcollastmod[j].text.strip()])

    # df contém a lista dos arquivos a serem baixados

    df = pd.DataFrame(lista, columns=['nome', 'last_mod'])
    df['ano'] = df.nome.str[15:19]
    df['last_mod'] = df['last_mod'].str[:10]

    df = df[df.last_mod > data_ultimo_download]


    for arq, ano in zip(df.nome, df.ano):

        print('Download do arquivo:', arq, ano)
        
        download(URL_CVM + arq, dest_folder=rf'Base_CVM\{tipo}')


base_download_cvm = ['DFP', 'ITR', 'FRE', 'FCA']

for tipo in base_download_cvm:
    print(tipo)
    download_arquivos_CVM(tipo)




def processa_base_cvm(tipo, arquivo):

    df = pd.DataFrame()

    pasta = f'Base_CVM\\{tipo}\\'

    for filezip in os.listdir(pasta):

        ano = re.search('[0-9]+', filezip).group(0)

        with ZipFile(pasta + filezip) as zip:

            nome_arq = f'{tipo.lower()}_cia_aberta_{arquivo}_{ano}.csv'
            with zip.open(nome_arq) as f:
                temp = pd.read_csv(f, encoding='Latin-1', delimiter=';')
                df = pd.concat([df, temp])

    return df

balanco_ativo = processa_base_cvm('DFP', 'BPA_con')
balanco_passivo = processa_base_cvm('DFP', 'BPP_con')
dre = processa_base_cvm('DFP', 'DRE_con')
dra = processa_base_cvm('DFP', 'DRA_con')
dfc_md = processa_base_cvm('DFP', 'DFC_MD_con')
dfc_mi = processa_base_cvm('DFP', 'DFC_MI_con')

dfp = pd.concat([balanco_ativo, balanco_passivo, dre, dra, dfc_md, dfc_mi])

dfp = dfp[dfp.ORDEM_EXERC == 'ÚLTIMO']       
dfp.VL_CONTA = dfp.VL_CONTA.astype(float)

#dfp.VL_CONTA = dfp.VL_CONTA / 100


fre_classe_acao = processa_base_cvm('FRE', 'capital_social_classe_acao')

cadastro = processa_base_cvm('FCA', 'geral')

cadastro_tickers = processa_base_cvm('FCA', 'valor_mobiliario')


dfp['ano'] = dfp['DT_REFER'].str[:4]
dfp['tipo'] = 'DFP'

#dfp['VL_CONTA'] = dfp['VL_CONTA'].astype(float)

dfp.columns = ['cnpj', 'dt_ref', 'versao', 'nome', 'cod_cvm', 'grupo_dfp', 'moeda', 'escala_moeda',
               'ordem_exerc', 'dt_fim_exerc', 'cod_conta', 'desc_conta', 'valor', 'sit_conta_fixa',
               'dt_ini_exerc', 'ano', 'form']

# Seleciona saldos de interesse

contas_selec = ['1', '1.01.01', '1.01.02', '2.03', '3.01', '3.03',
                '3.05', '3.11', '2.01.04', '2.02.01']

# idx saldos
idx_saldos = dfp.cod_conta.isin(contas_selec)

# idx deprec
idx_deprec = (dfp.cod_conta.str.startswith('6.01')
             ) & (
              dfp.desc_conta.str.contains('deprec|amortiz', regex=True))


saldos = dfp[idx_saldos]

deprec = dfp[idx_deprec]
deprec = deprec.groupby(['form', 'cod_cvm', 'ano', 'dt_ref']).sum('valor').reset_index()
deprec['deprec_amortiz'] = deprec['valor']
del deprec['valor']

df = saldos.pivot_table(values='valor', index=['form', 'cod_cvm', 'ano', 'dt_ref'], columns='cod_conta', fill_value=0).reset_index()

df = df.merge(deprec, on=['form', 'cod_cvm', 'ano', 'dt_ref'], how='left')
df = df.fillna(0)

df.ano = df.ano.astype(int)

df['ativo'] = df['1']
df['patr_liq'] = df['2.03']
df['receita'] = df['3.01']
df['lucro_bruto'] = df['3.03']
df['lucro_liq'] = df['3.11']
df['EBIT'] = df['3.05']
df['divida_curto_prazo'] = df['2.01.04']
df['divida_longo_prazo'] = df['2.02.01']
df['caixa'] = df['1.01.01'] + df['1.01.02']
df['divida_total'] = df['divida_curto_prazo'] + df['divida_longo_prazo']
df['EBIT_taxa'] = round(df['EBIT'] / df['receita'], 2)

df['endivid_taxa'] = round(df['divida_total'] / df['ativo'], 2)
df['net_rate'] = round(df['lucro_liq'] / df['receita'] * 100, 2)
df['EBITDA'] = round(df['EBIT'] + df['deprec_amortiz'], 2)
df['divida_liq'] = round((df['divida_total'] - df['caixa']) / df['EBITDA'], 2)
df['ROA'] = round(df['EBITDA'] / df['ativo'], 2)
#df['size'] = np.log(df['ativo'])
df['ROE'] = round(df['lucro_liq'] / df['patr_liq'], 2)
df['GAO'] = round(df['EBIT'] / df['lucro_bruto'], 2)

df = df[['form', 'cod_cvm', 'ano', 'dt_ref',
       'ativo', 'patr_liq', 'receita', 'lucro_bruto', 'lucro_liq', 'EBIT',
       'divida_curto_prazo', 'divida_longo_prazo', 'caixa', 'divida_total',
       'EBIT_taxa', 'endivid_taxa', 'net_rate', 'EBITDA', 'divida_liq', 'ROA',
       'ROE', 'GAO']]

df_ultimo_ano = df.groupby(['form', 'cod_cvm']).max('ano').reset_index()[['form', 'cod_cvm', 'ano']]
ano_anterior = df.ano.max() - 1

empresas_ano_anterior = df_ultimo_ano[df_ultimo_ano.ano >= ano_anterior]['cod_cvm']

df = df[df.cod_cvm.isin(empresas_ano_anterior)]

df[df.cod_cvm == 5410]








df = df.sort_values(['form', 'cod_cvm', 'ano'])

saldos['ativo'] = [saldos.valor if saldos.cod_conta == '1']

balanco_ativo = processa_base_cvm('ITR', 'BPA_con')
balanco_passivo = processa_base_cvm('ITR', 'BPP_con')
dre = processa_base_cvm('ITR', 'DRE_con')
dra = processa_base_cvm('ITR', 'DRA_con')

'''
summarise(total.assets = get.acc(acc.value, acc.number, '1'),
                capital = get.acc(acc.value, acc.number, '2.03'),
                total.sales = get.acc(acc.value, acc.number, '3.01'),
                gross.profit = get.acc(acc.value, acc.number, '3.03'),
                net.profit = get.acc(acc.value, acc.number, '3.11'),
                EBIT = get.acc(acc.value, acc.number, '3.05'),
                short.term.debt = get.acc(acc.value, acc.number, '2.01.04'),
                long.term.debt = get.acc(acc.value, acc.number, '2.02.01'),
                cash = get.acc(acc.value, acc.number, '1.01.01') + get.acc(acc.value, acc.number, '1.01.02')) %>%

mutate(total.debt = short.term.debt + long.term.debt,
           EBIT.rate = round(EBIT / total.sales, 2),
           leverage = round(total.debt / total.assets, 2),
           net.rate = round(net.profit / total.sales * 100, 2),
           EBITDA = EBIT + deprec.amort,
           net.debt = round((total.debt - cash) / EBITDA, 2),
           ROA = round(EBITDA / total.assets, 2),
           size = log(total.assets),
           ROE = round(net.profit / capital, 2),
           GAO = round(EBIT / gross.profit, 2),
           ref.year = year(ref.date))

  model.table <- model.table %>%
    select(segment, name.company, id, pregao, site, tickers, ref.year,
        total.assets, capital, total.sales, gross.profit, net.profit,
        EBIT, deprec.amort, EBITDA, net.rate, short.term.debt, long.term.debt,
        cash, net.debt, total.debt, size, leverage, EBIT.rate, ROA, ROE, GAO, size,
        leverage, main.sector, sector.activity, sub.sector,
        listing.segment)

financ.tabular <- model.table %>%
    select(segmento, nome, ticker, listagem, ibovespa, id, ano, rec_liq, lucro_liq, acoes) %>%
    group_by(segmento, nome, ticker, listagem, ibovespa, id) %>%
    summarise(acoes = acoes,
              ano_ini = min(ano),
              ano_fim = max(ano),
              periodos = ano_fim - ano_ini + 1, 
              prim_rec_liq = as.integer(first(rec_liq)),
              ult_rec_liq = as.integer(last(rec_liq)),
              margem_liq = round(sum(lucro_liq) / sum(rec_liq), 2),
              margem_liq_aa = round(margem_liq / periodos, 2),
              var_rec_liq = round(ult_rec_liq / prim_rec_liq - 1, 2),
              var_rec_liq_aa = round((var_rec_liq + 1) ^ (1 / (periodos - 1)) - 1, 2))
  
  names(model.table)[which(names(model.table) == 'name.company')] <- 'nome'
  names(model.table)[which(names(model.table) == 'segment')] <- 'segmento'
  names(model.table)[which(names(model.table) == 'listing.segment')] <- 'listagem'
  names(model.table)[which(names(model.table) == 'tickers')] <- 'ticker'
  names(model.table)[which(names(model.table) == 'ref.year')] <- 'ano'
  names(model.table)[which(names(model.table) == 'total.assets')] <- 'ativo'
  names(model.table)[which(names(model.table) == 'capital')] <- 'pl'
  names(model.table)[which(names(model.table) == 'total.sales')] <- 'rec_liq'
  names(model.table)[which(names(model.table) == 'gross.profit')] <- 'lucro_bruto'
  names(model.table)[which(names(model.table) == 'net.profit')] <- 'lucro_liq'
  names(model.table)[which(names(model.table) == 'deprec.amort')] <- 'deprec_amort'
  names(model.table)[which(names(model.table) == 'short.term.debt')] <- 'div_curto_prazo'
  names(model.table)[which(names(model.table) == 'long.term.debt')] <- 'div_longo_prazo'
  names(model.table)[which(names(model.table) == 'total.debt')] <- 'div_total'
  names(model.table)[which(names(model.table) == 'net.rate')] <- 'margem_liq'
  names(model.table)[which(names(model.table) == 'cash')] <- 'caixa'
  names(model.table)[which(names(model.table) == 'net.debt')] <- 'div_liq'
  names(model.table)[which(names(model.table) == 'number.of.stocks')] <- 'acoes'

'''

itr = pd.concat([balanco_ativo, balanco_passivo, dre, dra])
itr['ano'] = itr['DT_REFER'].str[:4]
itr['tipo'] = 'ITR'

itr['VL_CONTA'] = itr['VL_CONTA'].astype(float) / 100






for cik in submissions.cik:
    zipname = f"CIK{'{:0>10}'.format(cik)}.json"
    print(zipname)
    with ZipFile(f_companyfacts) as zip:
        try:
            zipfile = zip.read(zipname)
            companyfacts = pd.concat([companyfacts, processaCompanyFacts(zipfile)])
        except:
            pass




        # Download do arquivo da CVM        
        
        try:

            cvm = pd.read_csv(URL_CVM + arq, sep=';',
                                            usecols=['CNPJ_FUNDO', 'DT_COMPTC', 'VL_QUOTA'],
                                            dtype={'VL_QUOTA': 'float64'},
                                            parse_dates=['DT_COMPTC'],
                                            dayfirst=False)

            cvm.columns = ['fundo_cnpj', 'fundo_data', 'fundo_vlr_quota']

    cursor = conn.cursor()

    def downloadCVM(url, cursor):
        '''
        Acessa Histórico dos Fundos CVM do portal Dados Abertos para obter valor histórico
        Salva base em cache e atualiza data do último download
        Se já foi feito download no dia, obtém base do cache
        Só serão armazenadas informações dos CNPJ com investimento
        '''
        erro_download = []

        # Acessa primeira data de início das operações

        sql = '''
                SELECT min(oper_data) as data
                FROM OperacaoFundos
            '''

        df = pd.read_sql(sql, conn)
        data_ini = df.data.iloc[0][:10]

        # Verifica último download da base CVM
        sql = '''
                SELECT *
                FROM Config
                WHERE config_tag = 'dt_download_CVM'
            '''
        cursor = conn.cursor()
        query_results = cursor.execute(sql).fetchall()

        config = pd.DataFrame(query_results)
        config.columns = ['config_tag', 'config_valor', 'config_desc']

        dt_download_CVM = config.config_valor.iloc[0]

        if datetime.now().strftime('%Y-%m-%d') == dt_download_CVM:

            # Acessa Base de dados salva
            cvm = pd.read_sql('SELECT * FROM FundosCVM', conn)

            return cvm, data_ini, erro_download


        # Acessa lista CNPJ dos investimentos

        sql = '''
                SELECT inv_cnpj
                FROM Investimento
                WHERE inv_cnpj <> ''
            '''

        lista_cnpj = pd.read_sql(sql, conn)


        # Verifica data do último download para cada arquivo mensal

        sql = '''
                SELECT *
                FROM ControleDownloadCVM
            '''
        dwld_cvm = pd.read_sql(sql, conn)
        dwld_cvm.cvm_dwld_dt_download = pd.to_datetime(dwld_cvm.cvm_dwld_dt_download)

                # Retira CNPJ sem investimento

                cvm = cvm[cvm.fundo_cnpj.isin(lista_cnpj.inv_cnpj)]

                cvm['ano_mes'] = ano_mes

                # Remove registros anteriores do respectivo ano/mês
                sql = '''
                        DELETE
                        FROM FundosCVM
                        WHERE ano_mes = ? 
                    '''
                cursor.execute(sql, (ano_mes,))
                conn.commit()

                cvm.to_sql('FundosCVM', conn, if_exists='append', index=False)
                
            except Exception as e:

                print(e)
                
                erro_download.append(arq)

        # Elimina do df os arquivos com erro de download
        df = df[~df.nome.isin(erro_download)]

        # Atualiza tabela ControleDownloadCVM com novas datas de download

        df = df[['ano_mes', 'cvm_dwld_dt_download']]
        df.columns = ['cvm_dwld_ano_mes', 'cvm_dwld_dt_download']
        
        dwld_cvm = pd.concat([dwld_cvm, df])
        dwld_cvm.cvm_dwld_dt_download = pd.to_datetime(dwld_cvm.cvm_dwld_dt_download)
        dwld_cvm = dwld_cvm.sort_values(['cvm_dwld_ano_mes', 'cvm_dwld_dt_download'])
        dwld_cvm = dwld_cvm.drop_duplicates('cvm_dwld_ano_mes', keep='last')
        
        dwld_cvm.to_sql('ControleDownloadCVM', conn, if_exists='replace', index=False)

        cvm = pd.read_sql('SELECT * FROM FundosCVM', conn)

        # Atualiza Config com data do download
        # Não atualiza caso tenha dado erro em algum download

        if erro_download == []:

            cursor.execute('''
                UPDATE Config
                SET config_valor = ?
                WHERE config_tag = ?
            ''', (data_hoje, 'dt_download_CVM'))
            
            conn.commit()
        
        return cvm, data_ini, erro_download


    # Acessa histórico da base CVM

    cvm, data_ini, erro_download = downloadCVM(url=URL_CVM, cursor=cursor)

    cvm = cvm.sort_values(['fundo_cnpj', 'fundo_data'])

    for arq in erro_download:
        print('Erro no download do arquivo', arq)    

    cvm.fundo_data = pd.to_datetime(cvm.fundo_data)

    # Datas para Histórico CVM

    datas_cvm = pd.DataFrame(pd.date_range(start=data_ini, end=data_hoje).tolist())
    datas_cvm.columns = ['data']
    datas_cvm.data = pd.to_datetime(datas_cvm.data)

    # Obtém Operações Fundos agrupadas por corretora, fundo e dia

    sql = '''
            SELECT oper.*, inv.inv_cnpj
            FROM OperacaoFundos AS oper
            LEFT JOIN Investimento AS inv
            ON oper.oper_nome = inv.inv_nome    '''
    cursor = conn.cursor()
    query_results = cursor.execute(sql).fetchall()

    oper_cvm = pd.DataFrame(query_results)
    oper_cvm.columns = ['data', 'oper', 'corret', 'nome', 'qtd', 'preco_unit', 'valor', 'cnpj']
    oper_cvm.data = pd.to_datetime(oper_cvm.data)

    # Fundos com operação

    fundos = oper_cvm[['nome', 'cnpj']].drop_duplicates()


    # Seleciona histórico somente dos fundos com operação

    cvm = cvm[(cvm.fundo_cnpj.isin(fundos.cnpj))]

    historico_cvm = fundos.merge(datas_cvm, on=None, how='cross')

    historico_cvm = historico_cvm.merge(cvm,
                                    left_on=['cnpj', 'data'],
                                    right_on=['fundo_cnpj', 'fundo_data'],
                                    how='left').fillna(axis=0, method='ffill').dropna()

    historico_cvm = historico_cvm[['nome', 'cnpj', 'data', 'fundo_vlr_quota']]

    # Calcula o valor da posição diária por ativo e corretora

    cvm_corretora = oper_cvm[['corret', 'cnpj']].drop_duplicates()

    posicao_diaria = cvm_corretora.merge(historico_cvm, on=['cnpj'], how='left')

    posicao_diaria = posicao_diaria.merge(oper_cvm, on=['corret', 'cnpj', 'nome', 'data'], how='left')
    posicao_diaria = posicao_diaria[['corret', 'cnpj', 'nome', 'data', 'fundo_vlr_quota', 'qtd']]
    posicao_diaria.qtd = posicao_diaria.qtd.fillna(0)

    posicao_diaria['qtd_acum'] = posicao_diaria.groupby(['corret', 'cnpj'])['qtd'].cumsum(axis=0)
    posicao_diaria = posicao_diaria.dropna()
    posicao_diaria = posicao_diaria[(posicao_diaria.qtd_acum > 0) | (posicao_diaria.qtd != 0)]
    posicao_diaria['saldo'] = round(posicao_diaria.qtd_acum * posicao_diaria.fundo_vlr_quota, 2)

    # Classifica e reordena colunas

    posicao_diaria = posicao_diaria.sort_values(['data', 'corret', 'nome'])[
        ['data', 'corret', 'nome', 'cnpj', 'qtd', 'qtd_acum', 'fundo_vlr_quota', 'saldo']]

    # Salva posição diária no banco de dados

    posicao_diaria.to_sql('PosicaoDiariaFundos', con=conn, if_exists='replace', index=False)

    if print_resumo:

        # Totais por dia
        total_dia = posicao_diaria.groupby(['corret', 'nome', 'tipo', 'cnpj', 'data']).sum(['valor']).reset_index()[['corret', 'nome', 'tipo', 'cnpj', 'data', 'qtd_acum', 'saldo']]
        total_dia = total_dia[total_dia.data == data_hoje]

        # Posição Diária por fundo
        print(total_dia.groupby(['data', 'nome', 'tipo']).sum())

        # Posição Diária por tipo
        print(total_dia.groupby(['data', 'tipo']).sum())

        # Posição Diária por corretora/fundo
        print(total_dia.groupby(['data', 'corret', 'nome', 'tipo']).sum())

        # Posição Diária por corretora/tipo
        print(total_dia.groupby(['data', 'corret', 'tipo']).sum())

        # Posição Diária por corretora
        print(total_dia.groupby(['data', 'corret']).sum())

        # Posição Total
        print('TOTAL:', total_dia.groupby(['data']).sum())


    return posicao_diaria


def proventos_por_dia(conn):
    '''
    Relaciona proventos/dividendos por dia
    '''
    sql = '''
        SELECT prov_data, prov_corret, prov_tipo, prov_ticker, prov_valor, prov_moeda, prov_forma, dolar
        FROM ProventosAcao
        LEFT JOIN HistoricoDolar
        ON prov_data = data
        ORDER BY prov_data DESC, prov_corret, prov_tipo, prov_ticker
        '''
    return pd.read_sql(sql, conn)


def operacoes_acao_por_dia(conn):
    '''
    Relaciona operações por dia
    '''
    sql = '''
        SELECT *
        FROM OperacaoAcao
        ORDER BY oper_data DESC, oper_corret, oper_tipo, oper_ticker
        '''
    return pd.read_sql(sql, conn)


def operacoes_td_por_dia(conn):
    '''
    Relaciona operações por dia
    '''
    sql = '''
        SELECT *
        FROM OperacaoTD
        ORDER BY oper_data DESC, oper_corret, oper_nome
        '''
    return pd.read_sql(sql, conn)


def operacoes_td_por_dia(conn):
    '''
    Relaciona operações por dia
    '''
    sql = '''
        SELECT *
        FROM OperacaoTD
        ORDER BY oper_data DESC, oper_corret, oper_nome
        '''
    return pd.read_sql(sql, conn)


def operacoes_fundos_por_dia(conn):
    '''
    Relaciona operações por dia
    '''
    sql = '''
        SELECT *
        FROM OperacaoFundos
        ORDER BY oper_data DESC, oper_corret, oper_nome
        '''
    return pd.read_sql(sql, conn)


def operacoes_rf_por_dia(conn):
    '''
    Relaciona operações por dia
    '''
    sql = '''
        SELECT *
        FROM OperacaoRF
        ORDER BY rf_data DESC, rf_corret, rf_nome
        '''
    return pd.read_sql(sql, conn)


def apuracao_resultado_acao_dia(conn, data_base, acessa_YFinance=False):
    '''
    Apura resultado do dia com as ações
    '''

    cursor = conn.cursor()
    
    data_hoje = date.today().strftime('%Y-%m-%d')

    '''
    if data_base == '':
        data_fim_yahoo = datetime.utcnow().strftime('%Y-%m-%d')
    else:
        data_fim_yahoo = data_base
    '''

    # Obtém relação de tickers para buscar histórico
    sql = f'''
        SELECT oper_tipo, oper_ticker, oper_moeda, sum(oper_qtd) as acum
        FROM OperacaoAcao
        WHERE strftime('%Y-%m-%d', oper_data) <= '{data_base}'
        GROUP BY oper_tipo, oper_ticker, oper_moeda
        HAVING acum > 0
        '''
    sql = sql.format(data_base)
    query_results = cursor.execute(sql).fetchall()

    carteira = pd.DataFrame(query_results)
    carteira.columns = ['tipo', 'ticker', 'moeda', 'acum']

    carteira['ticker_yahoo'] = carteira.ticker
    carteira.loc[carteira.moeda == 'BRL', 'ticker_yahoo'] = carteira.ticker + '.SA'

    lista_tickers_yahoo = carteira.ticker_yahoo.tolist()

    # Busca cotações (dados Yahoo Finance)

    df = get_cotacao_data_unica_YahooFinance(conn, lista_tickers_yahoo, data_base, acessa_YFinance)

    dt_maxima = max(df.data)

    # Yahoo Finance trabalha com UTC
    if dt_maxima > data_hoje:
        dt_maxima = data_hoje

    # Obtém dolar e Bovespa na data base
    df_dolar = get_dolar_data(conn, data_base)
    df_bovespa = get_bovespa_data(conn, data_base)

    # Determina Dolar

    dolar = round(df_dolar.iloc[0]['cot_hoje'], 4)
    var_dolar = round((df_dolar.iloc[0]['cot_hoje'] / df_dolar.iloc[0]['cot_ant'] - 1) * 100, 2)

    # Determina Bovespa

    ibov = int(df_bovespa.iloc[0]['cot_hoje'])
    var_ibov = round((df_bovespa.iloc[0]['cot_hoje'] / df_bovespa.iloc[0]['cot_ant'] - 1) * 100, 2)

    # Apuração do dia

    #carteira['data'] = dt_maxima
    carteira = carteira.merge(df, on='ticker_yahoo')

    carteira['var_perc'] = (carteira.cot_hoje / carteira.cot_ant - 1) * 100
    carteira['valor'] = carteira.acum * carteira.cot_hoje
    carteira['var_valor'] = carteira.acum * (carteira.cot_hoje - carteira.cot_ant)

    carteira['var_saldo'] = carteira.acum * (carteira.cot_hoje - carteira.cot_ant)
    carteira.loc[(carteira.moeda == 'EUA'), 'var_saldo'] = carteira.var_saldo * dolar

    carteira['saldo'] = carteira.valor

    carteira['dolar'] = dolar

    carteira.loc[(carteira.moeda == 'EUA'), 'saldo'] = carteira.valor * dolar

    carteira.cot_ant = round(carteira.cot_ant, 2)
    carteira.cot_hoje = round(carteira.cot_hoje, 2)
    carteira.var_perc = round(carteira.var_perc, 2)
    carteira.valor = round(carteira.valor, 2)
    carteira.saldo = round(carteira.saldo, 2)
    carteira.var_valor = round(carteira.var_valor, 2)
    carteira.var_saldo = round(carteira.var_saldo, 2)

    ### CHECAR AQUI
    carteira['close'] = carteira.cot_hoje


    # Inclusão do preço médio
    sql = '''
        SELECT *
        FROM PrecoMedioAcao
        '''
    preco_medio = pd.read_sql(sql, conn)

    carteira = carteira.merge(preco_medio, on='ticker', how='left')

    # Salva tabela no banco de dados

    carteira.to_sql('CotacaoHoje', con=conn, if_exists='replace', index=False)

    dict_dolar_ibov = {
            'dolar': dolar,
            'var_dolar': var_dolar,
            'ibov': ibov,
            'var_ibov': var_ibov}


    # Atualiza Posição Diária Ação com cotações obtidas

    dt_ultima_cotacao_yfinance = config_get_parametro(conn, config_tag='dt_ultima_cotacao_yfinance')

    if dt_maxima >= dt_ultima_cotacao_yfinance:

        update_posicao_diaria_acao(conn, carteira)

        # Salva data da última cotação Yahoo Finance no Config
        config_update_parametro(conn, config_tag='dt_ultima_cotacao_yfinance', config_valor=dt_maxima)


    dia_var_valor = round(carteira.var_saldo.sum(), 2)

    print(carteira)
    print(f'Bovespa: {ibov}  {var_ibov}%')
    print(f'Dolar: {dolar}  {var_dolar}%')
    print(f'Var dia: {dia_var_valor}')

    return carteira, dict_dolar_ibov


def apuracao_posicao_diaria(conn, data_base, print_resumo=False):


    data_base = datetime.strptime(data_base, '%Y-%m-%d')

    # Posição diária Ações por corretora e tipo (Ação, FII, Stocks, REIT)

    sql = '''
            SELECT data, corret, tipo, ticker, sum(saldo) as saldo
            FROM PosicaoDiariaAcao
            GROUP BY data, corret, tipo, ticker
            ORDER BY data, corret, tipo, ticker
        '''
    pos_acao = pd.read_sql(sql, conn)
    pos_acao['classe'] = 'Ações'

    # Posição diária Tesouro Direto por corretora e tipo

    sql = '''
            SELECT data, corret, tipo, nome as ticker, sum(saldo) as saldo
            FROM PosicaoDiariaTD
            GROUP BY data, corret, tipo, nome
            ORDER BY data, corret, tipo, nome
        '''
    pos_td = pd.read_sql(sql, conn)
    pos_td['classe'] = 'Tesouro Direto'

    # Posição diária Fundos por corretora

    sql = '''
            SELECT data, corret, nome as ticker, sum(saldo) as saldo
            FROM PosicaoDiariaFundos
            GROUP BY data, corret, nome
            ORDER BY data, corret, nome
        '''
    pos_fundos = pd.read_sql(sql, conn)
    pos_fundos['tipo'] = ''
    pos_fundos['classe'] = 'Fundos'

    # Posição diária Renda Fixa por corretora

    sql = '''
            SELECT data, corret, tipo, nome as ticker, sum(saldo) as saldo
            FROM PosicaoDiariaRF
            GROUP BY data, corret, tipo, ticker
            ORDER BY data, corret, tipo, ticker
        '''
    pos_rf = pd.read_sql(sql, conn)
    pos_rf['classe'] = 'Renda Fixa'

    posicao_diaria = pd.concat([pos_acao, pos_td, pos_fundos, pos_rf])

    posicao_diaria.data = pd.to_datetime(posicao_diaria.data)

    posicao_diaria = posicao_diaria[['data', 'corret', 'classe', 'tipo', 'ticker', 'saldo']]

    posicao_diaria = posicao_diaria.sort_values(['data', 'corret', 'classe', 'tipo', 'ticker'])

    posicao_diaria.to_sql('PosicaoDiaria', con=conn, if_exists='replace', index=False)

    if print_resumo:

        # Totais por dia
        total_dia = posicao_diaria
        total_dia = total_dia[total_dia.data == data_base]

        # Posição Diária por classe, tipo e ticker
        print(total_dia.groupby(['data', 'classe', 'tipo', 'ticker']).sum())

        # Posição Diária por classe
        print(total_dia.groupby(['data', 'classe']).sum())

        # Posição Diária por classe e tipo
        print(total_dia.groupby(['data', 'classe', 'tipo']).sum())

        # Posição Diária por corretora e classe
        print(total_dia.groupby(['data', 'corret']).sum())

        # Posição Diária por corretora
        print(total_dia.groupby(['data', 'corret', 'classe']).sum())

        # Posição Total
        print('TOTAL:', total_dia.groupby(['data']).sum())


    return posicao_diaria


def processa_operacao_acao(conn, print_resumo=False):


    data_fim_yahoo = datetime.utcnow()

    data_hoje = date.today()

    data_fim = datetime.now().strftime('%Y-%m-%d')

    # Obtém relação de tickers para buscar histórico
    sql = '''
            SELECT oper_tipo, oper_ticker, sum(oper_qtd) as qtd, min(oper_data) as min_data
            FROM OperacaoAcao
            GROUP BY oper_tipo, oper_ticker
        '''
    cursor = conn.cursor()
    query_results = cursor.execute(sql).fetchall()

    df = pd.DataFrame(query_results)
    df.columns = ['tipo', 'ticker', 'qtd', 'data']

    data_ini = pd.to_datetime(df.data.min()) - timedelta(days=7)
    
    df['ticker_yahoo'] = df.ticker
    df.loc[df.tipo.isin(['Ação', 'FII', 'Crypto']), 'ticker_yahoo'] = df.ticker + '.SA'

    data = yf.download(df.ticker_yahoo.tolist(), start=data_ini, end=data_fim_yahoo, progress=True)

    # Datas para Yahoo Finance com todas as datas do histórico
    datas_yahoo = pd.DataFrame(pd.date_range(start=data_ini, end=data_hoje).tolist())
    datas_yahoo.columns = ['data']
    datas_yahoo.data = pd.to_datetime(datas_yahoo.data)

    # Obtém cotação histórica para todas as ações
    cotacao = pd.DataFrame()
    for ticker in df.ticker_yahoo:
        temp = datas_yahoo.copy()
        temp['ticker_yahoo'] = ticker
        yahoo_cotacao = data.loc[:, 'Close'][ticker].reset_index()
        temp = temp.merge(yahoo_cotacao, how='left', left_on='data', right_on='Date').fillna(axis=0, method='ffill')
        temp = temp[temp.data >= data_ini]
        temp.columns = ['data', 'ticker_yahoo', 'Date', 'close']
        cotacao = pd.concat([cotacao, temp])
    cotacao['ticker'] = cotacao.ticker_yahoo.str.replace('.SA', '', regex=False)
    cotacao = cotacao[['ticker', 'data', 'close', 'ticker_yahoo']]
    cotacao = cotacao.dropna()


    # Complementa cotações não trazidas pelo Yahoo Finance
    sql = '''
        SELECT * 
        FROM CotacoesINPUT
        '''
    cotacoes_input = pd.read_sql(sql, conn)
    cotacoes_input.data = pd.to_datetime(cotacoes_input.data)

    lista_tickers_input = cotacoes_input.ticker.drop_duplicates()

    df_cotacoes_input = datas_yahoo.merge(lista_tickers_input, how='cross')
    df_cotacoes_input = df_cotacoes_input.sort_values(['ticker', 'data'])
    df_cotacoes_input = df_cotacoes_input.merge(cotacoes_input, on=['ticker', 'data'], how='left').fillna(axis=0, method='ffill')
    df_cotacoes_input = df_cotacoes_input.dropna()
    df_cotacoes_input['ticker_yahoo'] = df_cotacoes_input.ticker + '.SA'

    cotacao = pd.concat([cotacao, df_cotacoes_input])

    cotacao = cotacao.drop_duplicates(['ticker', 'data'], keep='first')


    # Obtém quantidade de ações por dia para cada ticker
    sql = '''
            SELECT oper_corret, oper_tipo, oper_ticker, oper_moeda, oper_data, sum(oper_qtd) as qtd
            FROM OperacaoAcao
            GROUP BY oper_corret, oper_tipo, oper_ticker, oper_moeda, oper_data
            ORDER BY oper_corret, oper_tipo, oper_ticker, oper_moeda, oper_data
        '''
    cursor = conn.cursor()
    query_results = cursor.execute(sql).fetchall()

    df = pd.DataFrame(query_results)
    df.columns = ['corret', 'tipo', 'ticker', 'moeda', 'data', 'qtd']
    df.data = pd.to_datetime(df.data)

    # Calcula o valor da posição diária por ativo e corretora
    temp = df.groupby(['corret', 'tipo', 'ticker', 'moeda']).sum().reset_index()[['corret', 'tipo', 'ticker', 'moeda']]

    posicao_diaria = temp.merge(datas_yahoo, on=None, how='cross')
    posicao_diaria = pd.merge(posicao_diaria, df, on=['corret', 'tipo', 'ticker', 'moeda', 'data'], how='left').fillna(0)
    posicao_diaria['acum'] = posicao_diaria.groupby(['corret', 'tipo', 'ticker', 'moeda'])['qtd'].cumsum(axis=0)
    posicao_diaria = posicao_diaria[(posicao_diaria.acum > 0) | (posicao_diaria.qtd != 0)]
    posicao_diaria = posicao_diaria.merge(cotacao, on=['ticker', 'data'], how='left')
    posicao_diaria.close = posicao_diaria.close.fillna(0)
    posicao_diaria['valor'] = posicao_diaria.acum * posicao_diaria.close

    posicao_diaria.valor = posicao_diaria.valor.round(2)
    posicao_diaria.close = posicao_diaria.close.round(2)

    # Obtém histórico do dolar
    data = yf.download('USDBRL=X', start=data_ini, end=data_fim_yahoo, progress=False)
    dolar_historico = data.Close.reset_index()
    dolar_historico.columns=(['data', 'dolar'])
    dolar_historico.dolar = round(dolar_historico.dolar, 4)
    # Preenche sábado, domingo, feriado com dolar anterior
    dolar_historico = datas_yahoo.merge(dolar_historico, on='data', how='left').fillna(axis=0, method='ffill')
    dolar_historico.to_sql('HistoricoDolar', conn, if_exists='replace', index=False)

    # Obtém histórico do Bovespa
    data = yf.download('^BVSP', start=data_ini, end=data_fim_yahoo, progress=False)
    bovespa_historico = data.Close.reset_index()
    bovespa_historico.columns=(['data', 'valor'])
    bovespa_historico.valor = bovespa_historico.valor.astype(int)
    # Preenche sábado, domingo, feriado com dolar anterior
    bovespa_historico = datas_yahoo.merge(bovespa_historico, on='data', how='left').fillna(axis=0, method='ffill')
    bovespa_historico.to_sql('HistoricoBovespa', conn, if_exists='replace', index=False)

    posicao_diaria = posicao_diaria.merge(dolar_historico, on='data', how='left')
    posicao_diaria.dolar = posicao_diaria.dolar.fillna(0)
    posicao_diaria['saldo'] = posicao_diaria.valor
    posicao_diaria.loc[(posicao_diaria.moeda == 'EUA'), 'saldo'] = posicao_diaria.valor * posicao_diaria.dolar

    posicao_diaria.dolar = posicao_diaria.dolar.round(4)
    posicao_diaria.saldo = posicao_diaria.saldo.round(2)

    posicao_diaria = posicao_diaria.sort_values(['data', 'corret', 'tipo', 'ticker', 'moeda'])

    # Reordena colunas
    posicao_diaria = posicao_diaria[['data', 'corret', 'tipo', 'ticker', 'ticker_yahoo', 'moeda', 'qtd',
                                    'acum', 'close', 'valor', 'dolar', 'saldo']]

    # Salva posição diária no banco de dados
    posicao_diaria.to_sql('PosicaoDiariaAcao', con=conn, if_exists='replace', index=False)

    if print_resumo:

        # Totais por dia
        total_dia = posicao_diaria.groupby(['corret', 'tipo', 'ticker', 'moeda', 'data']).sum(['valor', 'valor_real']).reset_index()[['corret', 'tipo', 'ticker', 'moeda', 'data', 'acum', 'valor', 'saldo']]
        total_dia = total_dia[total_dia.data == data_hoje]

        # Posição Diária por ticker
        print(total_dia.groupby(['data', 'corret', 'tipo', 'ticker', 'moeda']).sum())

        # Posição Diária por corretora/tipo
        print(total_dia.groupby(['data', 'corret', 'tipo', 'moeda']).sum())

        # Posição Diária por tipo
        print(total_dia.groupby(['data', 'tipo', 'moeda']).sum())

        # Posição Diária por corretora
        print(total_dia.groupby(['data', 'corret', 'moeda']).sum())

        # Total Brasil e EUA
        print('BR ', total_dia[total_dia.moeda == 'BRL'].groupby(['data']).sum())
        print('EUA', total_dia[total_dia.moeda != 'BRL'].groupby(['data']).sum())

        # Posição Total
        print('TOTAL:', total_dia.groupby(['data']).sum())


    return posicao_diaria


def processa_operacao_fundos(conn, print_resumo=False):


    URL_CVM =  'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/'

    data_hoje = datetime.now().strftime('%Y-%m-%d')

    cursor = conn.cursor()

    def downloadCVM(url, cursor):
        '''
        Acessa Histórico dos Fundos CVM do portal Dados Abertos para obter valor histórico
        Salva base em cache e atualiza data do último download
        Se já foi feito download no dia, obtém base do cache
        Só serão armazenadas informações dos CNPJ com investimento
        '''
        erro_download = []

        # Acessa primeira data de início das operações

        sql = '''
                SELECT min(oper_data) as data
                FROM OperacaoFundos
            '''

        df = pd.read_sql(sql, conn)
        data_ini = df.data.iloc[0][:10]

        # Verifica último download da base CVM
        sql = '''
                SELECT *
                FROM Config
                WHERE config_tag = 'dt_download_CVM'
            '''
        cursor = conn.cursor()
        query_results = cursor.execute(sql).fetchall()

        config = pd.DataFrame(query_results)
        config.columns = ['config_tag', 'config_valor', 'config_desc']

        dt_download_CVM = config.config_valor.iloc[0]

        if datetime.now().strftime('%Y-%m-%d') == dt_download_CVM:

            # Acessa Base de dados salva
            cvm = pd.read_sql('SELECT * FROM FundosCVM', conn)

            return cvm, data_ini, erro_download


        # Acessa lista CNPJ dos investimentos

        sql = '''
                SELECT inv_cnpj
                FROM Investimento
                WHERE inv_cnpj <> ''
            '''

        lista_cnpj = pd.read_sql(sql, conn)


        # Verifica data do último download para cada arquivo mensal

        sql = '''
                SELECT *
                FROM ControleDownloadCVM
            '''
        dwld_cvm = pd.read_sql(sql, conn)
        dwld_cvm.cvm_dwld_dt_download = pd.to_datetime(dwld_cvm.cvm_dwld_dt_download)

        # Acessa site CVM para verificar arquivos mensais para download

        try:

            resp = req.get(url)

        except Exception as e:

            print(e)
            
            erro_download.append('Erro acesso ao Portal Dados Abertos CVM')

            cvm = pd.read_sql('SELECT * FROM FundosCVM', conn)

            return cvm, data_ini, erro_download


        bs = BeautifulSoup(resp.text, 'html.parser')

        tab = bs.find_all('table', {'id': 'indexlist'})

        # Lista armazena todos os arquivos listados no site
        lista = []
        for i in tab:
            class_indexcolname = i.findChildren('td', 'indexcolname')
            class_indexcollastmod = i.findChildren('td', 'indexcollastmod')
            for j, z in enumerate(class_indexcolname):
                if class_indexcolname[j].text.startswith('inf_diario'):
                    lista.append([class_indexcolname[j].text.strip(), class_indexcollastmod[j].text.strip()])

        # df contém a lista dos arquivos a serem baixados

        df = pd.DataFrame(lista, columns=['nome', 'last_mod'])
        df['ano_mes'] = df.nome.str[14:20]
        df['last_mod'] = pd.to_datetime(df.last_mod)

        df = df.merge(dwld_cvm, left_on='ano_mes', right_on='cvm_dwld_ano_mes', how='left')
        df.cvm_dwld_dt_download = pd.to_datetime(df.cvm_dwld_dt_download)
        df.cvm_dwld_dt_download = df.cvm_dwld_dt_download.fillna(value=pd.to_datetime('2000-01-01'))
        df = df[df.last_mod > df.cvm_dwld_dt_download]
        df = df[df.ano_mes >= data_ini[:4] + data_ini[5:7]]
        df.cvm_dwld_dt_download = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        for arq, ano_mes in zip(df.nome, df.ano_mes):

            print('Download do arquivo:', arq)
            
            # Download do arquivo da CVM        
            
            try:

                cvm = pd.read_csv(URL_CVM + arq, sep=';',
                                                usecols=['CNPJ_FUNDO', 'DT_COMPTC', 'VL_QUOTA'],
                                                dtype={'VL_QUOTA': 'float64'},
                                                parse_dates=['DT_COMPTC'],
                                                dayfirst=False)

                cvm.columns = ['fundo_cnpj', 'fundo_data', 'fundo_vlr_quota']

                # Retira CNPJ sem investimento

                cvm = cvm[cvm.fundo_cnpj.isin(lista_cnpj.inv_cnpj)]

                cvm['ano_mes'] = ano_mes

                # Remove registros anteriores do respectivo ano/mês
                sql = '''
                        DELETE
                        FROM FundosCVM
                        WHERE ano_mes = ? 
                    '''
                cursor.execute(sql, (ano_mes,))
                conn.commit()

                cvm.to_sql('FundosCVM', conn, if_exists='append', index=False)
                
            except Exception as e:

                print(e)
                
                erro_download.append(arq)

        # Elimina do df os arquivos com erro de download
        df = df[~df.nome.isin(erro_download)]

        # Atualiza tabela ControleDownloadCVM com novas datas de download

        df = df[['ano_mes', 'cvm_dwld_dt_download']]
        df.columns = ['cvm_dwld_ano_mes', 'cvm_dwld_dt_download']
        
        dwld_cvm = pd.concat([dwld_cvm, df])
        dwld_cvm.cvm_dwld_dt_download = pd.to_datetime(dwld_cvm.cvm_dwld_dt_download)
        dwld_cvm = dwld_cvm.sort_values(['cvm_dwld_ano_mes', 'cvm_dwld_dt_download'])
        dwld_cvm = dwld_cvm.drop_duplicates('cvm_dwld_ano_mes', keep='last')
        
        dwld_cvm.to_sql('ControleDownloadCVM', conn, if_exists='replace', index=False)

        cvm = pd.read_sql('SELECT * FROM FundosCVM', conn)

        # Atualiza Config com data do download
        # Não atualiza caso tenha dado erro em algum download

        if erro_download == []:

            cursor.execute('''
                UPDATE Config
                SET config_valor = ?
                WHERE config_tag = ?
            ''', (data_hoje, 'dt_download_CVM'))
            
            conn.commit()
        
        return cvm, data_ini, erro_download


    # Acessa histórico da base CVM

    cvm, data_ini, erro_download = downloadCVM(url=URL_CVM, cursor=cursor)

    cvm = cvm.sort_values(['fundo_cnpj', 'fundo_data'])

    for arq in erro_download:
        print('Erro no download do arquivo', arq)    

    cvm.fundo_data = pd.to_datetime(cvm.fundo_data)

    # Datas para Histórico CVM

    datas_cvm = pd.DataFrame(pd.date_range(start=data_ini, end=data_hoje).tolist())
    datas_cvm.columns = ['data']
    datas_cvm.data = pd.to_datetime(datas_cvm.data)

    # Obtém Operações Fundos agrupadas por corretora, fundo e dia

    sql = '''
            SELECT oper.*, inv.inv_cnpj
            FROM OperacaoFundos AS oper
            LEFT JOIN Investimento AS inv
            ON oper.oper_nome = inv.inv_nome    '''
    cursor = conn.cursor()
    query_results = cursor.execute(sql).fetchall()

    oper_cvm = pd.DataFrame(query_results)
    oper_cvm.columns = ['data', 'oper', 'corret', 'nome', 'qtd', 'preco_unit', 'valor', 'cnpj']
    oper_cvm.data = pd.to_datetime(oper_cvm.data)

    # Fundos com operação

    fundos = oper_cvm[['nome', 'cnpj']].drop_duplicates()


    # Seleciona histórico somente dos fundos com operação

    cvm = cvm[(cvm.fundo_cnpj.isin(fundos.cnpj))]

    historico_cvm = fundos.merge(datas_cvm, on=None, how='cross')

    historico_cvm = historico_cvm.merge(cvm,
                                    left_on=['cnpj', 'data'],
                                    right_on=['fundo_cnpj', 'fundo_data'],
                                    how='left').fillna(axis=0, method='ffill').dropna()

    historico_cvm = historico_cvm[['nome', 'cnpj', 'data', 'fundo_vlr_quota']]

    # Calcula o valor da posição diária por ativo e corretora

    cvm_corretora = oper_cvm[['corret', 'cnpj']].drop_duplicates()

    posicao_diaria = cvm_corretora.merge(historico_cvm, on=['cnpj'], how='left')

    posicao_diaria = posicao_diaria.merge(oper_cvm, on=['corret', 'cnpj', 'nome', 'data'], how='left')
    posicao_diaria = posicao_diaria[['corret', 'cnpj', 'nome', 'data', 'fundo_vlr_quota', 'qtd']]
    posicao_diaria.qtd = posicao_diaria.qtd.fillna(0)

    posicao_diaria['qtd_acum'] = posicao_diaria.groupby(['corret', 'cnpj'])['qtd'].cumsum(axis=0)
    posicao_diaria = posicao_diaria.dropna()
    posicao_diaria = posicao_diaria[(posicao_diaria.qtd_acum > 0) | (posicao_diaria.qtd != 0)]
    posicao_diaria['saldo'] = round(posicao_diaria.qtd_acum * posicao_diaria.fundo_vlr_quota, 2)

    # Classifica e reordena colunas

    posicao_diaria = posicao_diaria.sort_values(['data', 'corret', 'nome'])[
        ['data', 'corret', 'nome', 'cnpj', 'qtd', 'qtd_acum', 'fundo_vlr_quota', 'saldo']]

    # Salva posição diária no banco de dados

    posicao_diaria.to_sql('PosicaoDiariaFundos', con=conn, if_exists='replace', index=False)

    if print_resumo:

        # Totais por dia
        total_dia = posicao_diaria.groupby(['corret', 'nome', 'tipo', 'cnpj', 'data']).sum(['valor']).reset_index()[['corret', 'nome', 'tipo', 'cnpj', 'data', 'qtd_acum', 'saldo']]
        total_dia = total_dia[total_dia.data == data_hoje]

        # Posição Diária por fundo
        print(total_dia.groupby(['data', 'nome', 'tipo']).sum())

        # Posição Diária por tipo
        print(total_dia.groupby(['data', 'tipo']).sum())

        # Posição Diária por corretora/fundo
        print(total_dia.groupby(['data', 'corret', 'nome', 'tipo']).sum())

        # Posição Diária por corretora/tipo
        print(total_dia.groupby(['data', 'corret', 'tipo']).sum())

        # Posição Diária por corretora
        print(total_dia.groupby(['data', 'corret']).sum())

        # Posição Total
        print('TOTAL:', total_dia.groupby(['data']).sum())


    return posicao_diaria


def processa_operacao_rf(conn, print_resumo=False):


    data_hoje = datetime.now().strftime('%Y-%m-%d')

    #cursor = conn.cursor()

    # Obtém histórico do dolar
    sql = '''
        SELECT *
        FROM HistoricoDolar
        '''
    dolar_historico = pd.read_sql(sql, conn)
    dolar_historico.data = pd.to_datetime(dolar_historico.data)

    # Obtém Operações Renda Fixa agrupadas por corretora, nome e dia

    sql = '''
        SELECT rf_corret, rf_tipo, rf_nome, rf_moeda, rf_data, sum(rf_valor_deb) as rf_deb, sum(rf_valor_cred) as rf_cred
        FROM OperacaoRF
        GROUP BY rf_corret, rf_tipo, rf_nome, rf_moeda, rf_data
        ORDER BY rf_corret, rf_tipo, rf_nome, rf_moeda, rf_data
        '''
    oper_rf = pd.read_sql(sql, conn)

    oper_rf.columns = ['corret', 'tipo', 'nome', 'moeda', 'data', 'deb', 'cred']
    oper_rf.data = pd.to_datetime(oper_rf.data)
    oper_rf['movdia'] = oper_rf.cred - oper_rf.deb

    # Determina data de início das operações

    data_ini = oper_rf.data.min().strftime('%Y-%m-%d')

    # Datas para Histórico RF

    datas_rf = pd.DataFrame(pd.date_range(start=data_ini, end=data_hoje).tolist())
    datas_rf.columns = ['data']
    datas_rf.data = pd.to_datetime(datas_rf.data)


    # Investimentos com operação

    invest = oper_rf[['corret', 'tipo', 'nome', 'moeda']].drop_duplicates()

    # Calcula o valor da posição diária por investimento

    posicao_diaria = invest.merge(datas_rf, on=None, how='cross')
    posicao_diaria = posicao_diaria.merge(oper_rf, on=['corret', 'tipo', 'nome', 'moeda', 'data'], how='left')
    posicao_diaria = posicao_diaria.fillna(0)

    posicao_diaria = posicao_diaria.merge(dolar_historico, on='data', how='left')
    posicao_diaria.dolar = posicao_diaria.dolar.fillna(0)

    posicao_diaria['saldo'] = round(posicao_diaria.groupby(['corret', 'tipo', 'nome', 'moeda'])['movdia'].cumsum(axis=0), 2)
    posicao_diaria.loc[(posicao_diaria.moeda == 'EUA'), 'saldo'] = posicao_diaria.saldo * posicao_diaria.dolar

    posicao_diaria = posicao_diaria[(posicao_diaria.movdia != 0) | (posicao_diaria.saldo != 0)]

    # Classifica e reordena colunas

    posicao_diaria = posicao_diaria.sort_values(['data', 'corret', 'tipo', 'nome', 'moeda'])[
        ['data', 'corret', 'tipo', 'nome', 'moeda', 'dolar', 'saldo']]

    # Salva posição diária no banco de dados

    posicao_diaria.to_sql('PosicaoDiariaRF', con=conn, if_exists='replace', index=False)

    if print_resumo:

        # Totais por dia
        total_dia = posicao_diaria
        total_dia = total_dia[total_dia.data == data_hoje]

        # Posição Diária por investimento
        print(total_dia.groupby(['data', 'corret', 'tipo', 'nome']).sum())

        # Posição Diária por corretora
        print(total_dia.groupby(['data', 'corret']).sum())

        # Posição Total
        print('TOTAL:', total_dia.groupby(['data']).sum())


    return posicao_diaria


def processa_operacao_td(conn, print_resumo=False):
    
    
    data_hoje = datetime.now().strftime('%Y-%m-%d')

    cursor = conn.cursor()
        

    def downloadTD(data_hoje, titulos, cursor):
        '''
        Acessa Tesouro Direto do portal Dados Abertos para obter preço histórico
        Salva base em cache e atualiza data do último download
        Se já foi feito download no dia, obtém base do cache
        '''
        url =  'https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3/resource/796d2059-14e9-44e3-80c9-2d9e30b405c1/download/PrecoTaxaTesouroDireto.csv'

        # Verifica último download da base Tesouro Direto
    
        sql = '''
                SELECT *
                FROM Config
                WHERE config_tag = 'dt_download_TD'
            '''
        cursor = conn.cursor()
        query_results = cursor.execute(sql).fetchall()

        config = pd.DataFrame(query_results)
        config.columns = ['config_tag', 'config_valor', 'config_desc']

        dt_download_TD = config.config_valor.loc[0]

        if data_hoje > dt_download_TD:

            print('Download base Tesouro Direto')
    
            td = pd.read_csv(url, sep=';', thousands='.', decimal=',')

            # Seleciona histórico somente dos títulos com operação

            td = td[(td['Tipo Titulo'] + td['Data Vencimento']).isin(titulos.tipo + titulos.vencto)]

            td.to_sql('TesouroDireto', conn, if_exists='replace', index=False)

            # Atualiza Config com data do download
            cursor.execute('''
                UPDATE Config
                SET config_valor = ?
                WHERE config_tag = ?
            ''', (data_hoje, 'dt_download_TD'))

            # Armazena data base do último preço para cada título

            df = td[['Tipo Titulo', 'Data Vencimento', 'Data Base']].copy()
            df['nome'] = df['Tipo Titulo'] + ' ' + df['Data Vencimento']
            df['data_base'] = pd.to_datetime(df['Data Base'], dayfirst=True)
            df = df.sort_values(['nome', 'data_base'])            
            df = df.groupby(['nome']).max().reset_index()[['nome', 'data_base']]
            df.data_base = df.data_base.dt.strftime('%Y-%m-%d')

            df.to_sql('DataBaseTesouroDireto', conn, if_exists='replace', index=False)
            
            conn.commit()

        else:

            td = pd.read_sql('SELECT * FROM TesouroDireto', conn)

        
        # Ajusta coluna Data Base para formato data
        td['Data Base'] = pd.to_datetime(td['Data Base'], dayfirst=True)

        return td


    # Obtém Operações Tesouro Direto agrupadas por corretora, título e dia

    sql = '''
            SELECT oper_corret, oper_tipo, oper_vencto, oper_data, sum(oper_qtd) as qtd
            FROM OperacaoTD
            GROUP BY oper_corret, oper_tipo, oper_vencto, oper_data
            ORDER BY oper_corret, oper_tipo, oper_vencto, oper_data
        '''
    cursor = conn.cursor()
    query_results = cursor.execute(sql).fetchall()

    oper_td = pd.DataFrame(query_results)
    oper_td.columns = ['corret', 'tipo', 'vencto', 'data', 'qtd']
    oper_td.data = pd.to_datetime(oper_td.data)
    oper_td['nome'] = oper_td.tipo + ' ' + oper_td.vencto

    data_ini = oper_td.data.min().strftime('%Y-%m-%d')

    # Títulos com operação

    titulos = oper_td[['tipo', 'vencto']].drop_duplicates()

    # Datas para Histórico Tesouro Direto

    datas_td = pd.DataFrame(pd.date_range(start=data_ini, end=data_hoje).tolist())
    datas_td.columns = ['data']
    datas_td.data = pd.to_datetime(datas_td.data)

    # Obtém Histórico da base Tesouro Direto

    td = downloadTD(data_hoje=data_hoje, titulos=titulos, cursor=cursor)

    # Seleciona histórico somente dos títulos com operação

    #td = td[(td['Tipo Titulo'] + td['Data Vencimento']).isin(titulos.tipo + titulos.vencto)]

    historico_td = titulos.merge(datas_td, on=None, how='cross')

    historico_td = historico_td.merge(td,
                                    left_on=['tipo', 'vencto', 'data'],
                                    right_on=['Tipo Titulo', 'Data Vencimento', 'Data Base'],
                                    how='left').fillna(axis=0, method='ffill').dropna()

    historico_td = historico_td[['tipo', 'vencto', 'data', 'PU Venda Manha']]
    historico_td.columns=['tipo', 'vencto', 'data', 'preco_unit']


    # Calcula o valor da posição diária por ativo e corretora

    td_corretora = oper_td[['corret', 'tipo', 'vencto']].drop_duplicates()

    posicao_diaria = td_corretora.merge(historico_td, on=['tipo', 'vencto'], how='left')

    posicao_diaria = posicao_diaria.merge(oper_td, on=['corret', 'tipo', 'vencto', 'data'], how='left')
    posicao_diaria.qtd = posicao_diaria.qtd.fillna(0)
    posicao_diaria.nome = posicao_diaria.nome.fillna(axis=0, method='ffill')
    posicao_diaria['qtd_acum'] = round(posicao_diaria.groupby(['corret', 'tipo', 'vencto'])['qtd'].cumsum(axis=0), 2)
    posicao_diaria = posicao_diaria.dropna()
    posicao_diaria = posicao_diaria[(posicao_diaria.qtd_acum != 0) | (posicao_diaria.qtd != 0)]
    posicao_diaria['saldo'] = round(posicao_diaria.qtd_acum * posicao_diaria.preco_unit, 2)

    # Classifica e reordena colunas

    posicao_diaria = posicao_diaria.sort_values(['data', 'corret', 'nome'])[
        ['data', 'corret', 'nome', 'tipo', 'vencto', 'qtd', 'qtd_acum', 'preco_unit', 'saldo']]

    # Salva posição diária no banco de dados

    posicao_diaria.to_sql('PosicaoDiariaTD', con=conn, if_exists='replace', index=False)

    if print_resumo:

        # Totais por dia
        total_dia = posicao_diaria.groupby(['corret', 'tipo', 'vencto', 'nome', 'data']).sum(['valor']).reset_index()[['corret', 'tipo', 'vencto', 'nome', 'data', 'qtd_acum', 'saldo']]
        total_dia = total_dia[total_dia.data == data_hoje]

        # Posição Diária por título
        print(total_dia.groupby(['data', 'corret', 'nome']).sum())

        # Posição Diária por corretora/tipo
        print(total_dia.groupby(['data', 'corret', 'nome']).sum())

        # Posição Diária por tipo
        print(total_dia.groupby(['data', 'tipo']).sum())

        # Posição Diária por corretora
        print(total_dia.groupby(['data', 'corret']).sum())

        # Posição Total
        print('TOTAL:', total_dia.groupby(['data']).sum())


    return posicao_diaria


def importa_operacoes(conn, nome_arquivo, f):

    nome_arquivo = nome_arquivo.replace('.csv', '')

    lista_tabelas = [
        'Investimento',
        'OperacaoAcao',
        'OperacaoTD',
        'OperacaoFundos',
        'OperacaoRF',
        'ProventosAcao',
        'CotacoesINPUT',
        'DolarOficial',
        'Empresas',
        'AcoesInteresse']

    if nome_arquivo in lista_tabelas:

        df = pd.read_csv(f, encoding='latin-1', sep=';', decimal=',')

        if nome_arquivo == 'Investimento':
            df.inv_cnpj = df.inv_cnpj.fillna('')
     
        if nome_arquivo in ['OperacaoAcao', 'OperacaoTD', 'OperacaoFundos']:
            df.oper_data = pd.to_datetime(df.oper_data, dayfirst=True)

        if nome_arquivo == 'OperacaoRF':
            df.rf_data = pd.to_datetime(df.rf_data, dayfirst=True)

        if nome_arquivo == 'ProventosAcao':
            df.prov_data = pd.to_datetime(df.prov_data, dayfirst=True)

        if nome_arquivo == 'CotacoesINPUT':
            df.data = pd.to_datetime(df.data, dayfirst=True)

        if nome_arquivo == 'DolarOficial':
            df.data = pd.to_datetime(df.data, dayfirst=True)

        df.to_sql(nome_arquivo, conn, index=False, if_exists='replace')


def update_posicao_diaria_acao(conn, carteira):

    cursor = conn.cursor()

    json_result = carteira.to_json(orient="records")

    json_parsed = json.loads(json_result)

    for cotacao in json_parsed:
        
        cotacao['data'] = datetime.strptime(cotacao['data'], '%Y-%m-%d')

        # Atualiza cotação
        cursor.execute('''
            UPDATE PosicaoDiariaAcao
            SET close = ?,
                dolar = ?
            WHERE data = ? AND
                tipo = ? AND
                ticker = ?
        ''', ((cotacao['close'],
               cotacao['dolar'],
               cotacao['data'], 
               cotacao['tipo'],
               cotacao['ticker'],)))
        
        # Calcula saldo para cada ticker
        cursor.execute('''
            UPDATE PosicaoDiariaAcao
            SET valor = acum * close,
                saldo = valor
            WHERE data = ? AND
                  tipo = ? AND
                  ticker = ?
        ''', ((cotacao['data'], 
               cotacao['tipo'],
               cotacao['ticker'],)))
        
        # Calcula saldo em reais para ativos em dolar
        cursor.execute('''
            UPDATE PosicaoDiariaAcao
            SET saldo = ROUND(valor * dolar, 2)
            WHERE moeda = 'EUA' AND
                  data = ? AND
                  tipo = ? AND
                  ticker = ?
        ''', ((cotacao['data'],        
               cotacao['tipo'],
               cotacao['ticker'],)))
        
    conn.commit()


def config_get_parametro(conn, config_tag):


    cursor = conn.cursor()

    sql = '''
        SELECT config_valor
        FROM Config
        WHERE config_tag = ?
    '''

    t = (config_tag, )

    query_results = cursor.execute(sql, t).fetchall()

    config_valor = query_results[0][0]

    return config_valor


def config_update_parametro(conn, config_tag, config_valor):


    cursor = conn.cursor()

    sql = '''
        UPDATE Config
        SET config_valor = ?
        WHERE config_tag = ?
    '''

    t = (config_valor, config_tag, )

    cursor.execute(sql, t)

    conn.commit()


def inicializacao(conn):
    
    # Verifica se Posição Diária Ação está atualizada
    data_hoje = datetime.today().strftime('%Y-%m-%d')

    dt_ultima_cotacao_yfinance = config_get_parametro(conn, config_tag='dt_ultima_cotacao_yfinance')

    if data_hoje > dt_ultima_cotacao_yfinance:


        processa_operacao_acao(conn, print_resumo=True)

        processa_operacao_td(conn)

        processa_operacao_fundos(conn)

        processa_operacao_rf(conn)

        # Salva data da última cotação Yahoo Finance no Config
        config_update_parametro(conn, config_tag='dt_ultima_cotacao_yfinance', config_valor=data_hoje)


def get_cotacao_data_unica_YahooFinance(conn, lista_tickers_yahoo, data_base, acessa_YFinance=False):


    data_hoje = date.today().strftime('%Y-%m-%d')

    # Se data antiga, não acessa Yahoo Finance
    # obtém do histórico PosicaoDiariaAcao
    if data_base != data_hoje or acessa_YFinance == False:
        df = get_cotacao_data_passada(conn, data_base)
        return df


    data_fim_yahoo = datetime.utcnow()

    data_ini_yahoo = data_fim_yahoo - timedelta(days=5)

    # Busca dados Yahoo Finance

    df = yf.download(lista_tickers_yahoo, start=data_ini_yahoo, end=data_fim_yahoo, progress=True)
    '''
    # Deleta data posterior a hoje (UFC)
    # Cotações (exceto dolar) zeradas
    temp = df.T.reset_index().copy()
    temp = temp[temp['level_1'] != 'USDBRL=X']
    soma = temp.iloc[:,-1].sum()
    if soma == 0:
        num_ult_linha = df.index.shape[0]-1
        idx = df.index.delete(num_ult_linha)
        df = df.loc[idx]
    '''
    dt_maxima = max(df.index).strftime('%Y-%m-%d')
    if dt_maxima > data_hoje:
        dt_maxima = data_hoje

    df.loc[:]['Close'] = df.loc[:]['Close'].fillna(axis=0, method='ffill')
    df = df.iloc[[-2,-1]]

    df = df.fillna(axis=0, method='bfill')
    #df = df.fillna(0)
    df = df.T.reset_index()
    df.columns = ['tipo', 'ticker_yahoo', 'cot_ant', 'cot_hoje']
    df = df[df.tipo == 'Close']
    df['data'] = dt_maxima
    df = df[['data', 'ticker_yahoo', 'cot_ant', 'cot_hoje']]

    df = df.round(2)

    df.to_sql('CotacaoYahooFinance', conn, if_exists='replace', index=False)

    return df


def saldos_ultimo_dia_mes(df, data_base, lista_agrupamento):
    '''
    Retorna dataframe com o saldo no último dia do mês
    para cada investimento
    Se mês corrente, retorna saldo do dia atual
    '''
    data_hoje = date.today().strftime('%Y-%m-%d')

    # Se data base for mês corrente, traz tudo, senão até data base informada
    if data_hoje[0:7] == data_base[0:7]:
        df = df[(df.data == df.data + MonthEnd(0)) | (df.data == data_base)]
    else:
        df = df[(df.data == df.data + MonthEnd(0)) & (df.data <= data_base)]

    df = df.groupby(lista_agrupamento).sum().reset_index()

    return df


def get_cotacao_data_passada(conn, data_base):


    # Recupera cotação do histórico (PosicaoDiariaAcao)

    data_de = (datetime.strptime(data_base, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    data_ate = (datetime.strptime(data_base, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

    sql = '''
            SELECT ticker_yahoo, data, close
            FROM PosicaoDiariaAcao
            WHERE data BETWEEN '{}' AND '{}'
            ORDER BY ticker_yahoo, data
        '''
    sql = sql.format(data_de, data_ate)

    df = pd.read_sql(sql, conn)

    df = df.groupby(['ticker_yahoo']).agg({'close': ['first', 'last']}).reset_index()
    df.columns= ['ticker_yahoo', 'cot_ant', 'cot_hoje']
    df['data'] = data_base
    df = df[['data', 'ticker_yahoo', 'cot_ant', 'cot_hoje']]

    # Obtém dolar e Bovespa na data base
    df_dolar = get_dolar_data(conn, data_base)
    df_bovespa = get_bovespa_data(conn, data_base)

    df = pd.concat([df, df_dolar, df_bovespa])

    return df


def get_dolar_data(conn, data_base):


    # Recupera histórico

    # Se data_base = hoje, acessa Yahoo Finance para cotação atual

    data_hoje = datetime.today().strftime('%Y-%m-%d')

    if data_hoje == data_base:
        data_de = (datetime.strptime(data_base, '%Y-%m-%d') - timedelta(days=5)).strftime('%Y-%m-%d')
        data_ate = (datetime.strptime(data_base, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        df = yf.download('USDBRL=X', start=data_de, end=data_ate, progress=False)
        df = df.iloc[[-2,-1]].T.reset_index()
        df = df[df['index'] == 'Close']
        df.columns = ['tipo', 'cot_ant', 'cot_hoje']
        df['data'] = data_base
        df.cot_ant = round(df.cot_ant, 4)
        df.cot_hoje = round(df.cot_hoje, 4)
        df['ticker_yahoo'] = 'USDBRL=X'
        df = df[['data', 'ticker_yahoo', 'cot_ant', 'cot_hoje']]
        return df 
    
    
    # Recupera histórico

    data_de = (datetime.strptime(data_base, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    data_ate = (datetime.strptime(data_base, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

    sql = '''
            SELECT 'dolar' as string_dolar, data, dolar
            FROM HistoricoDolar
            WHERE data BETWEEN '{}' AND '{}'
            ORDER BY data
        '''
    sql = sql.format(data_de, data_ate)

    df = pd.read_sql(sql, conn)

    df = df.groupby(['string_dolar']).agg({'dolar': ['first', 'last']}).reset_index()
    df.columns= ['dolar', 'cot_ant', 'cot_hoje']
    df['data'] = data_base
    df['ticker_yahoo'] = 'USDBRL=X'
    df = df[['data', 'ticker_yahoo', 'cot_ant', 'cot_hoje']]

    return df


def get_bovespa_data(conn, data_base):


    # Se data_base = hoje, acessa Yahoo Finance para cotação atual

    data_hoje = datetime.today().strftime('%Y-%m-%d')

    if data_hoje == data_base:
        data_de = (datetime.strptime(data_base, '%Y-%m-%d') - timedelta(days=5)).strftime('%Y-%m-%d')
        data_ate = (datetime.strptime(data_base, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        df = yf.download('^BVSP', start=data_de, end=data_ate, progress=False)
        df = df.iloc[[-2,-1]].T.reset_index()
        df = df[df['index'] == 'Close']
        df.columns = ['tipo', 'cot_ant', 'cot_hoje']
        df['data'] = data_base
        df.cot_ant = round(df.cot_ant, 0)
        df.cot_hoje = round(df.cot_hoje, 0)
        df['ticker_yahoo'] = '^BVSP'
        df = df[['data', 'ticker_yahoo', 'cot_ant', 'cot_hoje']]
        return df 
    
    
    # Recupera histórico

    data_de = (datetime.strptime(data_base, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    data_ate = (datetime.strptime(data_base, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

    sql = '''
            SELECT 'bovespa' as bovespa, data, valor
            FROM HistoricoBovespa
            WHERE data BETWEEN '{}' AND '{}'
            ORDER BY data
        '''
    sql = sql.format(data_de, data_ate)

    df = pd.read_sql(sql, conn)

    df = df.groupby(['bovespa']).agg({'valor': ['first', 'last']}).reset_index()
    df.columns= ['bovespa', 'cot_ant', 'cot_hoje']
    df['data'] = data_base
    df['ticker_yahoo'] = '^BVSP'
    df = df[['data', 'ticker_yahoo', 'cot_ant', 'cot_hoje']]

    return df


def aportes_acao_por_periodo(conn, periodicidade):


    sql = '''
        SELECT oper_data,
            oper_moeda,
            oper_qtd,
            oper_valor,
            dolar
        FROM OperacaoAcao as t1
        INNER JOIN HistoricoDolar as t2
        ON t1.oper_data = t2.data
    '''
    df = pd.read_sql(sql, conn)

    df.loc[df.oper_qtd < 0, 'oper_valor'] = df.oper_valor * -1
    df['valor'] = df.oper_valor
    df.loc[df.oper_moeda == 'EUA', 'valor'] = df.oper_valor * df.dolar

    df.oper_data = pd.to_datetime(df.oper_data)

    df['data'] = df['oper_data']
    if periodicidade == 'mes':
        df['data'] = df['oper_data'] + MonthEnd(0)

    aporte = df.groupby(['data']).sum().reset_index()
    aporte = aporte[['data', 'valor']]
    aporte['classe'] = 'Ações'
    
    return aporte


def aportes_td_por_periodo(conn, periodicidade):


    sql = '''
        SELECT oper_data,
            oper_qtd,
            oper_valor
        FROM OperacaoTD
    '''
    df = pd.read_sql(sql, conn)

    df.loc[df.oper_qtd < 0, 'oper_valor'] = df.oper_valor * -1
    df['valor'] = df.oper_valor

    df.oper_data = pd.to_datetime(df.oper_data)

    df['data'] = df['oper_data']
    if periodicidade == 'mes':
        df['data'] = df['oper_data'] + MonthEnd(0)
    
    aporte = df.groupby(['data']).sum().reset_index()
    aporte = aporte[['data', 'valor']]
    aporte['classe'] = 'Tesouro Direto'
    
    return aporte


def aportes_fundos_por_periodo(conn, periodicidade):


    sql = '''
        SELECT oper_data,
            oper_qtd,
            oper_valor
        FROM OperacaoFundos
    '''
    df = pd.read_sql(sql, conn)

    df.loc[df.oper_qtd < 0, 'oper_valor'] = df.oper_valor * -1
    df['valor'] = df.oper_valor

    df.oper_data = pd.to_datetime(df.oper_data)

    df['data'] = df['oper_data']
    if periodicidade == 'mes':
        df['data'] = df['oper_data'] + MonthEnd(0)
    
    aporte = df.groupby(['data']).sum().reset_index()
    aporte = aporte[['data', 'valor']]
    aporte['classe'] = 'Fundos'
    
    return aporte


def aportes_rf_por_periodo(conn, periodicidade):


    sql = '''
        SELECT rf_data, rf_valor_deb, rf_valor_cred
        FROM OperacaoRF
        WHERE rf_oper IN ('Saldo Inicial', 'Aplicação', 'Resgate', 'Entradas', 'Saídas')
    '''
    df = pd.read_sql(sql, conn)

    df['valor'] = df.rf_valor_cred - df.rf_valor_deb

    df.rf_data = pd.to_datetime(df.rf_data)

    df['data'] = df['rf_data']
    if periodicidade == 'mes':
        df['data'] = df['rf_data'] + MonthEnd(0)

    aporte = df.groupby(['data']).sum().reset_index()
    aporte = aporte[['data', 'valor']]
    aporte['classe'] = 'Renda Fixa'
    
    return aporte


def dividendos_por_periodo(conn, periodicidade):


    sql = '''
        SELECT prov_data,
            prov_moeda,
            prov_valor,
            dolar
        FROM ProventosAcao as t1
        INNER JOIN HistoricoDolar as t2
        ON t1.prov_data = t2.data
    '''
    df = pd.read_sql(sql, conn)

    df['valor'] = df.prov_valor
    df.loc[df.prov_moeda == 'EUA', 'valor'] = df.prov_valor * df.dolar

    df.prov_data = pd.to_datetime(df.prov_data)

    df['data'] = df['prov_data']
    if periodicidade == 'mes':
        df['data'] = df['prov_data'] + MonthEnd(0)
    
    dividendos = df.groupby(['data']).sum().reset_index()
    dividendos = dividendos[['data', 'valor']]
    dividendos['classe'] = 'Dividendos'
    dividendos = dividendos.round(2)
    
    return dividendos


def aportes_mensais(conn, data_base):


    # Aportes mensais

    aportes_acao = aportes_acao_por_periodo(conn, periodicidade='mes')
    aportes_td = aportes_td_por_periodo(conn, periodicidade='mes')
    aportes_fundos = aportes_fundos_por_periodo(conn, periodicidade='mes')
    aportes_rf = aportes_rf_por_periodo(conn, periodicidade='mes')

    # Dividendos
    dividendos = dividendos_por_periodo(conn, periodicidade='mes')

    aportes = pd.concat([aportes_acao,
                         aportes_td,
                         aportes_fundos,
                         aportes_rf,
                         dividendos], ignore_index=True)

    aportes.loc[aportes.data > data_base, 'data'] = data_base

    aportes.loc[aportes.classe == 'Dividendos', 'valor'] = aportes.valor * -1

    aportes = aportes.sort_values(['classe', 'data']).round(2)

    return aportes


def preco_medio_acao(conn):


    # Obtém relação de tickers da posição atual
    sql = '''
        SELECT oper_ticker, sum(oper_qtd) as acum
        FROM OperacaoAcao
        GROUP BY oper_ticker
        HAVING acum > 0
        '''
    df = pd.read_sql(sql, conn)
    tickers = df.oper_ticker.tolist()

    # Obtém operações dos tickers com posição
    sql = f'''
        SELECT oper_ticker, oper_data, oper_cv, sum(oper_qtd) as qtd, sum(oper_valor) as valor
        FROM OperacaoAcao
        WHERE oper_ticker IN ({tickers})
        GROUP BY oper_ticker, oper_data, oper_cv
        ORDER BY oper_ticker, oper_data, oper_cv
    '''
    sql = sql.format(tickers).replace('[', '').replace(']', '')
    oper = pd.read_sql(sql, conn)
    oper.loc[oper.qtd < 0, 'valor'] = oper.valor * -1
    oper.columns = ['ticker', 'data', 'cv', 'qtd', 'valor']

    # Obtém dividendos dos tickers com posição
    sql = f'''
        SELECT prov_ticker, prov_data, sum(prov_valor) as valor
        FROM ProventosAcao
        WHERE prov_ticker IN ({tickers})
        GROUP BY prov_ticker, prov_data
        ORDER BY prov_ticker, prov_data
    '''
    sql = sql.format(tickers).replace('[', '').replace(']', '')
    divid = pd.read_sql(sql, conn)
    divid.valor = divid.valor * -1
    divid.columns = ['ticker', 'data', 'valor']

    # Para cada ticker, calcula o preço médio

    df_preco_medio = []

    for ticker in tickers:

        df_oper = oper[oper.ticker == ticker].copy()
        df_divid = divid[divid.ticker == ticker].copy()

        df_oper['qtd_acum'] = df_oper.qtd.cumsum()

        if df_oper.loc[df_oper.qtd_acum == 0, 'data'].shape[0] > 0:
            maior_data_referencia = datetime.strptime(df_oper.loc[df_oper.qtd_acum == 0, 'data'].max(), '%Y-%m-%d %H:%M:%S')
        else:
            maior_data_referencia = datetime.strptime(df_oper.data.min(), '%Y-%m-%d %H:%M:%S') - timedelta(days=1)

        df_oper = df_oper[~(pd.to_datetime(df_oper.data) <= maior_data_referencia)]

        df_divid = df_divid[~(pd.to_datetime(df_divid.data) <= maior_data_referencia)]
        
        df = pd.concat([df_oper, df_divid])
        df = df.fillna(0)

        preco_medio = round(df.valor.sum() / df.qtd.sum(), 2)

        df_preco_medio.append([ticker, preco_medio])

    # Salva tabela em banco de dados

    preco_medio = pd.DataFrame(df_preco_medio, columns=['ticker', 'preco_medio'])

    preco_medio.to_sql('PrecoMedioAcao', conn, if_exists='replace', index=False)

    return preco_medio


def preco_medio_td(conn):


    # Obtém relação de títulos da posição atual

    sql = '''
        SELECT oper_nome, round(sum(oper_qtd), 2) as acum
        FROM OperacaoTD
        GROUP BY oper_nome
        HAVING acum > 0
        ORDER BY oper_nome
    '''
    df = pd.read_sql(sql, conn)
    titulos = df.oper_nome.tolist()

    # Obtém operações dos títulos com posição

    sql = f'''
        SELECT oper_nome, oper_data, sum(oper_qtd) as qtd, sum(oper_valor) as valor
        FROM OperacaoTD
        WHERE oper_nome IN ({titulos})
        GROUP BY oper_nome, oper_data
        ORDER BY oper_nome, oper_data
    '''
    sql = sql.format(titulos).replace('[', '').replace(']', '')
    oper = pd.read_sql(sql, conn)
    oper.columns = ['nome', 'data', 'qtd', 'valor']

    # Para cada título, calcula o preço médio

    df_preco_medio = []

    for titulo in titulos:

        df = oper[oper.nome == titulo].copy()

        df['qtd_acum'] = df.qtd.cumsum()

        if df.loc[df.qtd_acum == 0, 'data'].shape[0] > 0:
            maior_data_referencia = datetime.strptime(df.loc[df.qtd_acum == 0, 'data'].max(), '%Y-%m-%d %H:%M:%S')
        else:
            maior_data_referencia = datetime.strptime(df.data.min(), '%Y-%m-%d %H:%M:%S') - timedelta(days=1)

        df = df[~(pd.to_datetime(df.data) <= maior_data_referencia)]

        preco_medio = round(df.valor.sum() / df.qtd.sum(), 2)

        df_preco_medio.append([titulo, preco_medio])

    # Salva tabela em banco de dados

    preco_medio = pd.DataFrame(df_preco_medio, columns=['nome', 'preco_medio'])

    preco_medio.to_sql('PrecoMedioTD', conn, if_exists='replace', index=False)

    return preco_medio


def preco_medio_fundos(conn):


    # Obtém relação de fundos da posição atual

    sql = '''
        SELECT oper_nome, round(sum(oper_qtd), 2) as qtd
        FROM OperacaoFundos
        GROUP BY oper_nome
        HAVING qtd > 0
        ORDER BY oper_nome
    '''
    df = pd.read_sql(sql, conn)
    fundos = df.oper_nome.tolist()

    # Obtém operações dos fundos com posição

    sql = f'''
        SELECT oper_nome, oper_data, sum(oper_qtd) as qtd, sum(oper_valor) as valor
        FROM OperacaoFundos
        WHERE oper_nome IN ({fundos})
        GROUP BY oper_nome, oper_data
        ORDER BY oper_nome, oper_data
    '''
    sql = sql.format(fundos).replace('[', '').replace(']', '')
    oper = pd.read_sql(sql, conn)
    oper.columns = ['nome', 'data', 'qtd', 'valor']

    # Para cada fundo, calcula o preço médio

    df_preco_medio = []

    for fundo in fundos:

        df = oper[oper.nome == fundo].copy()

        df['qtd_acum'] = df.qtd.cumsum()

        if df.loc[df.qtd_acum == 0, 'data'].shape[0] > 0:
            maior_data_referencia = datetime.strptime(df.loc[df.qtd_acum == 0, 'data'].max(), '%Y-%m-%d %H:%M:%S')
        else:
            maior_data_referencia = datetime.strptime(df.data.min(), '%Y-%m-%d %H:%M:%S') - timedelta(days=1)

        df = df[~(pd.to_datetime(df.data) <= maior_data_referencia)]

        preco_medio = round(df.valor.sum() / df.qtd.sum(), 6)

        df_preco_medio.append([fundo, preco_medio])

    # Salva tabela em banco de dados

    preco_medio = pd.DataFrame(df_preco_medio, columns=['nome', 'preco_medio'])

    preco_medio.to_sql('PrecoMedioFundos', conn, if_exists='replace', index=False)

    return preco_medio


def evolucao_diaria(conn):


    # Saldos diários
    sql = '''
        SELECT data, classe, sum(saldo) as saldo
        FROM PosicaoDiaria
        GROUP BY data, classe
    '''
    posicao_diaria = pd.read_sql(sql, conn)
    posicao_diaria.data = pd.to_datetime(posicao_diaria.data)

    posicao_diaria = posicao_diaria.pivot_table(values=['saldo'], index='data', columns='classe', fill_value=0).reset_index()
    posicao_diaria.columns = ['data', 'saldo_acao', 'saldo_fundos', 'saldo_rf', 'saldo_td']
    posicao_diaria['saldo_total'] = posicao_diaria.saldo_acao + posicao_diaria.saldo_td + posicao_diaria.saldo_fundos + posicao_diaria.saldo_rf

    # Aportes
    aporte_acao = aportes_acao_por_periodo(conn, periodicidade='dia')
    aporte_td = aportes_td_por_periodo(conn, periodicidade='dia')
    aporte_fundos = aportes_fundos_por_periodo(conn, periodicidade='dia')
    aporte_rf = aportes_rf_por_periodo(conn, periodicidade='dia')

    aportes = pd.concat([aporte_acao, aporte_td, aporte_fundos, aporte_rf])

    aportes = aportes.pivot_table(values=['valor'], index='data', columns='classe', fill_value=0).reset_index()
    aportes.columns = ['data', 'aporte_acao', 'aporte_fundos', 'aporte_rf', 'aporte_td']
    aportes['aporte_total'] = aportes.aporte_acao + aportes.aporte_td + aportes.aporte_fundos + aportes.aporte_rf

    # Dividendos
    dividendos = dividendos_por_periodo(conn, periodicidade='dia')
    dividendos = dividendos[['data', 'valor']]
    dividendos.columns = ['data', 'dividendos']

    df = posicao_diaria.merge(aportes, on='data', how='left')
    df = df.merge(dividendos, on='data', how='left')

    df = round(df.fillna(0), 2)

    return df


def rentabilidade_por_ativo(conn, data_base):


    data_base = datetime.strptime(data_base, '%Y-%m-%d')


    # Ações BRL
    sql = '''
        SELECT oper_tipo as tipo, oper_ticker as ticker, oper_cv, sum(oper_qtd) as qtd, sum(oper_valor) as valor
        FROM OperacaoAcao
        WHERE oper_data <= '{}' AND oper_moeda <> 'EUA'
        GROUP BY tipo, ticker, oper_cv
        '''
    sql = sql.format(data_base)

    df_acao_brl = pd.read_sql(sql, conn)

    # Ações EUA
    sql = '''
        SELECT oper_data, oper_tipo as tipo, oper_ticker as ticker, oper_cv, oper_qtd as qtd, oper_valor, dolar
        FROM OperacaoAcao, HistoricoDolar
        WHERE oper_data <= '{}' AND oper_moeda = 'EUA' AND oper_data = data
        '''
    sql = sql.format(data_base)

    df_acao_eua = pd.read_sql(sql, conn)

    df_acao_eua['valor'] = df_acao_eua.oper_valor * df_acao_eua.dolar

    df_acao_eua = df_acao_eua.groupby(['tipo', 'ticker', 'oper_cv']).sum().reset_index()

    del df_acao_eua['dolar']
    del df_acao_eua['oper_valor']


    df_acao = pd.concat([df_acao_brl, df_acao_eua]).reset_index()

    df_acao.loc[df_acao.qtd > 0, 'valor'] = df_acao.valor * -1

    df_acao = df_acao.groupby(['tipo', 'ticker']).sum().reset_index()

    df_acao['classe'] = 'Ações'


    # Dividendos BRL
    sql = '''
        SELECT prov_ticker as ticker, sum(prov_valor) as dividendo
        FROM ProventosAcao
        WHERE prov_data <= '{}' AND prov_moeda <> 'EUA'
        GROUP BY ticker
        '''
    sql = sql.format(data_base)

    divid_brl = pd.read_sql(sql, conn)

    # Dividendos EUA
    sql = '''
        SELECT prov_data, prov_ticker as ticker, prov_valor, dolar
        FROM ProventosAcao, HistoricoDolar
        WHERE prov_data <= '{}' AND prov_moeda = 'EUA' AND prov_data = data
        '''
    sql = sql.format(data_base)

    divid_eua = pd.read_sql(sql, conn)

    divid_eua['dividendo'] = divid_eua.prov_valor * divid_eua.dolar

    divid_eua = divid_eua.groupby(['ticker']).sum().reset_index()

    del divid_eua['dolar']
    del divid_eua['prov_valor']

    divid = pd.concat([divid_brl, divid_eua]).reset_index()

    df_acao = df_acao.merge(divid, on='ticker', how='left')


    # Tesouro Direto
    sql = '''
        SELECT oper_nome as ticker, oper_cv, sum(oper_qtd) as qtd, sum(oper_valor) as valor
        FROM OperacaoTD
        WHERE oper_data <= '{}'
        GROUP BY ticker, oper_cv
        '''
    sql = sql.format(data_base)

    df_td = pd.read_sql(sql, conn)

    df_td.loc[df_td.qtd > 0, 'valor'] = df_td.valor * -1

    df_td = df_td.groupby(['ticker']).sum().reset_index()

    df_td['tipo'] = ''
    df_td['classe'] = 'Tesouro Direto'


    # Fundos
    sql = '''
        SELECT oper_nome as ticker, oper_cv, sum(oper_qtd) as qtd, sum(oper_valor) as valor
        FROM OperacaoFundos
        WHERE oper_data <= '{}'
        GROUP BY ticker, oper_cv
        '''
    sql = sql.format(data_base)

    df_fundos = pd.read_sql(sql, conn)

    df_fundos.loc[df_fundos.qtd > 0, 'valor'] = df_fundos.valor * -1

    df_fundos = df_fundos.groupby(['ticker']).sum().reset_index()

    df_fundos['tipo'] = ''
    df_fundos['classe'] = 'Fundos'

    # Concatena bases

    df = pd.concat([df_acao, df_td, df_fundos])

    # Obtém posição na data base
    sql = '''
        SELECT ticker, sum(saldo) as saldo
        FROM PosicaoDiaria
        WHERE data = '{}'
        GROUP BY ticker
        '''
    sql = sql.format(data_base)
    posicao_diaria = pd.read_sql(sql, conn)

    df = df.merge(posicao_diaria, on='ticker', how='left').fillna(0)

    df['rentab'] = df.valor + df.dividendo + df.saldo

    df = df[['classe', 'tipo', 'ticker', 'dividendo', 'saldo', 'rentab']]

    df = df.sort_values(['classe', 'tipo', 'ticker'])

    return df


def imposto_renda_acao(conn, data_base):


    # Obtém tabela de conversão Dolar Oficial
    sql = '''
        SELECT *
        FROM DolarOficial
        '''
    sql = sql.format(data_base)
    dolar_oficial = pd.read_sql(sql, conn)

    # Obtém relação de tickers na data base
    sql = f'''
        SELECT oper_tipo, oper_ticker, sum(oper_qtd) as acum
        FROM OperacaoAcao
        WHERE strftime('%Y-%m-%d', oper_data) <= '{data_base}'
        GROUP BY oper_tipo, oper_ticker
        HAVING acum > 0
        '''
    sql = sql.format(data_base)
    df = pd.read_sql(sql, conn)
    tickers = df.oper_ticker.tolist()

    # Obtém operações dos tickers com posição na data base
    sql = f'''
        SELECT oper_tipo, oper_ticker, oper_data, oper_cv, oper_moeda, sum(oper_qtd) as qtd, sum(oper_valor) as valor
        FROM OperacaoAcao
        WHERE strftime('%Y-%m-%d', oper_data) <= '{data_base}' AND oper_ticker IN ({tickers})
        GROUP BY oper_tipo, oper_ticker, oper_data, oper_cv, oper_moeda
        ORDER BY oper_tipo, oper_ticker, oper_data, oper_cv, oper_moeda
    '''
    sql = sql.format(data_base, tickers).replace('[', '').replace(']', '')
    oper = pd.read_sql(sql, conn)
    oper.loc[oper.qtd < 0, 'valor'] = oper.valor * -1
    oper.columns = ['tipo', 'ticker', 'data', 'cv', 'moeda', 'qtd', 'valor']

    oper = oper.merge(dolar_oficial, how='left', on='data')
    oper.loc[oper.moeda == 'BRL', 'venda'] = 1
    oper['valor_real'] = round(oper.valor * oper.venda, 2)
    del oper['compra']
    oper = oper.rename(columns={'venda': 'dolar'})

    # Para cada ticker, calcula o preço médio

    operacoes_ir = pd.DataFrame()

    preco_medio = []

    for ticker in tickers:

        df = oper[oper.ticker == ticker].copy()

        df['qtd_acum'] = df.qtd.cumsum()

        if df.loc[df.qtd_acum == 0, 'data'].shape[0] > 0:
            maior_data_referencia = datetime.strptime(df.loc[df.qtd_acum == 0, 'data'].max(), '%Y-%m-%d %H:%M:%S')
        else:
            maior_data_referencia = datetime.strptime(df.data.min(), '%Y-%m-%d %H:%M:%S') - timedelta(days=1)

        df = df[~(pd.to_datetime(df.data) <= maior_data_referencia)]

        operacoes_ir = pd.concat([operacoes_ir, df])

        custo_medio_reais = round(df.valor_real.sum() / df.qtd.sum(), 2)

        qtd = df.qtd.sum()

        tipo = df.iloc[0]['tipo']
        moeda = df.iloc[0]['moeda']

        preco_medio.append([tipo, ticker, moeda, qtd, custo_medio_reais])

    custo_medio_ir = pd.DataFrame(preco_medio, columns=['tipo', 'ticker', 'moeda', 'qtd', 'custo_medio_reais'])
    custo_medio_ir['custo_total_ir'] = custo_medio_ir.qtd * custo_medio_ir.custo_medio_reais

    # Salva tabelas em banco de dados

    custo_medio_ir.to_sql('ImpostoRendaCustoMedioAcao', conn, if_exists='replace', index=False)

    operacoes_ir.to_sql('ImpostoRendaOperacoesAcao', conn, if_exists='replace', index=False)

    return custo_medio_ir, operacoes_ir


def imposto_renda_td(conn, data_base):


    # Obtém relação de títulos na data base
    sql = f'''
        SELECT oper_nome, round(sum(oper_qtd), 2) as acum
        FROM OperacaoTD
        WHERE strftime('%Y-%m-%d', oper_data) <= '{data_base}'
        GROUP BY oper_nome
        HAVING acum > 0
        ORDER BY oper_nome
    '''
    sql = sql.format(data_base)
    df = pd.read_sql(sql, conn)
    titulos = df.oper_nome.tolist()

    # Obtém operações dos títulos com posição na data base
    sql = f'''
        SELECT oper_nome, oper_data, oper_cv, sum(oper_qtd) as qtd, sum(oper_valor) as valor
        FROM OperacaoTD
        WHERE strftime('%Y-%m-%d', oper_data) <= '{data_base}'
          AND oper_nome IN ({titulos})
          AND oper_cv <> 'Come-Cotas'
        GROUP BY oper_nome, oper_data, oper_cv
        ORDER BY oper_nome, oper_data, oper_cv
    '''
    sql = sql.format(data_base, titulos).replace('[', '').replace(']', '')
    oper = pd.read_sql(sql, conn)
    oper.loc[oper.qtd < 0, 'valor'] = oper.valor * -1
    oper.columns = ['nome', 'data', 'cv', 'qtd', 'valor']

    # Para cada título, calcula o preço médio

    operacoes_ir = pd.DataFrame()

    preco_medio = []

    for titulo in titulos:

        df = oper[oper.nome == titulo].copy()

        df['qtd_acum'] = df.qtd.cumsum()

        if df.loc[df.qtd_acum == 0, 'data'].shape[0] > 0:
            maior_data_referencia = datetime.strptime(df.loc[df.qtd_acum == 0, 'data'].max(), '%Y-%m-%d %H:%M:%S')
        else:
            maior_data_referencia = datetime.strptime(df.data.min(), '%Y-%m-%d %H:%M:%S') - timedelta(days=1)

        df = df[~(pd.to_datetime(df.data) <= maior_data_referencia)]

        operacoes_ir = pd.concat([operacoes_ir, df])

        custo_medio_reais = round(df.valor.sum() / df.qtd.sum(), 2)

        qtd = df.qtd.sum()

        preco_medio.append([titulo, qtd, custo_medio_reais])

    custo_medio_ir = pd.DataFrame(preco_medio, columns=['nome', 'qtd', 'custo_medio_reais'])
    custo_medio_ir['custo_total_ir'] = custo_medio_ir.qtd * custo_medio_ir.custo_medio_reais

    # Salva tabelas em banco de dados

    custo_medio_ir.to_sql('ImpostoRendaCustoMedioTD', conn, if_exists='replace', index=False)

    operacoes_ir.to_sql('ImpostoRendaOperacoesTD', conn, if_exists='replace', index=False)

    return custo_medio_ir, operacoes_ir


def imposto_renda_fundos(conn, data_base):


    # Obtém relação de títulos na data base
    sql = f'''
        SELECT oper_nome, round(sum(oper_qtd), 2) as acum
        FROM OperacaoFundos
        WHERE strftime('%Y-%m-%d', oper_data) <= '{data_base}'
        GROUP BY oper_nome
        HAVING acum > 0
        ORDER BY oper_nome
    '''
    sql = sql.format(data_base)
    df = pd.read_sql(sql, conn)
    titulos = df.oper_nome.tolist()

    # Obtém operações dos títulos com posição na data base
    sql = f'''
        SELECT oper_nome, oper_data, oper_cv, sum(oper_qtd) as qtd, sum(oper_valor) as valor
        FROM OperacaoFundos
        WHERE strftime('%Y-%m-%d', oper_data) <= '{data_base}'
          AND oper_nome IN ({titulos})
          AND oper_cv <> 'Come-Cotas'
        GROUP BY oper_nome, oper_data, oper_cv
        ORDER BY oper_nome, oper_data, oper_cv
    '''
    sql = sql.format(data_base, titulos).replace('[', '').replace(']', '')
    oper = pd.read_sql(sql, conn)
    oper.loc[oper.qtd < 0, 'valor'] = oper.valor * -1
    oper.columns = ['nome', 'data', 'cv', 'qtd', 'valor']

    # Para cada título, calcula o preço médio

    operacoes_ir = pd.DataFrame()

    preco_medio = []

    for titulo in titulos:

        df = oper[oper.nome == titulo].copy()

        df['qtd_acum'] = df.qtd.cumsum()

        if df.loc[df.qtd_acum == 0, 'data'].shape[0] > 0:
            maior_data_referencia = datetime.strptime(df.loc[df.qtd_acum == 0, 'data'].max(), '%Y-%m-%d %H:%M:%S')
        else:
            maior_data_referencia = datetime.strptime(df.data.min(), '%Y-%m-%d %H:%M:%S') - timedelta(days=1)

        df = df[~(pd.to_datetime(df.data) <= maior_data_referencia)]

        operacoes_ir = pd.concat([operacoes_ir, df])

        custo_medio_reais = round(df.valor.sum() / df.qtd.sum(), 8)

        qtd = df.qtd.sum()

        preco_medio.append([titulo, qtd, custo_medio_reais])

    custo_medio_ir = pd.DataFrame(preco_medio, columns=['nome', 'qtd', 'custo_medio_reais'])
    custo_medio_ir['custo_total_ir'] = round(custo_medio_ir.qtd * custo_medio_ir.custo_medio_reais, 2)

    # Salva tabelas em banco de dados

    custo_medio_ir.to_sql('ImpostoRendaCustoMedioFundos', conn, if_exists='replace', index=False)

    operacoes_ir.to_sql('ImpostoRendaOperacoesFundos', conn, if_exists='replace', index=False)

    return custo_medio_ir, operacoes_ir


def imposto_renda_dividendos(conn, data_base):


    # Obtém dividendos recebidos no ano

    primeiro_dia_ano = datetime(int(data_base[:4]), 1, 1).strftime('%Y-%m-%d')

    # Obtém CNPJ e nome das Empresas
    sql = '''
        SELECT *
        FROM Empresas
        '''
    sql = sql.format(data_base)
    empresas = pd.read_sql(sql, conn)

    # Dividendos BRL
    sql = f'''
        SELECT prov_data, prov_tipo, prov_ticker, prov_forma, prov_moeda, prov_bruto, prov_ir, prov_valor
        FROM ProventosAcao
        WHERE strftime('%Y-%m-%d', prov_data) >= '{primeiro_dia_ano}'
          AND strftime('%Y-%m-%d', prov_data) <= '{data_base}'
          AND prov_moeda = 'BRL'
        ORDER BY prov_tipo, prov_ticker, prov_forma, prov_moeda, prov_data
    '''
    sql = sql.format(primeiro_dia_ano, data_base)
    oper_brl = pd.read_sql(sql, conn)

    oper_brl['dolar'] = 1.0
    oper_brl['ir_real'] = round(oper_brl.prov_ir * oper_brl.dolar, 2)
    oper_brl['dividendo'] = round(oper_brl.prov_valor * oper_brl.dolar, 2)

    divid_brl = oper_brl.groupby(['prov_tipo', 'prov_ticker', 'prov_forma', 'prov_moeda']).sum().reset_index()

    # Dividendos EUA
    sql = f'''
        SELECT prov_data, prov_tipo, prov_ticker, prov_forma, prov_moeda, prov_bruto, prov_ir, prov_valor, compra as dolar
        FROM ProventosAcao, DolarOficial
        WHERE strftime('%Y-%m-%d', prov_data) >= '{primeiro_dia_ano}'
          AND strftime('%Y-%m-%d', prov_data) <= '{data_base}'
          AND prov_moeda = 'EUA'
          AND prov_data = data
        ORDER BY prov_tipo, prov_ticker, prov_forma, prov_moeda, prov_data
        '''
    sql = sql.format(primeiro_dia_ano, data_base)
    oper_eua = pd.read_sql(sql, conn)

    oper_eua['ir_real'] = round(oper_eua.prov_ir * oper_eua.dolar, 2)
    oper_eua['dividendo'] = round(oper_eua.prov_valor * oper_eua.dolar, 2)

    divid_eua = oper_eua.groupby(['prov_tipo', 'prov_ticker', 'prov_forma', 'prov_moeda']).sum().reset_index()

    operacoes = pd.concat([oper_brl, oper_eua])

    divid = pd.concat([divid_brl, divid_eua])

    divid = divid.merge(empresas, left_on='prov_ticker', right_on='ticker', how='left')

    divid = divid.fillna('')

    del divid['dolar']
    del divid['tipo']
    del divid['ticker']

    divid = divid[['prov_tipo', 'prov_ticker', 'nome', 'cnpj', 'prov_forma', 'prov_moeda', 'prov_bruto', 'prov_ir', 'prov_valor', 'ir_real', 'dividendo']]


    # Salva tabelas em banco de dados

    divid.to_sql('ImpostoRendaDividendos', conn, if_exists='replace', index=False)

    operacoes.to_sql('ImpostoRendaOperacoesDividendos', conn, if_exists='replace', index=False)

    operacoes.prov_data = pd.to_datetime(operacoes.prov_data)

    ir_mensal = operacoes.groupby(pd.Grouper(key='prov_data', freq='M')).sum().reset_index()[['prov_data', 'ir_real']]

    return divid, operacoes, ir_mensal


def cotacao_acoes_interesse(conn):
    '''
    Obtém lista e cotação do yahoo das ações de interesse
    '''

    # Data de início: 5 anos atrás (1825 dias)
    data_ini = (date.today() - timedelta(days=1825)).strftime('%Y-%m-%d')

    data_hoje = date.today()

    # Datas para Yahoo Finance com todas as datas do histórico
    datas_yahoo = pd.DataFrame(pd.date_range(start=data_ini, end=data_hoje).tolist())
    datas_yahoo.columns = ['data']
    datas_yahoo.data = pd.to_datetime(datas_yahoo.data)

    # Obtém relação de tickers para buscar histórico
    sql = f'''
        SELECT *
        FROM AcoesInteresse
        WHERE favorito <> '.'
        ORDER BY tipo, segmento, ticker
        '''

    acoes_interesse = pd.read_sql(sql, conn)

    acoes_interesse['ticker_yahoo'] = acoes_interesse.ticker
    acoes_interesse.loc[~acoes_interesse.tipo.isin(['ETF', 'REIT', 'Stock']), 'ticker_yahoo'] = acoes_interesse.ticker + '.SA'

    lista_tickers_yahoo = acoes_interesse.ticker_yahoo.tolist()

    # Obtém cotação histórica para todas as ações
    data = yf.download(lista_tickers_yahoo, start=data_ini, progress=True)


    # Processa histórico
    cotacao = pd.DataFrame()

    for ticker in acoes_interesse.ticker_yahoo:
        temp = datas_yahoo.copy()
        temp['ticker_yahoo'] = ticker
        yahoo_cotacao = data.loc[:, 'Close'][ticker].reset_index()
        temp = temp.merge(yahoo_cotacao, how='left', left_on='data', right_on='Date').fillna(axis=0, method='ffill')
        temp.columns = ['data', 'ticker_yahoo', 'Date', 'close']
        yahoo_volume = data.loc[:, 'Volume'][ticker].reset_index()
        temp = temp.merge(yahoo_volume, how='left', left_on='data', right_on='Date').fillna(axis=0, method='ffill')
        temp.columns = ['data', 'ticker_yahoo', 'Date_x', 'close', 'Date_y', 'volume']
        temp = temp[['data', 'ticker_yahoo', 'close', 'volume']]
        cotacao = pd.concat([cotacao, temp])
    
    cotacao['ticker'] = cotacao.ticker_yahoo.str.replace('.SA', '', regex=False)
    cotacao = cotacao[['ticker', 'data', 'close', 'volume', 'ticker_yahoo']]
    cotacao = cotacao.dropna()

    cotacao.close = round(cotacao.close, 2)
    cotacao.volume = (cotacao.volume * cotacao.close / 1_000_000).astype(int)

    # Salva tabela no banco de dados
    cotacao.to_sql('CotacaoAcaoInteresse', con=conn, if_exists='replace', index=False)

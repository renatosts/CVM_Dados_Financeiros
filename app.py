from bs4 import BeautifulSoup
from datetime import date, datetime
from zipfile import ZipFile
import csv
import os
import pandas as pd
import re
import requests as req


def cria_cadastro():

    # Determina Cadastro
    cadastro = processa_base_cvm('FCA', 'geral')
    cadastro  = cadastro.groupby('CNPJ_Companhia')[['Codigo_CVM', 'Nome_Empresarial', 'Setor_Atividade', 'Pagina_Web']].last().reset_index()
    cadastro.columns = ['cnpj', 'cod_cvm', 'nome', 'segmento', 'site']
    cadastro.segmento = cadastro.segmento.str.replace('Emp. Adm. Part. - ', '', regex=False)
    cadastro.segmento = cadastro.segmento.str.replace('Emp. Adm. Part.-', '', regex=False)
    cadastro.nome = cadastro.nome.str.upper()

    # Determina tickers de negociação
    tickers = processa_base_cvm('FCA', 'valor_mobiliario')
    tickers = tickers[['CNPJ_Companhia', 'Codigo_Negociacao']].drop_duplicates().dropna()
    tickers = tickers[~tickers.Codigo_Negociacao.str.contains('*', regex=False)]
    tickers = tickers.groupby(['CNPJ_Companhia']).agg({'Codigo_Negociacao': ','.join}).reset_index()
    tickers.columns = ['cnpj', 'ticker']

    cadastro = cadastro.merge(tickers, on='cnpj')


    # Quantidade de ações
    distribuicao_capital = processa_base_cvm('FRE', 'distribuicao_capital')
    distribuicao_capital = distribuicao_capital[distribuicao_capital['Quantidade_Total_Acoes_Circulacao'] > 0]
    distribuicao_capital = distribuicao_capital[distribuicao_capital['Percentual_Total_Acoes_Circulacao'] > 0]
    distribuicao_capital = distribuicao_capital.groupby('CNPJ_Companhia')[['Data_Referencia', 'Versao', 'Quantidade_Total_Acoes_Circulacao', 'Percentual_Total_Acoes_Circulacao']].last().reset_index()
    distribuicao_capital['acoes'] = (distribuicao_capital.Quantidade_Total_Acoes_Circulacao / (distribuicao_capital.Percentual_Total_Acoes_Circulacao / 100)).astype('int64')
    distribuicao_capital.columns = ['cnpj', 'dt_ref', 'versao', 'acoes_circul', 'free_float', 'acoes']
    distribuicao_capital = distribuicao_capital[['cnpj', 'acoes', 'free_float']]

    cadastro = cadastro.merge(distribuicao_capital, on='cnpj', how='left')
    cadastro['acoes'] = cadastro['acoes'].fillna(0).astype('int64')


    # Governança
    governanca = processa_base_cvm('FCA', 'valor_mobiliario')
    governanca = governanca.groupby('CNPJ_Companhia')[['Data_Referencia', 'Versao', 'Segmento']].last().reset_index()
    governanca = governanca[['CNPJ_Companhia', 'Segmento']]
    governanca.columns = ['cnpj', 'governanca']

    cadastro = cadastro.merge(governanca, on='cnpj', how='left')

    return cadastro


def download_arquivos_CVM(tipo):

    # Acessa site CVM para verificar arquivos anuais para download

    URL_CVM = f'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/{tipo}/DADOS/'

    # Verifica data do último download

    with open('controle_download.csv', 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            data_ultimo_download = row[0]

    # Acessa site CVM para verificar arquivos mensais para download

    try:

        resp = req.get(URL_CVM)

    except Exception as e:

        print(e)


    bs = BeautifulSoup(resp.text, 'html.parser')

    pre = bs.find('pre')

    nome = []
    for m in re.finditer(rf"{tipo.lower()}_cia_aberta_\w*.zip", pre.text):  
        nome.append(m.group(0).rstrip('.zip'))
    
    last_mod = []
    for m in re.finditer(r"\d{2}-\w{3}-\d{4} \w{2}:\w{2}", pre.text):  
        last_mod.append(m.group(0))

    # df contém a lista dos arquivos a serem baixados
    df = pd.DataFrame()
    df['nome'] = nome
    df['last_mod'] = last_mod
    df['ano'] = df.nome.str[15:19]
    df['last_mod'] = pd.to_datetime(df.last_mod)

    df = df[df['last_mod'] > data_ultimo_download]

    for arq in df['nome']:
        print('Download do arquivo:', arq)
        download_url(URL_CVM + arq + '.zip', dest_folder=rf'Base_CVM\{tipo}')


def download_url(url: str, dest_folder: str):


    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)  # create folder if it does not exist

    filename = url.split('/')[-1].replace(' ', '_')  # be careful with file names
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
        print('Download failed: status code {}\n{}'.format(r.status_code, r.text))


def processa_base_cvm(tipo, arquivo):

    primeiro_ano = datetime.today().year - 9

    df = pd.DataFrame()

    pasta = f'Base_CVM\\{tipo}\\'

    for filezip in os.listdir(pasta):

        ano = int(re.search('[0-9]+', filezip).group(0))

        if ano >= primeiro_ano:

            with ZipFile(pasta + filezip) as zip:

                nome_arq = f'{tipo.lower()}_cia_aberta_{arquivo}_{ano}.csv'
                with zip.open(nome_arq) as f:
                    temp = pd.read_csv(f, encoding='Latin-1', delimiter=';')
                    df = pd.concat([df, temp])

    return df


def processa_dados_financeiros(tipo):

    balanco_ativo = processa_base_cvm(tipo, 'BPA_con')
    balanco_passivo = processa_base_cvm(tipo, 'BPP_con')
    dre = processa_base_cvm(tipo, 'DRE_con')
    dra = processa_base_cvm(tipo, 'DRA_con')
    dfc_md = processa_base_cvm(tipo, 'DFC_MD_con')
    dfc_mi = processa_base_cvm(tipo, 'DFC_MI_con')

    df = pd.concat([balanco_ativo, balanco_passivo, dre, dra, dfc_md, dfc_mi])

    df = df[df.ORDEM_EXERC == 'ÚLTIMO']       
    df.VL_CONTA = df.VL_CONTA.astype(float)
    df['ano'] = df['DT_REFER'].str[:4].astype(int)

    df['tipo'] = tipo

    df.columns = ['cnpj', 'dt_ref', 'versao', 'nome', 'cod_cvm', 'grupo_dfp', 'moeda', 'escala_moeda',
                  'ordem_exerc', 'dt_fim_exerc', 'cod_conta', 'desc_conta', 'valor', 'sit_conta_fixa',
                  'dt_ini_exerc', 'ano', 'form']

    df.dt_ref = pd.to_datetime(df.dt_ref)

    return df


# Início

base_download_cvm = ['DFP', 'ITR', 'FRE', 'FCA']

for tipo in base_download_cvm:
    print(tipo)
    download_arquivos_CVM(tipo)

# Atualiza data do último download

data_ultimo_download = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

with open('controle_download.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow([data_ultimo_download])

# Gera cadastro

cadastro = cria_cadastro()

# Dados Financeiros

# Processa DFP

dfp = processa_dados_financeiros('DFP')

ultimo_ano_dfp = dfp.groupby(['cod_cvm']).last().reset_index()[['cod_cvm', 'dt_ref', 'ano']]
ultimo_ano_dfp.columns = ['cod_cvm', 'ultimo_dfp_dt_ref', 'ultimo_dfp_ano']

ano_anterior = dfp.ano.max() - 1

empresas_ano_anterior = ultimo_ano_dfp[ultimo_ano_dfp.ultimo_dfp_ano >= ano_anterior]
dfp = dfp[dfp.cod_cvm.isin(empresas_ano_anterior.cod_cvm)]

# Processa ITR (deixa somente último ITR se houver)

itr = processa_dados_financeiros('ITR')

ultimo_itr = itr.groupby(['cod_cvm']).last().reset_index()[['cod_cvm', 'ano', 'dt_ref', 'versao']]
ultimo_itr = ultimo_itr.merge(ultimo_ano_dfp, on='cod_cvm', how='left')

ultimo_itr = ultimo_itr[ultimo_itr.dt_ref > ultimo_itr.ultimo_dfp_dt_ref]
ultimo_itr = ultimo_itr[((ultimo_itr.ultimo_dfp_dt_ref.isna()) & (ultimo_itr.ano > ano_anterior)) |
                        (ultimo_itr.ano > ano_anterior)]


itr['chave'] = itr.cod_cvm.astype(str) + itr.dt_ref.dt.strftime('%Y-%m-%d') + itr.versao.astype(str)
ultimo_itr['chave'] = ultimo_itr.cod_cvm.astype(str) + ultimo_itr.dt_ref.dt.strftime('%Y-%m-%d') + ultimo_itr.versao.astype(str)

itr = itr[itr.chave.isin(ultimo_itr.chave)]

del itr['chave']



# Junta DFP com último ITR
financ = pd.concat([dfp, itr])


# Seleciona saldos de interesse

contas_selec = ['1', '1.01.01', '1.01.02', '2.03', '3.01', '3.03',
                '3.05', '3.11', '2.01.04', '2.02.01']

# idx saldos
idx_saldos = financ.cod_conta.isin(contas_selec)

# idx deprec
idx_deprec = (financ.cod_conta.str.startswith('6.01')
             ) & (
              financ.desc_conta.str.lower().str.contains('deprec|amortiz', regex=True))


saldos = financ[idx_saldos]

deprec = financ[idx_deprec]
deprec = deprec.groupby(['form', 'cod_cvm', 'ano', 'dt_ref']).sum('valor').reset_index()
deprec['deprec_amortiz'] = deprec['valor']
del deprec['valor']

# Gera arquivo de saída

df = saldos.pivot_table(values='valor', index=['form', 'cod_cvm', 'ano', 'dt_ref'], columns='cod_conta', fill_value=0).reset_index()

df = df.merge(deprec, on=['form', 'cod_cvm', 'ano', 'dt_ref'], how='left')
df = df.fillna(0)

df['ativo'] = df['1']
df['caixa'] = df['1.01.01'] + df['1.01.02']
df['divida_curto_prazo'] = df['2.01.04']
df['divida_longo_prazo'] = df['2.02.01']
df['divida_total'] = df['divida_curto_prazo'] + df['divida_longo_prazo']
df['patr_liq'] = df['2.03']
df['receita_liq'] = df['3.01']
df['lucro_bruto'] = df['3.03']
df['lucro_liq'] = df['3.11']
df['EBIT'] = df['3.05']

df['endivid_taxa'] = round(df['divida_total'] / df['ativo'], 2)
df['margem_liq'] = round(df['lucro_liq'] / df['receita_liq'] * 100, 2)
df['EBITDA'] = round(df['EBIT'] + df['deprec_amortiz'], 2)
df['divida_liq'] = round((df['divida_total'] - df['caixa']) / df['EBITDA'], 2)

df = df[['form', 'cod_cvm', 'ano', 'dt_ref',
       'ativo', 'patr_liq', 'receita_liq', 'lucro_bruto', 'lucro_liq', 'EBIT',
       'divida_curto_prazo', 'divida_longo_prazo', 'caixa', 'divida_total',
       'endivid_taxa', 'margem_liq', 'deprec_amortiz', 'EBITDA', 'divida_liq']]


df = df.merge(cadastro, on='cod_cvm')

df = df[['segmento', 'nome', 'cod_cvm', 'site', 'ticker', 'ano', 'form', 'dt_ref',
       'ativo', 'patr_liq', 'receita_liq', 'lucro_bruto', 'lucro_liq', 'EBIT',
       'deprec_amortiz', 'EBITDA', 'margem_liq', 'divida_curto_prazo', 'divida_longo_prazo',
       'caixa', 'divida_liq', 'divida_total', 'acoes', 'free_float', 'governanca']]


df = df.sort_values(by=['segmento', 'nome', 'ano'])

df.to_csv('DadosFinanceiros.csv', sep=';', decimal=',', index=False, encoding='Latin1')

df[df.cod_cvm.isin([22470, 5410, 94, 701, 4693, 24252, 25780, 25470, 9512, 26069])][['cod_cvm', 'nome', 'ano', 'form', 'dt_ref']]
df[df.cod_cvm.isin([22470])][['cod_cvm', 'nome', 'ano', 'form', 'dt_ref']]

df[df.cod_cvm.isin([24228])].T



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

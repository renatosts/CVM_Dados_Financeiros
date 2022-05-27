from datetime import date, datetime, timedelta
from pandas.tseries.offsets import MonthEnd
import investimentos
import sqlite3
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def procedimento_inicial(conn):
    
    # Verifica se Posição Diária Ação está atualizada
    # Quando for a primeira execução do dia

    data_hoje = datetime.today().strftime('%Y-%m-%d')

    dt_ultima_cotacao_yfinance = investimentos.config_get_parametro(conn, config_tag='dt_ultima_cotacao_yfinance')

    if data_hoje > dt_ultima_cotacao_yfinance:

        with st.spinner(
            '''\nExecutando procedimentos iniciais...\n
                Na primeira execução do dia, é feita a validação da base de dados.\n
                Cálculo do histórico das cotações das ações Yahoo Finance\n
                Download da base Tesouro Direto\n
                Download da base de Fundos CVM'''):

            atualiza_base(conn)
        
            # Salva data da última cotação Yahoo Finance no Config
            investimentos.config_update_parametro(conn, config_tag='dt_ultima_cotacao_yfinance', config_valor=data_hoje)


def atualiza_base(conn):


    print('Processando base Ações') 
    investimentos.processa_operacao_acao(conn)

    print('Processando base Tesouro Direto')    
    investimentos.processa_operacao_td(conn)

    print('Processando base Fundos CVM')  
    investimentos.processa_operacao_fundos(conn)

    print('Processando base Renda Fixa')  
    investimentos.processa_operacao_rf(conn)

    print('Calculando Preço Médio Ação')
    investimentos.preco_medio_acao(conn)

    print('Calculando Preço Médio Tesouro Direto')
    investimentos.preco_medio_td(conn)

    print('Calculando Preço Médio Fundos')
    investimentos.preco_medio_fundos(conn)

    conn.execute('VACUUM')


def dataframe_por_tipo(df, tipo):

    df = df.fillna(0)

    df = df[df.tipo == tipo]

    df['lucro_aberto'] = df.acum * (df.cot_hoje - df.preco_medio)
    df['lucro_aberto_perc'] = df.cot_hoje / df.preco_medio - 1

    if tipo == 'Ação':
        df = df.sort_values(['saldo'], ascending=[False])
        
    df = df[['ticker', 'acum', 'preco_medio', 'lucro_aberto_perc', 'lucro_aberto', 'cot_ant', 'cot_hoje', 'var_perc', 'var_valor', 'var_saldo']]

    df['pos'] = df.acum * df.cot_hoje

    df.var_perc = df.var_perc / 100

    df.reset_index(inplace=True, drop=True) 

    df = df.set_index('ticker')

    df.columns = ['Qtd', 'PM', 'L/P %', 'L/P', 'Ant', 'Cot', 'Var', 'US$', 'L/P Dia', 'Posição']

    if tipo not in ['Stock', 'REIT', 'ETF']:
        del df['US$']

    if tipo == 'Stock':
        formato_qtd = '{:,.1f}'
    else:
        formato_qtd = '{:,.0f}'

    if tipo in ['Stock', 'REIT', 'ETF']:
        df = df.style.format(thousands='.',
                            decimal = ',',
                            formatter={'Cot': '{:,.2f}',
                                        'Ant': '{:,.2f}',
                                        'PM': '{:,.2f}',
                                        'L/P %': '{:,.2%}',
                                        'L/P': '{:,.2f}',
                                        'Qtd': formato_qtd,
                                        'US$': '{:,.2f}',
                                        'L/P Dia': '{:,.2f}',
                                        'Posição': '{:,.2f}',
                                        'Var': '{:,.2%}'}
                            ).applymap(define_color, subset=['L/P %', 'L/P', 'Var', 'US$', 'L/P Dia'])
    else:
        df = df.style.format(thousands='.',
                            decimal = ',',
                            formatter={'Cot': '{:,.2f}',
                                        'Ant': '{:,.2f}',
                                        'PM': '{:,.2f}',
                                        'Qtd': formato_qtd,
                                        'L/P %': '{:,.2%}',
                                        'L/P': '{:,.2f}',
                                        'L/P Dia': '{:,.2f}',
                                        'Posição': '{:,.2f}',
                                        'Var': '{:,.2%}'}
                            ).applymap(define_color, subset=['L/P %', 'L/P', 'L/P Dia', 'Var'])

    return df


def dataframe_resumo_total(df):


    df = df.fillna(0)

    df = df.groupby('data').sum().reset_index()

    df['var_perc'] = df.var_saldo / (df.saldo - df.var_saldo)

    df = df[['data', 'var_perc', 'var_saldo']]

    df.data = df.data.str[8:10] + '/' + df.data.str[5:7] + '/' + df.data.str[0:4]

    df.reset_index(inplace=True, drop=True) 

    df = df.set_index('data')

    df.columns = ['Var', 'L/P Dia']

    df = df.style.format(thousands='.',
                         decimal = ',',
                         formatter={'L/P Dia': '{:,.2f}',
                                    'Var': '{:,.2%}'}
                        ).applymap(define_color, subset=['L/P Dia', 'Var'])
    
    return df


def dataframe_resumo_por_tipo(df):

    df = df.fillna(0)

    df = df.groupby('tipo').sum().reset_index()

    df['var_perc'] = df.var_saldo / (df.saldo - df.var_saldo)

    df = df[['tipo', 'var_perc', 'var_saldo', 'var_valor', 'saldo']]

    df.loc[~df.tipo.isin(['Stock', 'REIT', 'ETF']), 'var_valor'] = 0

    df = df[['tipo', 'var_perc', 'var_valor', 'var_saldo']]

    df.reset_index(inplace=True, drop=True) 

    df = df.set_index('tipo')

    df.columns = ['Var', 'US$', 'L/P Dia']

    df = df.style.format(thousands='.',
                         decimal = ',',
                         formatter={'L/P Dia': '{:,.2f}',
                                    'US$': '{:,.2f}',
                                    'Var': '{:,.2%}'}
                        ).applymap(define_color, subset=['L/P Dia', 'Var', 'US$'])

    return df


def define_color(val):
    if val < 0:
        color = 'red'
    elif val > 0:
        color = 'green'
    else:
        color = 'gray'
    return 'color: %s' % color


def resultado_do_dia(conn, data_base):

    # Obtém valor do dolar e Bovespa
  
    df_dolar = investimentos.get_dolar_data(conn, data_base)

    df_dolar['indicador'] = 'Dolar'
 
    df_dolar['ind_var'] = df_dolar.cot_hoje / df_dolar.cot_ant - 1
    df_dolar = df_dolar[['indicador', 'cot_hoje', 'ind_var']]
    df_dolar.columns = ['indicador', 'Valor', '%']
    df_dolar = df_dolar.set_index('indicador')
    df_dolar = df_dolar.style.format(thousands='.',
                         decimal = ',',
                         formatter={'Valor': '{:,.4f}',
                                    '%': '{:,.2%}'}).applymap(define_color, subset=['%'])

    df_bovespa = investimentos.get_bovespa_data(conn, data_base)

    df_bovespa['indicador'] = 'Bovespa'
 
    df_bovespa['ind_var'] = df_bovespa.cot_hoje / df_bovespa.cot_ant - 1
    df_bovespa = df_bovespa[['indicador', 'cot_hoje', 'ind_var']]
    df_bovespa.columns = ['indicador', 'Valor', '%']
    df_bovespa = df_bovespa.set_index('indicador')
    df_bovespa = df_bovespa.style.format(thousands='.',
                         decimal = ',',
                         formatter={'Valor': '{:,.0f}',
                                    '%': '{:,.2%}'}).applymap(define_color, subset=['%'])


    base_dia = pd.read_sql('SELECT * FROM CotacaoHoje', conn)

    col1, col2 = st.columns(2)

    with col1:
        st.table(dataframe_resumo_total(base_dia))
        st.table(dataframe_resumo_por_tipo(base_dia))

    with col2:
        st.table(df_bovespa)
        st.table(df_dolar)

    col1, col2 = st.columns([1.8, 1])

    #with col1:
    for tipo in base_dia.tipo.drop_duplicates():

        with st.expander(f'{tipo}', expanded=True):
        #st.markdown(f'*{tipo}*')
            st.table(dataframe_por_tipo(base_dia, tipo=tipo))    


def calcula_carteira(data_base):

    # Carteira

    # Corretoras

    sql = f'''
        SELECT data, corret, sum(saldo) as saldo
        FROM PosicaoDiaria
        WHERE saldo <> 0
        GROUP BY data, corret
        ORDER BY data, corret
    '''
    df = pd.read_sql(sql, conn)

    df.data = df.data.str[:10]
    df = df[df.data == data_base]

    lista_corret = ['']
    lista_corret.extend(df.corret.tolist())

    tot_saldo = df.saldo.sum()
    st.subheader(f'TOTAL: R$ {tot_saldo:,.2f}'.replace(',', 'x').replace('.', ',').replace('x', '.'))

    corret = st.selectbox(label= 'Corretora:', options=lista_corret)

    # Ações

    if corret == '':
        sql = '''
            SELECT data, tipo, ticker, sum(acum) as qtd, min(close) as cot,
                    sum(valor) as valor, sum(saldo) as saldo
            FROM PosicaoDiariaAcao
            GROUP BY data, tipo, ticker
            ORDER BY data, tipo, saldo DESC
        '''
    else:
        sql = f'''
            SELECT data, tipo, ticker, sum(acum) as qtd, min(close) as cot,
                    sum(valor) as valor, sum(saldo) as saldo
            FROM PosicaoDiariaAcao
            WHERE corret = '{corret}'
            GROUP BY data, tipo, ticker
            ORDER BY data, tipo, saldo DESC
        '''

    df = pd.read_sql(sql, conn)

    df.data = df.data.str[:10]
    df = df[df.data == data_base]

    preco_medio = investimentos.preco_medio_acao(conn)

    df = df.merge(preco_medio, on='ticker', how='left')
    
    df['lucro_aberto'] = df.qtd * (df.cot - df.preco_medio)
    df['lucro_aberto_perc'] = df.cot / df.preco_medio - 1

    
    for tipo in df.tipo.drop_duplicates():

        df_exib = df[df.tipo == tipo]

        tot_saldo = df_exib.saldo.sum()
        tot_dolar = df_exib.valor.sum()

        tot_lucro_aberto = df_exib.lucro_aberto.sum()

        df_exib = df_exib[['ticker', 'qtd', 'preco_medio', 'cot', 'lucro_aberto_perc', 'lucro_aberto', 'valor', 'saldo']]
        df_exib.columns = ['ticker', 'Qtd', 'Preço Médio', 'Cot', 'L/P %', 'L/P', 'US$', 'Saldo']

        if tipo not in ['ETF', 'REIT', 'Stock']:
            df_exib = df_exib[['ticker', 'Qtd', 'Preço Médio', 'Cot', 'L/P %', 'L/P', 'Saldo']]
        
        df_exib = df_exib.set_index('ticker')

        if tipo in ['ETF', 'REIT', 'Stock']:
            df_exib = df_exib.style.format(
            thousands='.',
            decimal = ',',
            formatter={'Qtd': '{:,.1f}',
                       'Cot': '{:,.2f}',
                       'L/P %': '{:,.2%}',
                       'L/P': '{:,.2f}',
                       'US$': '{:,.2f}',
                       'Preço Médio': '{:,.2f}',
                       'Saldo': '{:,.2f}'}).applymap(define_color, subset=['L/P %', 'L/P'])

            with st.expander(f'{tipo}:    US$ {tot_dolar:,.2f} ------------- R$ {tot_saldo:,.2f} ------------- Lucro Aberto: US$ {tot_lucro_aberto:,.2f}'.replace(',', 'x').replace('.', ',').replace('x', '.'),
                expanded=True):
                st.table(df_exib)

        else:
            df_exib = df_exib.style.format(
            thousands='.',
            decimal = ',',
            formatter={'Qtd': '{:,.0f}',
                       'Cot': '{:,.2f}',
                       'L/P %': '{:,.2%}',
                       'L/P': '{:,.2f}',
                       'Preço Médio': '{:,.2f}',
                       'Saldo': '{:,.2f}'}).applymap(define_color, subset=['L/P %', 'L/P'])
        
            with st.expander(f'{tipo}:   R$ {tot_saldo:,.2f} ------------- Lucro Aberto: R$ {tot_lucro_aberto:,.2f}'.replace(',', 'x').replace('.', ',').replace('x', '.'),
                expanded=True):
                st.table(df_exib)


    # Tesouro Direto

    if corret == '':
        sql = '''
            SELECT data, nome, sum(qtd_acum) as qtd, min(preco_unit) as cot, sum(saldo) as saldo
            FROM PosicaoDiariaTD
            GROUP BY data, nome
            ORDER BY data, saldo DESC
        '''
    else:
        sql = f'''
            SELECT data, nome, sum(qtd_acum) as qtd, min(preco_unit) as cot, sum(saldo) as saldo
            FROM PosicaoDiariaTD
            WHERE corret = '{corret}'
            GROUP BY data, nome
            ORDER BY data, saldo DESC
        '''
        
    df = pd.read_sql(sql, conn)
    df.data = df.data.str[:10]
    df = df[df.data == data_base]

    preco_medio = investimentos.preco_medio_td(conn)

    df = df.merge(preco_medio, on='nome', how='left')
    
    df['lucro_aberto'] = df.qtd * (df.cot - df.preco_medio)
    df['lucro_aberto_perc'] = df.cot / df.preco_medio - 1

    # Recupera data base da última cotação para cada título

    data_base_td = pd.read_sql('SELECT * FROM DataBaseTesouroDireto', conn)
        
    df_exib = df

    df_exib = df_exib.merge(data_base_td, on='nome', how='left')

    tot_lucro_aberto = df_exib.lucro_aberto.sum()

    if df_exib.shape[0] != 0:

        tot_saldo = df_exib.saldo.sum()

        df_exib.data_base = df_exib.data_base.str[8:10] + '/' + df_exib.data_base.str[5:7] + '/' + df_exib.data_base.str[0:4]
        
        df_exib = df_exib[['nome', 'data_base', 'qtd', 'preco_medio', 'cot', 'lucro_aberto_perc', 'lucro_aberto', 'saldo']]
        df_exib.columns = ['nome', 'Base', 'Qtd', 'Preço Médio', 'Preço', 'L/P %', 'L/P', 'Saldo']

        df_exib = df_exib.set_index('nome')

        df_exib = df_exib.style.format(
            thousands='.',
            decimal = ',',
            formatter={'Qtd': '{:,.2f}',
                       'L/P %': '{:,.2%}',
                       'L/P': '{:,.2f}',
                       'Preço': '{:,.2f}',
                       'Preço Médio': '{:,.2f}',
                       'Saldo': '{:,.2f}'}).applymap(define_color, subset=['L/P %', 'L/P'])
        
        with st.expander(f'Tesouro Direto:   R$ {tot_saldo:,.2f} ------------- Lucro Aberto: R$ {tot_lucro_aberto:,.2f}'.replace(',', 'x').replace('.', ',').replace('x', '.'),
            expanded=True):
            st.table(df_exib)

    # Fundos

    if corret == '':
        sql = '''
            SELECT data, corret, nome, sum(qtd_acum) as qtd, min(fundo_vlr_quota) as cot, sum(saldo) as saldo
            FROM PosicaoDiariaFundos
            GROUP BY data, corret, nome
            ORDER BY data, saldo DESC
        '''
    else:
        sql = f'''
            SELECT data, corret, nome, sum(qtd_acum) as qtd, min(fundo_vlr_quota) as cot, sum(saldo) as saldo
            FROM PosicaoDiariaFundos
            WHERE corret = '{corret}'
            GROUP BY data, corret, nome
            ORDER BY data, saldo DESC
        '''
    df_fundos = pd.read_sql(sql, conn)
    df_fundos.data = df_fundos.data.str[:10]
    df_fundos = df_fundos[df_fundos.data == data_base]

    preco_medio = investimentos.preco_medio_fundos(conn)

    df_fundos = df_fundos.merge(preco_medio, on='nome', how='left')
    
    df_fundos['lucro_aberto'] = df_fundos.qtd * (df_fundos.cot - df_fundos.preco_medio)
    df_fundos['lucro_aberto_perc'] = df_fundos.cot / df_fundos.preco_medio - 1

    df_exib = df_fundos

    tot_lucro_aberto = df_exib.lucro_aberto.sum()

    if df_exib.shape[0] != 0:

        tot_saldo = df_exib.saldo.sum()
        
        df_exib = df_exib[['nome', 'qtd', 'preco_medio', 'cot', 'lucro_aberto_perc', 'lucro_aberto', 'saldo']]
        df_exib.columns = ['nome', 'Qtd', 'Preço Médio', 'Preço', 'L/P %', 'L/P', 'Saldo']

        df_exib = df_exib.set_index('nome')

        df_exib = df_exib.style.format(
            thousands='.',
            decimal = ',',
            formatter={'Qtd': '{:,.6f}',
                       'L/P %': '{:,.2%}',
                       'L/P': '{:,.2f}',
                       'Preço': '{:,.6f}',
                       'Preço Médio': '{:,.6f}',
                       'Saldo': '{:,.2f}'}).applymap(define_color, subset=['L/P %', 'L/P'])

        with st.expander(f'Fundos:   R$ {tot_saldo:,.2f} ------------- Lucro Aberto: R$ {tot_lucro_aberto:,.2f}'.replace(',', 'x').replace('.', ',').replace('x', '.'),
            expanded=True):
            st.table(df_exib)


    # Renda Fixa

    if corret == '':
        sql = '''
            SELECT data, corret, tipo, nome, sum(saldo) as saldo
            FROM PosicaoDiariaRF
            GROUP BY data, corret, tipo, nome
            ORDER BY data, saldo DESC
    '''
    else:
        sql = f'''
            SELECT data, corret, tipo, nome, sum(saldo) as saldo
            FROM PosicaoDiariaRF
            WHERE corret = '{corret}'
            GROUP BY data, corret, tipo, nome
            ORDER BY data, saldo DESC
    '''

    df = pd.read_sql(sql, conn)

    df.data = df.data.str[:10]
    df = df[df.data == data_base]

    df_exib = df

    if df_exib.shape[0] != 0:

        tot_saldo = df_exib.saldo.sum()
        
        df_exib = df_exib[['corret', 'tipo', 'nome', 'saldo']]
        df_exib.columns = ['corret', 'Classe', 'Nome', 'Saldo']

        df_exib = df_exib.set_index('corret')

        df_exib = df_exib.style.format(
            thousands='.',
            decimal = ',',
            formatter={'Saldo': '{:,.2f}'})
        
        with st.expander(f'Renda Fixa:   R$ {tot_saldo:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.'),
            expanded=True):
            st.table(df_exib)


def posicao_por_classe(df, data_base):

    # Posição Diária por classe

    # Gera figura do gráfico histórico

    # Obtém df com saldos dos investimentos no último dia de cada mês
    df_graf = investimentos.saldos_ultimo_dia_mes(df, data_base, ['classe', 'data'])
    
    fig2 = go.Figure()

    for classe in df_graf.classe.drop_duplicates():
        barra = df_graf[df_graf.classe == classe]
        fig2.add_trace(
            go.Bar(x=barra.data.dt.strftime('%Y-%m'), y=barra.saldo, name=classe))
    
    fig2.update_layout(barmode='stack' , separators = '.,',)

    fig2.update_layout(legend=dict(orientation='h',
                                   yanchor='bottom',
                                   y=1,
                                   xanchor='right',
                                   x=1))

    # Gera dataframe de exibição

    df_exib1 = df_graf[df_graf.data == data_base]

    df_exib1 = df_exib1.sort_values('saldo', ascending=False)

    tot_saldo = df_exib1.saldo.sum()

    df_exib1 = df_exib1[['classe', 'saldo']]

    df_exib1.columns = ['Classe', 'Valor']

    # Gera figura do gráfico pizza

    fig1 = go.Figure()

    fig1 = px.pie(df_exib1, values=df_exib1['Valor'], names=df_exib1['Classe'])

    fig1.update_traces(textposition='inside', textinfo='percent+label+value')

    # Formata Dataframe

    row = pd.Series({'Classe': 'TOTAL', 'Valor': tot_saldo})

    df_exib1 = df_exib1.append(row, ignore_index=True)

    df_exib1['%'] = df_exib1.Valor / tot_saldo

    df_exib1.reset_index(inplace=True, drop=True) 

    df_exib1 = df_exib1.set_index('Classe')

    df_exib1 = df_exib1.style.format(
        thousands='.',
        decimal = ',',
        formatter={'Valor': '{:,.2f}',
                    '%': '{:,.1%}'}
        )

    # Gera dataframe de exibição Classe/Corretora
    df_exib2 = investimentos.saldos_ultimo_dia_mes(df, data_base, ['classe', 'corret', 'data'])

    df_exib2 = df_exib2[df_exib2.data == data_base]

    df_exib2 = df_exib2[['classe', 'corret', 'saldo']]

    df_exib2.columns = ['Classe', 'Corret', 'Valor']

    df_exib2 = df_exib2.set_index('Classe')

    df_exib2 = df_exib2.style.format(
        thousands='.',
        decimal = ',',
        formatter={'Valor': '{:,.2f}',
                    '%': '{:,.1%}'}
        )


    col1, col2 = st.columns(2)

    with col1:
        st.dataframe(df_exib1)

    with col2:
        st.dataframe(df_exib2)

    col1, col2 = st.columns([1, 1.5])

    with col1:
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        st.plotly_chart(fig2, use_container_width=True)


def posicao_por_estrategia(df, data_base):

    # Posição Diária por estratégia

    # Obtém base investimentos
    sql = '''
        SELECT *
        FROM Investimento
    '''
    invest = pd.read_sql(sql, conn)

    saldos = investimentos.saldos_ultimo_dia_mes(df, data_base, ['classe', 'tipo', 'ticker', 'data'])

    # Tratamento das Ações
    estrat_acao = saldos[saldos.classe == 'Ações'].groupby(['tipo', 'data']).sum().reset_index()
    estrat_acao = estrat_acao.merge(invest, left_on=['tipo'], right_on=['inv_nome'], how='left')
    estrat_acao = estrat_acao.groupby(['inv_book', 'data']).sum().reset_index()

    # Tratamento do Tesouro Direto
    estrat_td = saldos[saldos.classe == 'Tesouro Direto'].groupby(['classe', 'data']).sum().reset_index()
    estrat_td = estrat_td.merge(invest, left_on=['classe'], right_on=['inv_nome'], how='left')
    estrat_td = estrat_td.groupby(['inv_book', 'data']).sum().reset_index()

    # Tratamento dos Fundos
    estrat_fundos = saldos[saldos.classe == 'Fundos'].groupby(['ticker', 'data']).sum().reset_index()
    estrat_fundos = estrat_fundos.merge(invest, left_on=['ticker'], right_on=['inv_nome'], how='left')
    estrat_fundos = estrat_fundos.groupby(['inv_book', 'data']).sum().reset_index()

    # Tratamento de Renda Fixa
    estrat_rf = saldos[saldos.classe == 'Renda Fixa'].groupby(['ticker', 'data']).sum().reset_index()
    estrat_rf = estrat_rf.merge(invest, left_on=['ticker'], right_on=['inv_nome'], how='left')
    estrat_rf = estrat_rf.groupby(['inv_book', 'data']).sum().reset_index()

    
    df_graf = pd.concat([estrat_acao, estrat_td, estrat_fundos, estrat_rf])
    df_graf = df_graf.groupby(['inv_book', 'data']).sum().reset_index()

    carteira = df_graf.groupby(['data']).sum().reset_index()

    # Aportes

    aportes = investimentos.aportes_mensais(conn, data_base)
    aportes = aportes.groupby('data').sum().reset_index()
    aportes['valor_acum'] = aportes.valor.cumsum()


    fig2 = go.Figure()

    for book in df_graf.inv_book.drop_duplicates():
        barra = df_graf[df_graf.inv_book == book]
        fig2.add_trace(
            go.Bar(x=barra.data.dt.strftime('%Y-%m'), y=barra.saldo, name=book))

    fig2.add_trace(
        go.Scatter(x=aportes.data, y=aportes.valor_acum, name='Aportes', line=dict(color='purple')))
    fig2.add_trace(
        go.Scatter(x=carteira.data, y=carteira.saldo, name='Carteira', line=dict(color='midnightblue')))

    fig2.update_layout(barmode='stack' , separators = '.,',)

    fig2.update_layout(legend=dict(orientation='h',
                                   yanchor='bottom',
                                   y=1,
                                   xanchor='right',
                                   x=1))

    # Gera dataframe de exibição

    dt_maxima = df_graf.data.max()

    df_exib1 = df_graf[df_graf.data == dt_maxima]

    df_exib1 = df_exib1.sort_values('saldo', ascending=False)

    tot_saldo = df_exib1.saldo.sum()

    df_exib1 = df_exib1[['inv_book', 'saldo']]

    df_exib1.columns = ['Book', 'Valor']

    # Gera figura do gráfico pizza

    fig1 = go.Figure()

    fig1 = px.pie(df_exib1, values=df_exib1['Valor'], names=df_exib1['Book'])

    fig1.update_traces(textposition='inside', textinfo='percent+label+value')

    fig1.update_layout(showlegend=False)

    # Formata Dataframe

    row = pd.Series({'Book': 'TOTAL', 'Valor': tot_saldo})

    df_exib1 = df_exib1.append(row, ignore_index=True)

    df_exib1['%'] = df_exib1.Valor / tot_saldo

    df_exib1.reset_index(inplace=True, drop=True) 

    df_exib1 = df_exib1.set_index('Book')

    df_exib1 = df_exib1.style.format(
        thousands='.',
        decimal = ',',
        formatter={'Valor': '{:,.2f}',
                    '%': '{:,.1%}'}
        )


    col1, col2 = st.columns(2)

    with col1:
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        st.table(df_exib1)

    st.plotly_chart(fig2, use_container_width=True)


def liquidez(df, data_base):

    # Posição por liquidez

    # Obtém base investimentos
    sql = '''
        SELECT *
        FROM Investimento
    '''
    invest = pd.read_sql(sql, conn)

    saldos = investimentos.saldos_ultimo_dia_mes(df, data_base, ['classe', 'tipo', 'ticker', 'data'])

    # Tratamento das Ações
    estrat_acao = saldos[saldos.classe == 'Ações'].groupby(['tipo', 'data']).sum().reset_index()
    estrat_acao = estrat_acao.merge(invest, left_on=['tipo'], right_on=['inv_nome'], how='left')
    estrat_acao = estrat_acao.groupby(['inv_liquidez_sort', 'inv_liquidez', 'data']).sum().reset_index()

    # Tratamento do Tesouro Direto
    estrat_td = saldos[saldos.classe == 'Tesouro Direto'].groupby(['classe', 'data']).sum().reset_index()
    estrat_td = estrat_td.merge(invest, left_on=['classe'], right_on=['inv_nome'], how='left')
    estrat_td = estrat_td.groupby(['inv_liquidez_sort', 'inv_liquidez', 'data']).sum().reset_index()

    # Tratamento dos Fundos
    estrat_fundos = saldos[saldos.classe == 'Fundos'].groupby(['ticker', 'data']).sum().reset_index()
    estrat_fundos = estrat_fundos.merge(invest, left_on=['ticker'], right_on=['inv_nome'], how='left')
    estrat_fundos = estrat_fundos.groupby(['inv_liquidez_sort', 'inv_liquidez', 'data']).sum().reset_index()

    # Tratamento de Renda Fixa
    estrat_rf = saldos[saldos.classe == 'Renda Fixa'].groupby(['ticker', 'data']).sum().reset_index()
    estrat_rf = estrat_rf.merge(invest, left_on=['ticker'], right_on=['inv_nome'], how='left')
    estrat_rf = estrat_rf.groupby(['inv_liquidez_sort', 'inv_liquidez', 'data']).sum().reset_index()

    
    df_graf = pd.concat([estrat_acao, estrat_td, estrat_fundos, estrat_rf])
    df_graf = df_graf.groupby(['inv_liquidez_sort', 'inv_liquidez', 'data']).sum().reset_index()

    carteira = df_graf.groupby(['data']).sum().reset_index()

    # Aportes

    aportes = investimentos.aportes_mensais(conn, data_base)
    aportes = aportes.groupby('data').sum().reset_index()
    aportes['valor_acum'] = aportes.valor.cumsum()


    fig2 = go.Figure()

    for liquidez in df_graf.inv_liquidez.drop_duplicates():
        barra = df_graf[df_graf.inv_liquidez == liquidez]
        fig2.add_trace(
            go.Bar(x=barra.data.dt.strftime('%Y-%m'), y=barra.saldo, name=liquidez))

    fig2.add_trace(
        go.Scatter(x=aportes.data, y=aportes.valor_acum, name='Aportes', line=dict(color='purple')))
    fig2.add_trace(
        go.Scatter(x=carteira.data, y=carteira.saldo, name='Carteira', line=dict(color='midnightblue')))

    fig2.update_layout(barmode='stack' , separators = '.,',)

    fig2.update_layout(legend=dict(orientation='h',
                                   yanchor='bottom',
                                   y=1,
                                   xanchor='right',
                                   x=1))

    # Gera dataframe de exibição

    dt_maxima = df_graf.data.max()

    df_exib1 = df_graf[df_graf.data == dt_maxima]

    #df_exib1 = df_exib1.sort_values('saldo', ascending=False)

    tot_saldo = df_exib1.saldo.sum()

    df_exib1 = df_exib1[['inv_liquidez', 'saldo']]

    df_exib1.columns = ['Liquidez', 'Valor']

    # Gera figura do gráfico pizza

    fig1 = go.Figure()

    fig1 = px.pie(df_exib1, values=df_exib1['Valor'], names=df_exib1['Liquidez'])

    fig1.update_traces(textposition='inside', textinfo='percent+label+value')

    fig1.update_layout(showlegend=False)

    # Formata Dataframe

    row = pd.Series({'Liquidez': 'TOTAL', 'Valor': tot_saldo})

    df_exib1 = df_exib1.append(row, ignore_index=True)

    df_exib1['%'] = df_exib1.Valor / tot_saldo

    df_exib1.reset_index(inplace=True, drop=True) 

    df_exib1 = df_exib1.set_index('Liquidez')

    df_exib1 = df_exib1.style.format(
        thousands='.',
        decimal = ',',
        formatter={'Valor': '{:,.2f}',
                    '%': '{:,.1%}'}
        )


    col1, col2 = st.columns(2)

    with col1:
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        st.table(df_exib1)

    st.plotly_chart(fig2, use_container_width=True)


def posicao_por_corretora(df, data_base):

    # Posição Diária por corretora

    # Gera figura do gráfico histórico

    # Obtém df com saldos dos investimentos no último dia de cada mês
    df_graf = investimentos.saldos_ultimo_dia_mes(df, data_base, ['corret', 'data'])

    fig2 = go.Figure()

    for corret in df_graf.corret.drop_duplicates():
        barra = df_graf[df_graf.corret == corret]
        fig2.add_trace(
            go.Bar(x=barra.data.dt.strftime('%Y-%m'), y=barra.saldo, name=corret))
    
    fig2.update_layout(barmode='stack' , separators = '.,',)

    fig2.update_layout(legend=dict(orientation='h',
                                   yanchor='bottom',
                                   y=1,
                                   xanchor='right',
                                   x=1))

    # Gera dataframe de exibição

    df_exib1 = df_graf[df_graf.data == data_base]

    df_exib1 = df_exib1.sort_values('saldo', ascending=False)

    tot_saldo = df_exib1.saldo.sum()

    df_exib1 = df_exib1[['corret', 'saldo']]

    df_exib1.columns = ['Corret', 'Valor']

    # Gera figura do gráfico pizza

    fig1 = go.Figure()

    fig1 = px.pie(df_exib1, values=df_exib1['Valor'], names=df_exib1['Corret'])

    fig1.update_traces(textposition='inside', textinfo='percent+label+value')


    # Formata Dataframe

    row = pd.Series({'Corret': 'TOTAL', 'Valor': tot_saldo})

    df_exib1 = df_exib1.append(row, ignore_index=True)

    df_exib1['%'] = df_exib1.Valor / tot_saldo

    df_exib1.reset_index(inplace=True, drop=True) 

    df_exib1 = df_exib1.set_index('Corret')

    df_exib1 = df_exib1.style.format(
        thousands='.',
        decimal = ',',
        formatter={'Valor': '{:,.2f}',
                    '%': '{:,.1%}'}
        )

    # Gera dataframe de exibição Corretora/Classe
    df_exib2 = investimentos.saldos_ultimo_dia_mes(df, data_base, ['corret', 'classe', 'data'])

    df_exib2 = df_exib2[df_exib2.data == data_base]

    #df_temp2 = df_temp2.sort_values('saldo', ascending=False)

    df_exib2 = df_exib2[['corret', 'classe', 'saldo']]

    df_exib2.columns = ['Corret', 'Classe', 'Valor']

    df_exib2 = df_exib2.set_index('Corret')

    df_exib2 = df_exib2.style.format(
        thousands='.',
        decimal = ',',
        formatter={'Valor': '{:,.2f}',
                    '%': '{:,.1%}'}
        )


    col1, col2 = st.columns(2)

    with col1:
        st.dataframe(df_exib1)

    with col2:
        st.dataframe(df_exib2)

    col1, col2 = st.columns([1, 1.5])

    with col1:
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        st.plotly_chart(fig2, use_container_width=True)


def rentabilidade_grafica(conn, data_base):


    # Comparativo de Rentabilidade

    data_hoje = date.today().strftime('%Y-%m-%d')

    df = pd.DataFrame()

    col1, col2, col3 = st.columns(3)

    # Ações
    sql = '''
        SELECT data, ticker, close
        FROM PosicaoDiariaAcao
    '''
    df_acao = pd.read_sql(sql, conn)

    df_acao.columns = ['data', 'nome', 'valor']

    lista_acao = (df_acao.nome).sort_values().drop_duplicates().tolist()

    with col1:
        selec_acao = st.multiselect('Ações:', lista_acao)

  # Tesouro Direto
    sql = '''
        SELECT data, nome, preco_unit
        FROM PosicaoDiariaTD
    '''
    df_td = pd.read_sql(sql, conn)

    df_td.columns = ['data', 'nome', 'valor']

    lista_td = (df_td.nome).sort_values().drop_duplicates().tolist()

    with col2:
        selec_td = st.multiselect('Tesouro Direto:', lista_td)

  # Fundos
    sql = '''
        SELECT data, nome, fundo_vlr_quota
        FROM PosicaoDiariaFundos
    '''
    df_fundos = pd.read_sql(sql, conn)

    df_fundos.columns = ['data', 'nome', 'valor']

    lista_fundos = (df_fundos.nome).sort_values().drop_duplicates().tolist()

    with col3:
        selec_fundos = st.multiselect('Fundos:', lista_fundos)


    df = pd.concat([df_acao[df_acao.nome.isin(selec_acao)],
                    df_td[df_td.nome.isin(selec_td)],
                    df_fundos[df_fundos.nome.isin(selec_fundos)]])

    if data_hoje != data_base:
        df = df[df.data >= data_base]


    # Operações

    # Ações
    sql = '''
        SELECT oper_data, oper_cv, oper_ticker, oper_qtd, oper_preco_unit
        FROM OperacaoAcao
    '''
    df_oper_acao = pd.read_sql(sql, conn)

    df_oper_acao.columns = ['data', 'oper_cv', 'nome', 'qtd', 'preco_unit']

    # Tesouro Direto
    sql = '''
        SELECT oper_data, oper_cv, oper_nome, oper_qtd, oper_preco_unit
        FROM OperacaoTD
    '''
    df_oper_td = pd.read_sql(sql, conn)

    df_oper_td.columns = ['data', 'oper_cv', 'nome', 'qtd', 'preco_unit']

    # Fundos
    sql = '''
        SELECT oper_data, oper_cv, oper_nome, oper_qtd, oper_preco_unit
        FROM OperacaoFundos
    '''
    df_oper_fundos = pd.read_sql(sql, conn)

    df_oper_fundos.columns = ['data', 'oper_cv', 'nome', 'qtd', 'preco_unit']


    df_oper = pd.concat([df_oper_acao[df_oper_acao.nome.isin(selec_acao)],
                    df_oper_td[df_oper_td.nome.isin(selec_td)],
                    df_oper_fundos[df_oper_fundos.nome.isin(selec_fundos)]])

    if data_hoje != data_base:
        df_oper = df_oper[df_oper.data >= data_base]


    df_oper_compra =df_oper[df_oper.qtd > 0]

    df_oper_venda =df_oper[df_oper.qtd < 0]


    # Preço Médio

    # Ação
    sql = '''
        SELECT ticker as nome, preco_medio
        FROM PrecoMedioAcao
    '''
    df_preco_medio_acao = pd.read_sql(sql, conn)

    # Tesouro Direto
    sql = '''
        SELECT nome, preco_medio
        FROM PrecoMedioTD
    '''
    df_preco_medio_td = pd.read_sql(sql, conn)

    # Fundos
    sql = '''
        SELECT nome, preco_medio
        FROM PrecoMedioFundos
    '''
    df_preco_medio_fundos = pd.read_sql(sql, conn)

    df_preco_medio = pd.concat([df_preco_medio_acao, df_preco_medio_td, df_preco_medio_fundos])


    fig1 = go.Figure()
    fig2 = go.Figure()

    for nome in df.nome.drop_duplicates():

        df_exib = df[df.nome == nome].copy()
        df_exib['rentab'] = (df_exib.valor / df_exib.valor.iloc[0] - 1) * 100

        fig1.add_trace(
            go.Scatter(x=df_exib.data, y=df_exib.valor, name=nome, hovertemplate='%{y:,.4f}'))

        fig1.add_trace(
            go.Scatter(x=df_oper_compra.data, y=df_oper_compra.preco_unit, name='Compras '+ nome, hovertemplate='%{y:,.4f}', mode='markers', marker_symbol = 'star', marker_size = 12))

        fig1.add_trace(
            go.Scatter(x=df_oper_venda.data, y=df_oper_venda.preco_unit, name='Vendas '+ nome, hovertemplate='%{y:,.4f}', mode='markers', marker_symbol = 'star', marker_size = 12))


        if df_preco_medio[df_preco_medio.nome == nome].shape[0] > 0:
        
            preco_medio = df_preco_medio[df_preco_medio.nome == nome]['preco_medio'].iloc[0]
            
            fig1.add_hline(y=preco_medio)
        
        fig2.add_trace(
            go.Scatter(x=df_exib.data, y=df_exib.rentab, name=nome, hovertemplate='%{y:,.1f}%'))

    fig1.update_layout(hovermode='x unified',
                       legend=dict(orientation='h',
                                   yanchor='bottom',
                                   y=1,
                                   xanchor='right',
                                   x=1))

    fig2.update_layout(hovermode='x unified',
                       legend=dict(orientation='h',
                                   yanchor='bottom',
                                   y=1,
                                   xanchor='right',
                                   x=1))

    fig2.update_traces(mode='lines+text')


    st.plotly_chart(fig1, use_container_width=True)

    st.plotly_chart(fig2, use_container_width=True)


def proventos():


    df = investimentos.proventos_por_dia(conn)

    col1, col2 = st.columns(2)

    with col1:
        opcao_ativo = st.multiselect('Ativo:', df.prov_ticker.drop_duplicates().sort_values().tolist())

    with col2:
        opcao_corret = st.multiselect('Corretora:', df.prov_corret.drop_duplicates().sort_values().tolist())


    df.prov_data = pd.to_datetime(df.prov_data).dt.strftime('%d/%m/%Y')

    if opcao_ativo != []:
        df = df[df.prov_ticker.isin(opcao_ativo)]

    if opcao_corret != []:
        df = df[df.prov_corret.isin(opcao_corret)]

    # Calcula provento em reais
    df['prov_valor_real'] = df.prov_valor
    df.loc[df.prov_moeda == 'EUA', 'prov_valor_real'] = round(df.prov_valor * df.dolar, 2)

    tot_prov = df.prov_valor_real.sum()

    df = df[['prov_data', 'prov_corret', 'prov_tipo', 'prov_ticker', 'prov_forma', 'prov_moeda', 'prov_valor', 'prov_valor_real']]

    df.columns = ['Data', 'Corret', 'Tipo', 'Ticker', 'Forma', 'Moeda', 'Valor', 'Valor R$']

    df = df.set_index('Data')

    df = df.style.format(
            thousands='.',
            decimal = ',',
            formatter={'Qtd': '{:,.1f}',
                       'Cot': '{:,.2f}',
                       'Valor': '{:,.2f}',
                       'Valor R$': '{:,.2f}'})

    st.table(df)

    st.subheader(f'Proventos: R$ {tot_prov:,.2f}'.replace(',', 'x').replace('.', ',').replace('x', '.'))


def operacoes_acao(df):

    col1, col2 = st.columns([1.5, 1])

    with col1:
        opcao_ativo = st.multiselect('Ativo:', df.oper_ticker.drop_duplicates().sort_values().tolist())
    with col2:
        opcao_corret = st.multiselect('Corretora:', df.oper_corret.drop_duplicates().sort_values().tolist())

    df.oper_data = pd.to_datetime(df.oper_data).dt.strftime('%d/%m/%Y')

    if opcao_ativo != []:
        df = df[df.oper_ticker.isin(opcao_ativo)]

    if opcao_corret != []:
        df = df[df.oper_corret.isin(opcao_corret)]

    df = df[['oper_data', 'oper_cv', 'oper_corret', 'oper_tipo', 'oper_ticker', 'oper_moeda', 'oper_qtd', 'oper_preco_unit', 'oper_valor']]

    df.columns = ['Data', 'C/V', 'Corret', 'Tipo', 'Ticker', 'Moeda', 'Qtd', 'Preço', 'Valor']

    df = df.set_index('Data')

    df = df.style.format(
            thousands='.',
            decimal = ',',
            formatter={'Qtd': '{:,.1f}',
                       'Cot': '{:,.2f}',
                       'US$': '{:,.2f}',
                       'Preço': '{:,.2f}',
                       'Valor': '{:,.2f}'})

    st.table(df)


def operacoes_td(df):

    col1, col2 = st.columns([1.5, 1])

    with col1:
        opcao_ativo = st.multiselect('Ativo:', df.oper_nome.drop_duplicates().sort_values().tolist())

    with col2:
        opcao_corret = st.multiselect('Corretora:', df.oper_corret.drop_duplicates().sort_values().tolist())

    df.oper_data = pd.to_datetime(df.oper_data).dt.strftime('%d/%m/%Y')

    if opcao_ativo != []:
        df = df[df.oper_nome.isin(opcao_ativo)]

    if opcao_corret != []:
        df = df[df.oper_corret.isin(opcao_corret)]

    df = df[['oper_data', 'oper_cv', 'oper_corret', 'oper_nome', 'oper_qtd', 'oper_preco_unit', 'oper_valor']]

    df.columns = ['Data', 'C/V', 'Corret', 'Nome', 'Qtd', 'Preço', 'Valor']

    df = df.set_index('Data')

    df = df.style.format(
            thousands='.',
            decimal = ',',
            formatter={'Qtd': '{:,.2f}',
                       'Preço': '{:,.2f}',
                       'Valor': '{:,.2f}'})

    st.table(df)


def operacoes_fundos(df):

    col1, col2 = st.columns([1.5, 1])

    with col1:
        opcao_ativo = st.multiselect('Ativo:', df.oper_nome.drop_duplicates().sort_values().tolist())

    with col2:
        opcao_corret = st.multiselect('Corretora:', df.oper_corret.drop_duplicates().sort_values().tolist())

    df.oper_data = pd.to_datetime(df.oper_data).dt.strftime('%d/%m/%Y')

    if opcao_ativo != []:
        df = df[df.oper_nome.isin(opcao_ativo)]

    if opcao_corret != []:
        df = df[df.oper_corret.isin(opcao_corret)]

    df = df[['oper_data', 'oper_cv', 'oper_corret', 'oper_nome', 'oper_qtd', 'oper_preco_unit', 'oper_valor']]

    df.columns = ['Data', 'C/V', 'Corret', 'Nome', 'Qtd', 'Preço', 'Valor']

    df = df.set_index('Data')

    df = df.style.format(
            thousands='.',
            decimal = ',',
            formatter={'Qtd': '{:,.6f}',
                       'Preço': '{:,.6f}',
                       'Valor': '{:,.2f}'})

    st.table(df)


def operacoes_rf(df):

    col1, col2 = st.columns([1.5, 1])

    with col1:
        opcao_ativo = st.multiselect('Ativo:', df.rf_nome.drop_duplicates().sort_values().tolist())

    with col2:
        opcao_corret = st.multiselect('Corretora:', df.rf_corret.drop_duplicates().sort_values().tolist())

    #lista_ticker = (df.classe + ':' + df.ticker).sort_values().drop_duplicates().tolist()

    df.rf_data = pd.to_datetime(df.rf_data).dt.strftime('%d/%m/%Y')

    if opcao_ativo != []:
        df = df[df.rf_nome.isin(opcao_ativo)]

    if opcao_corret != []:
        df = df[df.rf_corret.isin(opcao_corret)]

    df = df[['rf_data', 'rf_oper', 'rf_corret', 'rf_nome', 'rf_moeda', 'rf_valor_deb', 'rf_valor_cred']]

    df.columns = ['Data', 'C/V', 'Corret', 'Nome', 'Moeda', 'Déb', 'Créd']

    df = df.set_index('Data')

    df = df.style.format(
            thousands='.',
            decimal = ',',
            formatter={'Qtd': '{:,.6f}',
                       'Déb': '{:,.2f}',
                       'Créd': '{:,.2f}'})

    st.table(df)


def aportes(conn, data_base):


    lista_classe = ['TOTAL', 'Ações', 'Tesouro Direto', 'Fundos', 'Renda Fixa']

    classe_selec = st.selectbox(label= 'Classe:', options=lista_classe)

    # Total da carteira
    
    df = investimentos.apuracao_posicao_diaria(conn, data_base=data_base)
    
    carteira = investimentos.saldos_ultimo_dia_mes(df, data_base=data_base, lista_agrupamento=['data', 'classe'])
    carteira_total = investimentos.saldos_ultimo_dia_mes(df, data_base=data_base, lista_agrupamento=['data'])
    carteira_total['classe'] = 'TOTAL'

    carteira = pd.concat([carteira, carteira_total])

    carteira = carteira[carteira.classe == classe_selec]
    del carteira['classe']
    carteira.columns = ['data', 'Saldo']

    # Aportes

    aportes = investimentos.aportes_mensais(conn, data_base)

    aportes = aportes.pivot_table(values=['valor'], index='data', columns='classe').reset_index().fillna(0)

    aportes = aportes.reindex([('data', ''),
                       ('valor', 'Ações'),
                       ('valor', 'Tesouro Direto'),
                       ('valor', 'Fundos'),
                       ('valor', 'Renda Fixa'),
                       ('valor', 'Dividendos')], axis=1)

    aportes.columns = ['data', 'Ações', 'Tesouro Direto', 'Fundos', 'Renda Fixa', 'Dividendos']

    if classe_selec == 'TOTAL':
        aportes['Aporte'] = aportes['Ações'] + aportes['Tesouro Direto'] + aportes['Fundos'] + aportes['Renda Fixa'] + aportes['Dividendos']
    elif classe_selec == 'Ações':
        aportes['Aporte'] = aportes['Ações'] + aportes['Dividendos']
    else:
        aportes['Aporte'] = aportes[classe_selec]

    aportes['TOTAL'] = aportes['Aporte']

    aportes['Aporte Acum'] = aportes['Aporte'].cumsum()


    if classe_selec == 'TOTAL':
        aportes = aportes[['data', 'Ações', 'Dividendos', 'Tesouro Direto', 'Fundos', 'Renda Fixa', 'Aporte', 'Aporte Acum', 'TOTAL']]
        subset_color = ['Ações', 'Dividendos', 'Tesouro Direto', 'Fundos', 'Renda Fixa', 'Aporte']
    elif classe_selec == 'Ações':
        aportes = aportes[['data', 'Ações', 'Dividendos', 'Aporte', 'Aporte Acum', 'TOTAL']]
        subset_color = ['Ações', 'Dividendos', 'Aporte']
    else:
        aportes = aportes[['data', classe_selec, 'Aporte Acum', 'TOTAL']]
        subset_color = [classe_selec]

    aportes = aportes.merge(carteira, on='data', how='left')

    aportes = aportes.dropna()


    # Gera gráficos


    fig1 = go.Figure()

    fig1.add_trace(
        go.Scatter(x=aportes.data.dt.strftime('%Y-%m'), y=aportes['Aporte Acum'], name='Aportes'))

    fig1.add_trace(
        go.Scatter(x=aportes.data.dt.strftime('%Y-%m'), y=aportes.Saldo, name='Carteira'))

    fig1.update_layout(barmode='stack' , separators = '.,',)

    fig1.update_layout(legend=dict(orientation='h',
                                   yanchor='bottom',
                                   y=1,
                                   xanchor='right',
                                   x=1))

    
    fig2 = go.Figure()

    fig2.add_trace(
        go.Bar(x=aportes.data.dt.strftime('%Y-%m'), y=aportes[classe_selec], name='Aportes', marker=dict(color='green')))

    fig2.update_layout(title='Aportes Mensais', barmode='stack' , separators = '.,',)

    fig2.update_layout(legend=dict(orientation='h',
                                   yanchor='bottom',
                                   y=1,
                                   xanchor='right',
                                   x=1))
    
    
    if classe_selec == 'TOTAL':
    
        fig3 = go.Figure()

        for classe in lista_classe[1:]:
            fig3.add_trace(
                go.Bar(x=aportes.data.dt.strftime('%Y-%m'), y=aportes[classe], name=classe))
    

        fig3.update_layout(barmode='stack' , separators = '.,',)

        fig3.update_layout(legend=dict(orientation='h',
                                       yanchor='bottom',
                                       y=1,
                                       xanchor='right',
                                       x=1))
    

    # Prepara dataframe para exibição

    del aportes['TOTAL']

    aportes = aportes.sort_values('data', ascending=False)

    aportes.data = aportes.data.dt.strftime('%d/%m/%Y')

    aportes = aportes.set_index('data')

    aportes = aportes.style.format(
            thousands='.',
            decimal = ',',
            formatter={'Ações': '{:,.2f}',
                       'Dividendos': '{:,.2f}',
                       'Tesouro Direto': '{:,.2f}',
                       'Fundos': '{:,.2f}',
                       'Renda Fixa': '{:,.2f}',
                       'Saldo': '{:,.2f}',
                       'Aporte': '{:,.2f}',
                       'Aporte Acum': '{:,.2f}'}).applymap(define_color, subset=subset_color)


    st.plotly_chart(fig1, use_container_width=True)

    st.dataframe(aportes)

    st.plotly_chart(fig2, use_container_width=True)

    if classe_selec == 'TOTAL':
        st.plotly_chart(fig3, use_container_width=True)


def rentabilidade_diaria(data_base):


    # Análise da Rentabilidade

    data_hoje = date.today().strftime('%Y-%m-%d')

    # Evolução da Carteira

    df = investimentos.evolucao_diaria(conn)

    if data_hoje != data_base:
        df = df[df.data >= data_base]

    df['var_dia'] = df.saldo_total - (df.saldo_total.shift(1) + df.aporte_total)

    df['var_dia_total'] = df.saldo_total / (df.saldo_total.shift(1) + df.aporte_total)

    df['total_acum'] = df.var_dia_total.cumprod()

    df.total_acum = df.total_acum * 100


    # Bovespa
    sql = '''
        SELECT *
        FROM HistoricoBovespa
    '''
    bovespa = pd.read_sql(sql, conn)

    if data_hoje != data_base:
        bovespa = bovespa[bovespa.data >= data_base]

    bovespa['var_dia_total'] = bovespa.valor / bovespa.valor.shift(1) 

    bovespa['total_acum'] = bovespa.var_dia_total.cumprod()

    bovespa.total_acum = bovespa.total_acum * 100

    bovespa = bovespa.fillna(100)


    # Dolar
    sql = '''
        SELECT *
        FROM HistoricoDolar
    '''
    dolar = pd.read_sql(sql, conn)

    if data_hoje != data_base:
        dolar = dolar[dolar.data >= data_base]

    dolar['var_dia_total'] = dolar.dolar / dolar.dolar.shift(1) 

    dolar['total_acum'] = dolar.var_dia_total.cumprod()

    dolar.total_acum = dolar.total_acum * 100

    dolar = dolar.fillna(100)


    fig = go.Figure()

    fig = make_subplots(rows=1, cols=1, 
                    shared_xaxes=True,
                    vertical_spacing=0.1,
                    specs=([[{'secondary_y': True}]]))

    fig.add_trace(
        go.Scatter(x=df.data, y=df.total_acum, name='Rentabilidade',
                    hovertemplate='%{y:,.1f}%'))
    
    fig.add_trace(
        go.Scatter(x=bovespa.data, y=bovespa.total_acum, name='Bovespa',
                    hovertemplate='%{y:,.1f}%'))

    fig.add_trace(
        go.Scatter(x=dolar.data, y=dolar.total_acum, name='Dolar', visible='legendonly',
                    hovertemplate='%{y:,.1f}%'))

    fig.add_trace(
        go.Scatter(x=df.data, y=df.saldo_total, name='Carteira',
                    hovertemplate='R%{y:$,.2f}', visible='legendonly'), secondary_y=True)

    fig.add_trace(
        go.Scatter(x=bovespa.data, y=bovespa.valor, name='iBovespa',
                    hovertemplate='%{y:,.0f}', visible='legendonly'), secondary_y=True,)

    fig.add_trace(
        go.Scatter(x=dolar.data, y=dolar.dolar, name='Dolar',
                    hovertemplate='%{y:,.4f}', visible='legendonly'), secondary_y=True,)

    fig.update_layout(hovermode='x unified',
                      legend=dict(orientation='h',
                                  yanchor='bottom',
                                  y=1,
                                  xanchor='right',
                                  x=1))

    fig.update_traces(mode='lines+text')
    
    st.plotly_chart(fig, use_container_width=True)

    # Gera dataframe de exibição

    df['aporte_total_acum'] = df.aporte_total.cumsum()

    df = df.sort_values('data', ascending=False)

    df.data = df.data.dt.strftime('%d/%m/%Y')

    df = df.set_index('data')

    df.var_dia_total = (df.var_dia_total - 1) * 100

    df = df[['saldo_total', 'aporte_total_acum', 'var_dia', 'var_dia_total', 'total_acum',
             'saldo_acao', 'saldo_td', 'saldo_fundos', 'saldo_rf', 'aporte_total']]

    df.columns = ['Carteira', 'Aporte Acum', 'Var Dia', 'Var Dia %', 'Var Acum %',
                  'Ações', 'Tesouro Direto', 'Fundos', 'Renda Fixa', 'Aporte']

    df1 = df.style.format(
        thousands='.',
        decimal = ',',
        formatter={'Carteira': '{:,.2f}',
                   'Aporte Acum': '{:,.2f}',
                   'Var Dia': '{:,.2f}',
                   'Var Dia %': '{:,.2f}',
                   'Ações': '{:,.2f}',
                   'Tesouro Direto': '{:,.2f}',
                   'Fundos': '{:,.2f}',
                   'Renda Fixa': '{:,.2f}',
                   'Aporte': '{:,.2f}',
                   'Var Acum %': '{:,.1f}'}
        ).applymap(define_color, subset=['Var Dia', 'Var Dia %', 'Var Acum %', 'Aporte'])


    st.table(df1)


def rentabilidade_mensal(data_base):


    # Análise da Rentabilidade Mensal

    data_hoje = date.today().strftime('%Y-%m-%d')

    # Evolução da Carteira

    df = investimentos.evolucao_diaria(conn)

    if data_hoje != data_base:
        df = df[df.data >= data_base]

    # Agrupamento mensal
    df['data'] = df['data'] + MonthEnd(0)

    df = df.groupby(['data']).agg({'saldo_total': 'last',
                                   'saldo_acao': 'last',
                                   'saldo_td': 'last',
                                   'saldo_fundos': 'last',
                                   'saldo_rf': 'last',
                                   'aporte_total': 'sum',
                                   'aporte_acao': 'sum',
                                   'aporte_td': 'sum',
                                   'aporte_fundos': 'sum',
                                   'aporte_rf': 'sum',
                                   'dividendos': 'sum'}).reset_index()


    df['var_mes'] = df.saldo_total - (df.saldo_total.shift(1) + df.aporte_total)

    df['var_mes_total'] = df.saldo_total / (df.saldo_total.shift(1) + df.aporte_total)

    df['total_acum'] = df.var_mes_total.cumprod()

    df.total_acum = df.total_acum * 100


    # Bovespa
    sql = '''
        SELECT *
        FROM HistoricoBovespa
    '''
    bovespa = pd.read_sql(sql, conn)

    if data_hoje != data_base:
        bovespa = bovespa[bovespa.data >= data_base]

    # Agrupamento mensal
    bovespa['data'] = pd.to_datetime(bovespa['data'].str[:10]) + MonthEnd(0)

    bovespa = bovespa.groupby(['data']).agg({'valor': 'last'}).reset_index()

    bovespa['var_mes_total'] = bovespa.valor / bovespa.valor.shift(1) 

    bovespa['total_acum'] = bovespa.var_mes_total.cumprod()

    bovespa.total_acum = bovespa.total_acum * 100

    bovespa = bovespa.fillna(100)


    # Dolar
    sql = '''
        SELECT *
        FROM HistoricoDolar
    '''
    dolar = pd.read_sql(sql, conn)

    if data_hoje != data_base:
        dolar = dolar[dolar.data >= data_base]

    # Agrupamento mensal
    dolar['data'] = pd.to_datetime(dolar['data'].str[:10]) + MonthEnd(0)

    dolar = dolar.groupby(['data']).agg({'dolar': 'last'}).reset_index()

    dolar['var_mes_total'] = dolar.dolar / dolar.dolar.shift(1) 

    dolar['total_acum'] = dolar.var_mes_total.cumprod()

    dolar.total_acum = dolar.total_acum * 100

    dolar = dolar.fillna(100)


    fig = go.Figure()

    fig = make_subplots(rows=1, cols=1, 
                    shared_xaxes=True,
                    vertical_spacing=0.1,
                    specs=([[{'secondary_y': True}]]))

    fig.add_trace(
        go.Scatter(x=df.data, y=df.total_acum, name='Rentabilidade',
                    hovertemplate='%{y:,.1f}%'))
    
    fig.add_trace(
        go.Scatter(x=bovespa.data, y=bovespa.total_acum, name='Bovespa',
                    hovertemplate='%{y:,.1f}%'))

    fig.add_trace(
        go.Scatter(x=dolar.data, y=dolar.total_acum, name='Dolar', visible='legendonly',
                    hovertemplate='%{y:,.1f}%'))

    fig.add_trace(
        go.Scatter(x=df.data, y=df.saldo_total, name='Carteira',
                    hovertemplate='R%{y:$,.2f}', visible='legendonly'), secondary_y=True)

    fig.add_trace(
        go.Scatter(x=bovespa.data, y=bovespa.valor, name='iBovespa',
                    hovertemplate='%{y:,.0f}', visible='legendonly'), secondary_y=True,)

    fig.add_trace(
        go.Scatter(x=dolar.data, y=dolar.dolar, name='Dolar',
                    hovertemplate='%{y:,.4f}', visible='legendonly'), secondary_y=True,)

    fig.update_layout(hovermode='x unified',
                      legend=dict(orientation='h',
                                  yanchor='bottom',
                                  y=1,
                                  xanchor='right',
                                  x=1))

    fig.update_traces(mode='lines+text')
    
    st.plotly_chart(fig, use_container_width=True)

    # Gera dataframe de exibição

    df['aporte_total_acum'] = df.aporte_total.cumsum()

    df = df.sort_values('data', ascending=False)

    df.data = df.data.dt.strftime('%d/%m/%Y')

    df = df.set_index('data')

    df.var_mes_total = (df.var_mes_total - 1) * 100

    df = df[['saldo_total', 'aporte_total_acum', 'var_mes', 'var_mes_total', 'total_acum',
             'saldo_acao', 'saldo_td', 'saldo_fundos', 'saldo_rf', 'aporte_total']]

    df.columns = ['Carteira', 'Aporte Acum', 'Var Mês', 'Var Mês %', 'Var Acum %',
                  'Ações', 'Tesouro Direto', 'Fundos', 'Renda Fixa', 'Aporte']

    df = df.fillna(0)

    df1 = df.style.format(
        thousands='.',
        decimal = ',',
        formatter={'Carteira': '{:,.2f}',
                   'Aporte Acum': '{:,.2f}',
                   'Var Mês': '{:,.2f}',
                   'Var Mês %': '{:,.2f}',
                   'Ações': '{:,.2f}',
                   'Tesouro Direto': '{:,.2f}',
                   'Fundos': '{:,.2f}',
                   'Renda Fixa': '{:,.2f}',
                   'Aporte': '{:,.2f}',
                   'Var Acum %': '{:,.1f}'}
        ).applymap(define_color, subset=['Var Mês', 'Var Mês %', 'Var Acum %', 'Aporte'])


    st.table(df1)


def rentabilidade_ativo(df):


    rentab_total = df.rentab.sum()
    
    for classe in ['Ações', 'Tesouro Direto', 'Fundos']:

        with st.expander(classe, expanded=True):

            df_exib = df[df.classe == classe]

            del df_exib['classe']

            col1, col2 = st.columns(2)

            with col1:
        
                opcao_ativo = st.multiselect('Ativo:', df_exib.ticker.drop_duplicates().sort_values().tolist())
        
                if opcao_ativo != []:
                    df_exib = df_exib[df_exib.ticker.isin(opcao_ativo)]

            if classe == 'Ações':
                with col2:
                    opcao_tipo = st.multiselect('Tipo:', df_exib.tipo.drop_duplicates().sort_values().tolist())
                    if opcao_tipo != []:
                        df_exib = df_exib[df_exib.tipo.isin(opcao_tipo)]
        
            rentab_classe = df_exib.rentab.sum()
            
            df_exib.columns = ['Tipo', 'Ativo', 'Dividendos', 'Posição', 'Rentabilidade']

            df_exib = df_exib.set_index('Ativo')

            if classe in ['Tesouro Direto', 'Fundos']:
                df_exib = df_exib[['Posição', 'Rentabilidade']]


            df_exib = df_exib.style.format(
                    thousands='.',
                    decimal = ',',
                    formatter={'Dividendos': '{:,.2f}',
                            'Posição': '{:,.2f}',
                            'Rentabilidade': '{:,.2f}'}).applymap(define_color, subset=['Rentabilidade'])

            st.table(df_exib)

            st.write(f'Rentabilidade: R$ {rentab_classe:,.2f}'.replace(',', 'Z').replace('.', ',').replace('Z', '.'))

    st.subheader(f'Rentabilidade total: R$ {rentab_total:,.2f}'.replace(',', 'Z').replace('.', ',').replace('Z', '.'))


def imposto_renda(conn, data_base):


    # Ações

    with st.expander('Ações', expanded=True):

        custo_medio_ir, operacoes_ir = investimentos.imposto_renda_acao(conn, data_base)


        col1, col2 = st.columns([1.5, 1])

        with col1:
            opcao_ativo = st.multiselect('Ativo:', custo_medio_ir.ticker.drop_duplicates().sort_values().tolist())
        with col2:
            opcao_tipo = st.multiselect('Tipo:', custo_medio_ir.tipo.drop_duplicates().sort_values().tolist())

        operacoes_ir.data = pd.to_datetime(operacoes_ir.data).dt.strftime('%d/%m/%Y')

        if opcao_ativo != []:
            custo_medio_ir = custo_medio_ir[custo_medio_ir.ticker.isin(opcao_ativo)]
            operacoes_ir = operacoes_ir[operacoes_ir.ticker.isin(opcao_ativo)]

        if opcao_tipo != []:
            custo_medio_ir = custo_medio_ir[custo_medio_ir.tipo.isin(opcao_tipo)]
            operacoes_ir = operacoes_ir[operacoes_ir.tipo.isin(opcao_tipo)]

        custo_medio_ir.columns = ['Tipo', 'Ticker', 'Moeda', 'Qtd', 'Custo Médio R$', 'Custo Total R$']

        custo_medio_ir = custo_medio_ir.set_index(['Tipo', 'Ticker'])

        custo_medio_ir = custo_medio_ir.style.format(
                thousands='.',
                decimal = ',',
                formatter={'Qtd': '{:,.1f}',
                        'Custo Médio R$': '{:,.2f}',
                        'Custo Total R$': '{:,.2f}'})

        st.table(custo_medio_ir)


    with st.expander('Detalhamento Ações', expanded=False):

        operacoes_ir.columns = ['Tipo', 'Ticker', 'Data', 'C/V', 'Moeda', 'Qtd', 'Valor', 'Dolar', 'Valor R$', 'Qtde Acum']
        operacoes_ir = operacoes_ir[['Tipo', 'Ticker', 'Data', 'C/V', 'Moeda', 'Qtd', 'Qtde Acum', 'Valor', 'Dolar', 'Valor R$']]

        operacoes_ir = operacoes_ir.set_index(['Tipo', 'Ticker'])

        operacoes_ir = operacoes_ir.style.format(
                thousands='.',
                decimal = ',',
                formatter={'Qtd': '{:,.1f}',
                        'Valor': '{:,.2f}',
                        'Dolar': '{:,.4f}',
                        'Valor R$': '{:,.2f}',
                        'Qtde Acum': '{:,.1f}'})

        st.table(operacoes_ir)


    # Tesouro Direto

    with st.expander('Tesouro Direto', expanded=True):

        custo_medio_ir, operacoes_ir = investimentos.imposto_renda_td(conn, data_base)


        col1, col2 = st.columns([1.5, 1])

        with col1:
            opcao_ativo = st.multiselect('Título:', custo_medio_ir.nome.drop_duplicates().sort_values().tolist())

        operacoes_ir.data = pd.to_datetime(operacoes_ir.data).dt.strftime('%d/%m/%Y')

        if opcao_ativo != []:
            custo_medio_ir = custo_medio_ir[custo_medio_ir.nome.isin(opcao_ativo)]
            operacoes_ir = operacoes_ir[operacoes_ir.nome.isin(opcao_ativo)]

        custo_medio_ir.columns = ['Título', 'Qtd', 'Custo Médio R$', 'Custo Total R$']

        custo_medio_ir = custo_medio_ir.set_index(['Título'])

        custo_medio_ir = custo_medio_ir.style.format(
                thousands='.',
                decimal = ',',
                formatter={'Qtd': '{:,.2f}',
                        'Custo Médio R$': '{:,.2f}',
                        'Custo Total R$': '{:,.2f}'})

        st.table(custo_medio_ir)


    with st.expander('Detalhamento Tesouro Direto', expanded=False):

        operacoes_ir.columns = ['Título', 'Data', 'C/V', 'Qtd', 'Valor', 'Qtde Acum']
        operacoes_ir = operacoes_ir[['Título', 'Data', 'C/V', 'Qtd', 'Qtde Acum', 'Valor']]

        operacoes_ir = operacoes_ir.set_index(['Título'])

        operacoes_ir = operacoes_ir.style.format(
                thousands='.',
                decimal = ',',
                formatter={'Qtd': '{:,.2f}',
                        'Valor': '{:,.2f}',
                        'Qtde Acum': '{:,.2f}'})

        st.table(operacoes_ir)


    # Fundos

    with st.expander('Fundos', expanded=True):

        custo_medio_ir, operacoes_ir = investimentos.imposto_renda_fundos(conn, data_base)


        col1, col2 = st.columns([1.5, 1])

        with col1:
            opcao_ativo = st.multiselect('Título:', custo_medio_ir.nome.drop_duplicates().sort_values().tolist())

        operacoes_ir.data = pd.to_datetime(operacoes_ir.data).dt.strftime('%d/%m/%Y')

        if opcao_ativo != []:
            custo_medio_ir = custo_medio_ir[custo_medio_ir.nome.isin(opcao_ativo)]
            operacoes_ir = operacoes_ir[operacoes_ir.nome.isin(opcao_ativo)]

        custo_medio_ir.columns = ['Título', 'Qtd', 'Custo Médio R$', 'Custo Total R$']

        custo_medio_ir = custo_medio_ir.set_index(['Título'])

        custo_medio_ir = custo_medio_ir.style.format(
                thousands='.',
                decimal = ',',
                formatter={'Qtd': '{:,.6f}',
                        'Custo Médio R$': '{:,.6f}',
                        'Custo Total R$': '{:,.2f}'})

        st.table(custo_medio_ir)


    with st.expander('Detalhamento Fundos', expanded=False):

        operacoes_ir.columns = ['Título', 'Data', 'C/V', 'Qtd', 'Valor', 'Qtde Acum']
        operacoes_ir = operacoes_ir[['Título', 'Data', 'C/V', 'Qtd', 'Qtde Acum', 'Valor']]

        operacoes_ir = operacoes_ir.set_index(['Título'])

        operacoes_ir = operacoes_ir.style.format(
                thousands='.',
                decimal = ',',
                formatter={'Qtd': '{:,.6f}',
                        'Valor': '{:,.2f}',
                        'Qtde Acum': '{:,.6f}'})

        st.table(operacoes_ir)


    # Dividendos

    with st.expander('Proventos/Dividendos', expanded=True):

        divid_ir, operacoes_ir, ir_mensal = investimentos.imposto_renda_dividendos(conn, data_base)


        col1, col2, col3 = st.columns([1.5, 1, 1])

        with col1:
            opcao_ativo = st.multiselect('Título:', divid_ir.prov_ticker.drop_duplicates().sort_values().tolist())

        with col2:
            opcao_tipo = st.multiselect('Tipo:', divid_ir.prov_tipo.drop_duplicates().sort_values().tolist())

        with col3:
            opcao_forma = st.multiselect('Forma:', divid_ir.prov_forma.drop_duplicates().sort_values().tolist())

        operacoes_ir.prov_data = pd.to_datetime(operacoes_ir.prov_data).dt.strftime('%d/%m/%Y')
        ir_mensal.prov_data = pd.to_datetime(ir_mensal.prov_data).dt.strftime('%d/%m/%Y')

        if opcao_ativo != []:
            divid_ir = divid_ir[divid_ir.prov_ticker.isin(opcao_ativo)]
            operacoes_ir = operacoes_ir[operacoes_ir.prov_ticker.isin(opcao_ativo)]

        if opcao_tipo != []:
            divid_ir = divid_ir[divid_ir.prov_tipo.isin(opcao_tipo)]
            operacoes_ir = operacoes_ir[operacoes_ir.prov_tipo.isin(opcao_tipo)]

        if opcao_forma != []:
            divid_ir = divid_ir[divid_ir.prov_forma.isin(opcao_forma)]
            operacoes_ir = operacoes_ir[operacoes_ir.prov_forma.isin(opcao_forma)]

        total_dividendos = divid_ir.dividendo.sum()
        total_ir_reais = divid_ir.ir_real.sum()
        
        divid_ir.columns = ['Tipo', 'Ticker', 'Nome', 'CNPJ', 'Forma', 'Moeda', 'Bruto', 'IR', 'Valor', 'IR R$', 'Proventos R$']

        divid_ir = divid_ir.set_index(['Tipo'])

        divid_ir = divid_ir.style.format(
                thousands='.',
                decimal = ',',
                formatter={'Bruto': '{:,.2f}',
                        'IR': '{:,.2f}',
                        'Valor': '{:,.2f}',
                        'IR R$': '{:,.2f}',
                        'Proventos R$': '{:,.2f}'})

        st.write(f'TOTAL Proventos: R$ {total_dividendos:,.2f}'.replace(',', 'x').replace('.', ',').replace('x', '.'))

        st.write(f'TOTAL IR: R$ {total_ir_reais:,.2f}'.replace(',', 'x').replace('.', ',').replace('x', '.'))

        st.table(divid_ir)


    with st.expander('Detalhamento de Proventos/Dividendos', expanded=False):

        operacoes_ir.columns = ['Data', 'Tipo', 'Ticker', 'Forma', 'Moeda', 'Bruto', 'IR', 'Valor', 'Dolar', 'IR R$', 'Proventos R$']

        operacoes_ir = operacoes_ir.set_index(['Tipo', 'Ticker'])

        operacoes_ir = operacoes_ir.style.format(
                thousands='.',
                decimal = ',',
                formatter={'Dolar': '{:,.4f}',
                        'Bruto': '{:,.2f}',
                        'IR': '{:,.2f}',
                        'Valor': '{:,.2f}',
                        'IR R$': '{:,.2f}',
                        'Proventos R$': '{:,.2f}'})

        st.table(operacoes_ir)

        # Imposto de Renda

    with st.expander('Imposto de Renda sobre Dividendos', expanded=True):

        total_ir_reais = ir_mensal.ir_real.sum()

        ir_mensal.columns = ['Data', 'IR R$']

        ir_mensal = ir_mensal.set_index(['Data'])

        ir_mensal = ir_mensal.style.format(
                thousands='.',
                decimal = ',',
                formatter={'IR R$': '{:,.2f}'})

        st.table(ir_mensal)

        st.write(f'TOTAL IR sobre dividendos: R$ {total_ir_reais:,.2f}'.replace(',', 'x').replace('.', ',').replace('x', '.'))


def grafico_cotacoes(conn, data_base):


    # Ações

    sql = '''
        SELECT tipo, ticker, data, sum(acum) as qtd, min(close) as cot,
                sum(valor) as valor, sum(saldo) as saldo
        FROM PosicaoDiariaAcao
        GROUP BY tipo, ticker, data
        ORDER BY tipo, saldo DESC, ticker
    '''

    df = pd.read_sql(sql, conn)

    df.data = df.data.str[:10]
    df = df[df.data == data_base]

    preco_medio = investimentos.preco_medio_acao(conn)

    df = df.merge(preco_medio, on='ticker', how='left')
    
    # Cotações
    sql = '''
        SELECT data, ticker, close
        FROM PosicaoDiariaAcao
    '''

    df_cot = pd.read_sql(sql, conn)

    df_cot.columns = ['data', 'nome', 'valor']


    for tipo in df.tipo.drop_duplicates().tolist():

        with st.expander(tipo, expanded=False):
        
            df_aux = df[df.tipo == tipo]

            for nome in df_aux.ticker.drop_duplicates():

                fig1 = go.Figure()


                df_exib = df_cot[df_cot.nome == nome].copy()

                fig1.add_trace(
                    go.Scatter(x=df_exib.data, y=df_exib.valor, name=nome, hovertemplate='%{y:,.2f}'))

                if df[df.ticker == nome].shape[0] > 0:
                
                    preco_medio = df[df.ticker == nome]['preco_medio'].iloc[0]
                    
                    fig1.add_hline(y=preco_medio, line_color='red')
                
                fig1.update_layout(title=nome,
                                   hovermode='x unified',
                                   legend=dict(orientation='h',
                                            yanchor='bottom',
                                            y=1,
                                            xanchor='right',
                                            x=1))

                st.plotly_chart(fig1, use_container_width=True)


    # Tesouro Direto

    sql = '''
        SELECT nome, data, sum(qtd_acum) as qtd, min(preco_unit) as cot, sum(saldo) as saldo
        FROM PosicaoDiariaTD
        GROUP BY data, nome
        ORDER BY data, saldo DESC
    '''

    df = pd.read_sql(sql, conn)

    df.data = df.data.str[:10]
    df = df[df.data == data_base]

    preco_medio = investimentos.preco_medio_td(conn)

    df = df.merge(preco_medio, on='nome', how='left')
    
    # Cotações
    sql = '''
        SELECT data, nome, preco_unit
        FROM PosicaoDiariaTD
    '''

    df_cot = pd.read_sql(sql, conn)

    df_cot.columns = ['data', 'nome', 'valor']


    with st.expander('Tesouro Direto', expanded=False):
        
            for nome in df.nome.drop_duplicates():

                fig1 = go.Figure()


                df_exib = df_cot[df_cot.nome == nome].copy()

                fig1.add_trace(
                    go.Scatter(x=df_exib.data, y=df_exib.valor, name=nome, hovertemplate='%{y:,.2f}'))

                if df[df.nome == nome].shape[0] > 0:
                
                    preco_medio = df[df.nome == nome]['preco_medio'].iloc[0]
                    
                    fig1.add_hline(y=preco_medio, line_color='red')
                
                fig1.update_layout(title=nome,
                                   hovermode='x unified',
                                   legend=dict(orientation='h',
                                            yanchor='bottom',
                                            y=1,
                                            xanchor='right',
                                            x=1))

                st.plotly_chart(fig1, use_container_width=True)


    # Fundos

    sql = '''
        SELECT nome, data, sum(qtd_acum) as qtd, min(fundo_vlr_quota) as cot, sum(saldo) as saldo
        FROM PosicaoDiariaFundos
        GROUP BY data, nome
        ORDER BY data, saldo DESC
    '''

    df = pd.read_sql(sql, conn)

    df.data = df.data.str[:10]
    df = df[df.data == data_base]

    preco_medio = investimentos.preco_medio_fundos(conn)

    df = df.merge(preco_medio, on='nome', how='left')
    
    # Cotações
    sql = '''
        SELECT data, nome, fundo_vlr_quota
        FROM PosicaoDiariaFundos
    '''

    df_cot = pd.read_sql(sql, conn)

    df_cot.columns = ['data', 'nome', 'valor']


    with st.expander('Fundos', expanded=False):
        
            for nome in df.nome.drop_duplicates():

                fig1 = go.Figure()


                df_exib = df_cot[df_cot.nome == nome].copy()

                fig1.add_trace(
                    go.Scatter(x=df_exib.data, y=df_exib.valor, name=nome, hovertemplate='%{y:,.2f}'))

                if df[df.nome == nome].shape[0] > 0:
                
                    preco_medio = df[df.nome == nome]['preco_medio'].iloc[0]
                    
                    fig1.add_hline(y=preco_medio, line_color='red')
                
                fig1.update_layout(title=nome,
                                   hovermode='x unified',
                                   legend=dict(orientation='h',
                                            yanchor='bottom',
                                            y=1,
                                            xanchor='right',
                                            x=1))

                st.plotly_chart(fig1, use_container_width=True)


def acoes_interesse_segmento_graficos(conn):


    # Ações de Interesse
    sql = '''
        SELECT *
        FROM AcoesInteresse
        WHERE favorito <> '.'
        ORDER BY tipo, ordem, ticker
    '''
    acoes_interesse = pd.read_sql(sql, conn)
    acoes_interesse['ticker_yahoo'] = acoes_interesse.ticker
    acoes_interesse.loc[~acoes_interesse.tipo.isin(['ETF', 'REIT', 'Stock']), 'ticker_yahoo'] = acoes_interesse.ticker + '.SA'

    # Cotações
    sql = '''
        SELECT *
        FROM CotacaoAcaoInteresse
    '''
    cotacao = pd.read_sql(sql, conn)


    for tipo in acoes_interesse.tipo.drop_duplicates().tolist():

        for segmento in acoes_interesse.segmento.drop_duplicates().tolist():

            df_aux = acoes_interesse[(acoes_interesse.tipo == tipo) & (acoes_interesse.segmento == segmento)]

            with st.expander(f'{tipo} - {segmento}', expanded=False):

                for ticker, ticker_yahoo, nome in zip(df_aux.ticker.tolist(), df_aux.ticker_yahoo.tolist(), df_aux.nome.tolist()):

                    df_cot = cotacao[cotacao.ticker_yahoo == ticker_yahoo]

                    fig1 = go.Figure()

                    df_exib = df_cot[df_cot.ticker_yahoo == ticker_yahoo].copy()

                    fig1.add_trace(
                        go.Scatter(x=df_exib.data, y=df_exib.close, name=ticker, hovertemplate='%{y:,.2f}'))


                    cot_atu = df_cot.iloc[-1, 2]
                    cot_ant = df_cot.iloc[-2, 2]
                    var = cot_atu / cot_ant - 1

                    if tipo in ['ETF', 'REIT', 'Stock']:
                        moeda = 'US$'
                    else:
                        moeda = 'R$'


                    titulo = f'<b>{ticker}  ({moeda} {cot_atu:,.2f})  {var:,.2%} - {nome}</b>'.replace(',', 'xxx').replace('.', ',').replace('xxx', '.')

                    fig1.update_layout(title=titulo,
                                        hovermode='x unified',
                                        legend=dict(orientation='h',
                                                yanchor='bottom',
                                                y=1,
                                                xanchor='right',
                                                x=1))

                    st.plotly_chart(fig1, use_container_width=True)


def acoes_interesse_segmento_tabela(conn, data_base):


    data_ant = (datetime.strptime(data_base, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

    # 1 ano antes
    data_12m = (datetime.strptime(data_base, '%Y-%m-%d') - timedelta(days=365)).strftime('%Y-%m-%d')

    # Primeiro dia do mês base
    data_dia1 = data_base[:8] + '01'

    # Ações de Interesse
    sql = '''
        SELECT *
        FROM AcoesInteresse
        WHERE favorito <> '.'
        ORDER BY tipo, ordem, ticker
    '''
    acoes_interesse = pd.read_sql(sql, conn)
    acoes_interesse['ticker_yahoo'] = acoes_interesse.ticker
    acoes_interesse.loc[~acoes_interesse.tipo.isin(['ETF', 'REIT', 'Stock']), 'ticker_yahoo'] = acoes_interesse.ticker + '.SA'

    # Cotações
    sql = '''
        SELECT *
        FROM CotacaoAcaoInteresse
    '''
    cotacao = pd.read_sql(sql, conn)
    cotacao.data = pd.to_datetime(cotacao.data)

    cotacao = cotacao[cotacao.data.isin([data_12m, data_dia1, data_ant, data_base])]

    cotacao = pd.pivot_table(cotacao, values=['close', 'volume'], index=['ticker', 'ticker_yahoo'], columns=['data']).reset_index()

    cotacao['close_12'] = cotacao[('close', data_12m)]
    cotacao['close_dia1'] = cotacao[('close', data_dia1)]
    cotacao['close_ant'] = cotacao[('close', data_ant)]
    cotacao['close_atu'] = cotacao[('close', data_base)]

    cotacao['volume_atu'] = cotacao[('volume', data_base)]
    cotacao['volume_ant'] = cotacao[('volume', data_ant)]

    cotacao = cotacao[['ticker', 'ticker_yahoo', 'close_12', 'close_dia1', 'close_ant', 'close_atu', 'volume_ant', 'volume_atu']]
    cotacao.columns = ['ticker', 'ticker_yahoo', 'close_12', 'close_dia1', 'close_ant', 'close_atu', 'volume_ant', 'volume_atu']

    cotacao.loc[cotacao.volume_atu == 0, 'volume_atu'] = cotacao.volume_ant
    del cotacao['volume_ant']

    cotacao = cotacao.fillna(0)

    cotacao = cotacao.merge(acoes_interesse, on=['ticker', 'ticker_yahoo'])

    cotacao.columns = ['ticker', 'ticker_yahoo', 'cot_12', 'cot_d1', 'cot_ant', 'cot_atu', 'volume', 'tipo', 'segmento', 'ordem', 'nome', 'favorito']

    cotacao['var_perc'] = cotacao.cot_atu / cotacao.cot_ant - 1
    cotacao['var_perc_12'] = cotacao.cot_atu / cotacao.cot_12 - 1
    cotacao['var_perc_mes'] = cotacao.cot_atu / cotacao.cot_d1 - 1

    cotacao = cotacao.sort_values(by=['tipo', 'ordem', 'volume', 'nome'],
                                ascending=[True, True, False, True])


    for tipo in cotacao.tipo.drop_duplicates().tolist():

        df_aux = cotacao[(cotacao.tipo == tipo)]

        for ordem, segmento in zip(df_aux.ordem.drop_duplicates().tolist(), df_aux.segmento.drop_duplicates().tolist()):

            with st.expander(f'{tipo} - {segmento}', expanded=True):
            
                df = df_aux[df_aux.segmento == segmento]

                df = df[['ticker', 'nome', 'cot_ant', 'cot_atu', 'var_perc_12', 'var_perc_mes', 'var_perc', 'volume']]

                df.reset_index(inplace=True, drop=True) 

                if tipo == 'Ação':
                    df = df.set_index(['nome', 'ticker'])
                else:
                    df = df.set_index(['ticker'])
                    del df['nome']

                df.columns = ['Ant', 'Cot', 'Var 12m', 'Var Mês', 'Var Dia', 'Volume']

                df = df.style.format(thousands='.',
                                    decimal = ',',
                                    formatter={'Cot': '{:,.2f}',
                                            'Ant': '{:,.2f}',
                                            'Var Dia': '{:,.2%}',
                                            'Var 12m': '{:,.2%}',
                                            'Var Mês': '{:,.2%}',
                                            'Volume': '{:,.0f}M'}
                                    ).applymap(define_color, subset=['Var Dia', 'Var 12m', 'Var Mês'])

                st.table(df)


def acoes_favoritas(conn, data_base):


    data_ant = (datetime.strptime(data_base, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

    # 1 ano antes
    data_12m = (datetime.strptime(data_base, '%Y-%m-%d') - timedelta(days=365)).strftime('%Y-%m-%d')

    # Primeiro dia do mês base
    data_dia1 = data_base[:8] + '01'

    # Ações de Interesse
    sql = '''
        SELECT *
        FROM AcoesInteresse
        WHERE favorito = 'Top'
    '''
    acoes_interesse = pd.read_sql(sql, conn)
    acoes_interesse['ticker_yahoo'] = acoes_interesse.ticker
    acoes_interesse.loc[~acoes_interesse.tipo.isin(['ETF', 'REIT', 'Stock']), 'ticker_yahoo'] = acoes_interesse.ticker + '.SA'

    # Cotações
    sql = '''
        SELECT *
        FROM CotacaoAcaoInteresse
    '''
    cotacao = pd.read_sql(sql, conn)
    cotacao.data = pd.to_datetime(cotacao.data)

    cotacao = cotacao[cotacao.data.isin([data_12m, data_dia1, data_ant, data_base])]

    cotacao = pd.pivot_table(cotacao, values=['close', 'volume'], index=['ticker', 'ticker_yahoo'], columns=['data']).reset_index()

    cotacao['close_12'] = cotacao[('close', data_12m)]
    cotacao['close_dia1'] = cotacao[('close', data_dia1)]
    cotacao['close_ant'] = cotacao[('close', data_ant)]
    cotacao['close_atu'] = cotacao[('close', data_base)]

    cotacao['volume_atu'] = cotacao[('volume', data_base)]
    cotacao['volume_ant'] = cotacao[('volume', data_ant)]

    cotacao = cotacao[['ticker', 'ticker_yahoo', 'close_12', 'close_dia1', 'close_ant', 'close_atu', 'volume_ant', 'volume_atu']]
    cotacao.columns = ['ticker', 'ticker_yahoo', 'close_12', 'close_dia1', 'close_ant', 'close_atu', 'volume_ant', 'volume_atu']

    cotacao.loc[cotacao.volume_atu == 0, 'volume_atu'] = cotacao.volume_ant
    del cotacao['volume_ant']

    cotacao = cotacao.fillna(0)

    cotacao = cotacao.merge(acoes_interesse, on=['ticker', 'ticker_yahoo'])

    cotacao.columns = ['ticker', 'ticker_yahoo', 'cot_12', 'cot_d1', 'cot_ant', 'cot_atu', 'volume', 'tipo', 'segmento', 'ordem', 'nome', 'favorito']

    cotacao['var_perc'] = cotacao.cot_atu / cotacao.cot_ant - 1
    cotacao['var_perc_12'] = cotacao.cot_atu / cotacao.cot_12 - 1
    cotacao['var_perc_mes'] = cotacao.cot_atu / cotacao.cot_d1 - 1

    cotacao = cotacao.sort_values(by=['tipo', 'volume', 'nome'],
                                ascending=[True, False, True])


    for tipo in cotacao.tipo.drop_duplicates().tolist():

        with st.expander(f'{tipo}', expanded=True):
            
            df = cotacao[(cotacao.tipo == tipo)]

            df = df[['ticker', 'nome', 'cot_ant', 'cot_atu', 'var_perc_12', 'var_perc_mes', 'var_perc', 'volume']]

            df.reset_index(inplace=True, drop=True) 

            if tipo == 'Ação':
                df = df.set_index(['nome', 'ticker'])
            else:
                df = df.set_index(['ticker'])
                del df['nome']

            df.columns = ['Ant', 'Cot', 'Var 12m', 'Var Mês', 'Var Dia', 'Volume']

            df = df.style.format(thousands='.',
                                decimal = ',',
                                formatter={'Cot': '{:,.2f}',
                                        'Ant': '{:,.2f}',
                                        'Var Dia': '{:,.2%}',
                                        'Var 12m': '{:,.2%}',
                                        'Var Mês': '{:,.2%}',
                                        'Volume': '{:,.0f}M'}
                                ).applymap(define_color, subset=['Var Dia', 'Var 12m', 'Var Mês'])

            st.table(df)


def maiores_altas_baixas(conn, data_base):


    data_ant = (datetime.strptime(data_base, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

    # 1 ano antes
    data_12m = (datetime.strptime(data_base, '%Y-%m-%d') - timedelta(days=365)).strftime('%Y-%m-%d')

    # Primeiro dia do mês base
    data_dia1 = data_base[:8] + '01'

    # Ações de Interesse
    sql = '''
        SELECT *
        FROM AcoesInteresse
        WHERE favorito <> '.'
    '''
    acoes_interesse = pd.read_sql(sql, conn)
    acoes_interesse['ticker_yahoo'] = acoes_interesse.ticker
    acoes_interesse.loc[~acoes_interesse.tipo.isin(['ETF', 'REIT', 'Stock']), 'ticker_yahoo'] = acoes_interesse.ticker + '.SA'

    # Cotações
    sql = '''
        SELECT *
        FROM CotacaoAcaoInteresse
    '''
    cotacao = pd.read_sql(sql, conn)
    cotacao.data = pd.to_datetime(cotacao.data)

    cotacao = cotacao[cotacao.data.isin([data_12m, data_dia1, data_ant, data_base])]

    cotacao = pd.pivot_table(cotacao, values=['close', 'volume'], index=['ticker', 'ticker_yahoo'], columns=['data']).reset_index()

    cotacao['close_12'] = cotacao[('close', data_12m)]
    cotacao['close_dia1'] = cotacao[('close', data_dia1)]
    cotacao['close_ant'] = cotacao[('close', data_ant)]
    cotacao['close_atu'] = cotacao[('close', data_base)]

    cotacao['volume_atu'] = cotacao[('volume', data_base)]
    cotacao['volume_ant'] = cotacao[('volume', data_ant)]

    cotacao = cotacao[['ticker', 'ticker_yahoo', 'close_12', 'close_dia1', 'close_ant', 'close_atu', 'volume_ant', 'volume_atu']]
    cotacao.columns = ['ticker', 'ticker_yahoo', 'close_12', 'close_dia1', 'close_ant', 'close_atu', 'volume_ant', 'volume_atu']

    cotacao.loc[cotacao.volume_atu == 0, 'volume_atu'] = cotacao.volume_ant
    del cotacao['volume_ant']

    cotacao = cotacao.fillna(0)

    cotacao = cotacao.merge(acoes_interesse, on=['ticker', 'ticker_yahoo'])

    cotacao.columns = ['ticker', 'ticker_yahoo', 'cot_12', 'cot_d1', 'cot_ant', 'cot_atu', 'volume', 'tipo', 'segmento', 'ordem', 'nome', 'favorito']

    cotacao['var_perc'] = cotacao.cot_atu / cotacao.cot_ant - 1
    cotacao['var_perc_12'] = cotacao.cot_atu / cotacao.cot_12 - 1
    cotacao['var_perc_mes'] = cotacao.cot_atu / cotacao.cot_d1 - 1

    cotacao = cotacao.sort_values(by=['tipo', 'var_perc', 'nome'],
                                ascending=[True, False, True])


    for tipo in cotacao.tipo.drop_duplicates().tolist():

        with st.expander(f'{tipo}', expanded=True):
            
            df = cotacao[(cotacao.tipo == tipo) & (cotacao.var_perc > 0)].head(10)

            df = df[['ticker', 'nome', 'cot_ant', 'cot_atu', 'var_perc']]

            df.reset_index(inplace=True, drop=True) 

            if tipo == 'Ação':
                df = df.set_index(['nome', 'ticker'])
            else:
                df = df.set_index(['ticker'])
                del df['nome']

            df.columns = ['Ant', 'Cot', 'Var']

            df = df.style.format(thousands='.',
                                decimal = ',',
                                formatter={'Cot': '{:,.2f}',
                                        'Ant': '{:,.2f}',
                                        'Var': '{:,.2%}'}
                                ).applymap(define_color, subset=['Var'])

            st.table(df)


            df = cotacao[(cotacao.tipo == tipo) & (cotacao.var_perc < 0)].tail(10)
            df = df.sort_values(by=['var_perc', 'nome'])

            df = df[['ticker', 'nome', 'cot_ant', 'cot_atu', 'var_perc']]

            df.reset_index(inplace=True, drop=True) 

            if tipo == 'Ação':
                df = df.set_index(['nome', 'ticker'])
            else:
                df = df.set_index(['ticker'])
                del df['nome']

            df.columns = ['Ant', 'Cot', 'Var']

            df = df.style.format(thousands='.',
                                decimal = ',',
                                formatter={'Cot': '{:,.2f}',
                                        'Ant': '{:,.2f}',
                                        'Var': '{:,.2%}'}
                                ).applymap(define_color, subset=['Var'])

            st.table(df)


# Configuração da página
st.set_page_config(
    layout='wide',
    page_icon='app.jpg',
    page_title='Investimentos',
    initial_sidebar_state='expanded')


dbname = 'INVESTIMENTOS.db'

conn = sqlite3.connect(dbname)

data_hoje = datetime.today().strftime('%Y-%m-%d')


# Procedimentos para primeira execução do dia

procedimento_inicial(conn)


with st.sidebar:

    tipo = st.selectbox(
        label = 'INVESTIMENTOS',
        options = [
            'Resultado do Dia',
            'Carteira',
            'Gráficos de Cotações',
            'Ações por Segmento',
            'Ações Favoritas',
            'Maiores Altas e Baixas',
            'Posição por Classe',
            'Posição por Estratégia',
            'Posição por Corretora',
            'Liquidez',
            'Rentabilidade Diária',
            'Rentabilidade Mensal',
            'Rentabilidade Gráfica',
            'Rentabilidade por Ativo',
            'Operações',
            'Aportes',
            'Proventos/Dividendos',
            'Imposto de Renda',
            'Ações de Interesse Gráficos'])

    data_max = datetime.today()
    data_min = datetime.today() - timedelta(days=365 * 6)
    data_base = st.date_input('Data base:', value=data_max, min_value=data_min, max_value=data_max).strftime('%Y-%m-%d')

    col1, col2 = st.columns(2)

    with col1:
    
        if st.button('Atualiza Cotações'):
            with st.spinner('Atualizando Cotações...'):
                carteira, dict_dolar_ibov = investimentos.apuracao_resultado_acao_dia(conn, data_base, acessa_YFinance=True)
    
    with col2:
    
        if st.button('Atualiza Ações Interesse'):
            with st.spinner('Atualizando Cotações...'):
                investimentos.cotacao_acoes_interesse(conn)

    # Importação de operações das planilhas Excel
    
    with st.form('form1', clear_on_submit=True):
        upload_files = st.file_uploader('Importar operações', accept_multiple_files=True)
        submitted = st.form_submit_button('Processar...')

    if submitted and upload_files is not None:
        with st.spinner('Processando...'):
            for f in upload_files:
                print('Arquivo selecionado:', f.name)
                investimentos.importa_operacoes(conn, f.name, f)
            atualiza_base(conn)
            st.write('Arquivos importados!')

    if st.button('Download do Banco de Dados'):
        tipo = 'Download do Banco de Dados'


if tipo == 'Resultado do Dia':
    investimentos.apuracao_resultado_acao_dia(conn, data_base, acessa_YFinance=False)
    df = investimentos.apuracao_posicao_diaria(conn, data_base)
    resultado_do_dia(conn, data_base)

if tipo == 'Carteira':
    #df = investimentos.apuracao_posicao_diaria(conn, data_base)
    calcula_carteira(data_base)

if tipo == 'Posição por Classe':
    df = investimentos.apuracao_posicao_diaria(conn, data_base)
    posicao_por_classe(df, data_base)

if tipo == 'Posição por Estratégia':
    df = investimentos.apuracao_posicao_diaria(conn, data_base)
    posicao_por_estrategia(df, data_base)

if tipo == 'Posição por Corretora':
    df = investimentos.apuracao_posicao_diaria(conn, data_base)
    posicao_por_corretora(df, data_base)

if tipo == 'Liquidez':
    df = investimentos.apuracao_posicao_diaria(conn, data_base)
    liquidez(df, data_base)

if tipo == 'Rentabilidade Diária':
    rentabilidade_diaria(data_base)

if tipo == 'Rentabilidade Mensal':
    rentabilidade_mensal(data_base)

if tipo == 'Rentabilidade Gráfica':
    rentabilidade_grafica(conn, data_base)

if tipo == 'Rentabilidade por Ativo':
    df = investimentos.rentabilidade_por_ativo(conn, data_base)
    rentabilidade_ativo(df)

if tipo == 'Operações':
    with st.expander('Ações', expanded=True):
        df = investimentos.operacoes_acao_por_dia(conn)
        operacoes_acao(df)
    with st.expander('Tesouro Direto'):
        df = investimentos.operacoes_td_por_dia(conn)
        operacoes_td(df)
    with st.expander('Fundos'):
        df = investimentos.operacoes_fundos_por_dia(conn)
        operacoes_fundos(df)
    with st.expander('Renda Fixa'):
        df = investimentos.operacoes_rf_por_dia(conn)
        operacoes_rf(df)

if tipo == 'Download do Banco de Dados':
    st.subheader('Backup do Banco de Dados')
    with st.spinner('Gerando base para exportar...'):
            f = ''
            for line in conn.iterdump():
                f = f + line
            st.download_button('Download', f, 'INVESTIMENTO_Backup.sql', 'text/csv')

if tipo == 'Aportes':
    aportes(conn, data_base)

if tipo == 'Proventos/Dividendos':
    proventos()

if tipo == 'Imposto de Renda':
    imposto_renda(conn, data_base)

if tipo == 'Gráficos de Cotações':
    grafico_cotacoes(conn, data_base)

if tipo == 'Ações por Segmento':
    acoes_interesse_segmento_tabela(conn, data_base)

if tipo == 'Ações de Interesse Gráficos':
    acoes_interesse_segmento_graficos(conn)

if tipo == 'Ações Favoritas':
    acoes_favoritas(conn, data_base)

if tipo == 'Maiores Altas e Baixas':
    maiores_altas_baixas(conn, data_base)

import cowsay
import schedule
import time
import pandas as pd
from datetime import datetime as dt, timedelta as delta
from utils import checking_valid_dates as check_dt
import dataset
from console_logging.console import Console
import os 

console = Console()
url_db = os.getenv("DATABASE_URL")

db = dataset.connect(url_db)
table = db['tbl_baseboletim']

endpoint = "https://ptax.bcb.gov.br/ptax_internet/consultaBoletim.do?method=gerarCSVFechamentoMoedaNoPeriodo&ChkMoeda=61&DATAINI={}&DATAFIM={}"
 
endpoint_euro = "https://ptax.bcb.gov.br/ptax_internet/consultaBoletim.do?method=gerarCSVFechamentoMoedaNoPeriodo&ChkMoeda=222&DATAINI={}&DATAFIM={}"

def extracao_base_cotacao(moeda,dt_inicial,dt_final):
    console.log("MOEDA : "+ moeda)
    console.info("[E] - Extraindo bados")
    if moeda=="USD":
      url = endpoint.format(dt_inicial,dt_final)
      print(url)
      df = pd.read_csv(url,sep=";",names=['Data', 'Código da Moeda','Tipo',"Símbolo da Moeda",'USD_Compra','USD_Venda','Var','Var2'])
      df = df[['Data', 'USD_Compra','USD_Venda']]
      return df 
    if moeda == "EUR":
      url = endpoint_euro.format(dt_inicial,dt_final)
      df = pd.read_csv(url,sep=";",names=['Data', 'Código da Moeda','Tipo',"Símbolo da Moeda",'EUR_Compra','EUR_Venda','Par_Compra','Par_Venda'])
      df = df[['Data','EUR_Compra','EUR_Venda']]
      return df 
    
def transformacao_base_cotacao(df):
    console.info("[T] - Normalizando data")
    df['Data'] = df['Data'].apply(check_dt)
    return df 

def load_base_cotacao(moeda,df):
    console.info("[L] - Carregando Base to CSV")
    df.to_csv("final_{}.csv".format(moeda),index=False)
    console.success("Base final_{}.csv".format(moeda))

def job():
    cowsay.cow('É Nois que AVOAA POHA ')
    # regra de busca de dados - dia atual - 10
    dt_inicial =  str(dt.now().date() - delta(days=20))
    dt_final =    str(dt.now().date()) 

    dt_inicial = dt.strptime(dt_inicial, "%Y-%m-%d").strftime("%d/%m/%Y")
    dt_final = dt.strptime(dt_final, "%Y-%m-%d").strftime("%d/%m/%Y")
    
    console.info("PERIODO: {} {}".format(dt_inicial,dt_final))
    ## ETL Dolar Base 
    output = extracao_base_cotacao(moeda="USD",
                                   dt_inicial=dt_inicial,
                                   dt_final=dt_final)

    base = transformacao_base_cotacao(output)
    load_base_cotacao(moeda="USD", df=base)

    ## ETL Euro Base
    output = extracao_base_cotacao(moeda="EUR",
                                   dt_inicial=dt_inicial,
                                   dt_final=dt_final)

    base = transformacao_base_cotacao(output)
    load_base_cotacao(moeda="EUR", df=base)
    
    ## concat bases 
    console.info("[T] - CONCAT BASES [DATA | USD | EUR]")
    base_usd = pd.read_csv("final_USD.csv")
    base_eur = pd.read_csv("final_EUR.csv")

    base_eur.drop(['Data'],inplace=True,axis=1)

    final_base = pd.concat([base_usd,base_eur],axis=1)
    #final_base.to_csv("final.csv",index=False)
    r = [table.insert(data) for data in final_base.T.to_dict().values()]
    console.success("Base.csv to SQL insert...")


def main():
  
  schedule.every(5).minutes.do(job)
  while True:
    cowsay.cow('{} Aguardando....'.format(dt.now()))
    schedule.run_pending()
    time.sleep(1)
 
if __name__ == "__main__":
  main()
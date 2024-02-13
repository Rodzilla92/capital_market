import pandas as pd
import datetime
import pyRofex
import numpy as np
import time
import gspread
import pytz

############
### HORA ###
############
hora_arg = pytz.timezone('America/Argentina/Buenos_Aires')

datetime.datetime.now(hora_arg).strftime('%d-%m-%Y')

##############
### SHEETS ###
##############
gc = gspread.service_account(filename= '########')
sh = gc.open_by_url('#####')
AL_GD = sh.worksheet("Datos")


##############
### BROKER ###
##############
api_url = "https://api.veta.xoms.com.ar/"    # Cambiar por el broker que utilicen
ws_url = "wss://api.veta.xoms.com.ar/"

username = "#######"
password = "#######"
account = "#######"

##########################
### conexion a pyrofex ###
##########################
pyRofex._set_environment_parameter("url", api_url, pyRofex.Environment.LIVE)
pyRofex._set_environment_parameter("ws", ws_url, pyRofex.Environment.LIVE)

pyRofex.initialize(user=username,
                   password=password,
                   account=account,
                   environment=pyRofex.Environment.LIVE
                   )


instruments = ["MERV - XMEV - GD30 - 48hs", "MERV - XMEV - AL30 - 48hs"
               ]

entries = [pyRofex.MarketDataEntry.BIDS,
           pyRofex.MarketDataEntry.OFFERS,
           pyRofex.MarketDataEntry.LAST
           ]

prices = pd.DataFrame(columns=['Symbol', 'Time', 'Bid', 'Offer', 'Last'])
prices.set_index('Symbol', inplace = True)


###################
### Data Handler###
###################
def market_data_handler(message):
    global prices
    h = datetime.datetime.fromtimestamp(message['timestamp']/1000)
    time_a= h.astimezone(hora_arg)
    prices.loc[message['instrumentId']['symbol']] = [
        time_a.strftime('%H:%M:%S'),
        message['marketData']['BI'][0]['price'],
        message['marketData']['OF'][0]['price'],
        message['marketData']['LA']['price']
    ]


pyRofex.init_websocket_connection(market_data_handler=market_data_handler)


pyRofex.market_data_subscription(tickers = instruments,
                                 entries=entries)


#############
### RATIO ###
#############

def ratio(r1,r2):
  ratio1 = prices[prices.index == r1]
  ratio2 = prices[prices.index == r2]
  merge = pd.merge(ratio1, ratio2, on='Time', how='outer')
  #merge


  if merge.isna().any().any():
      merge.iloc[0] = merge.iloc[0].fillna(merge.iloc[1])
      merge = merge.drop(1)
      #return(merge)
      merge
  else:
    #return(merge)
    merge


  merge['ratio'] = [round(((merge['Last_x'] / merge['Last_y'])-1)*100, 2)]
  merge['ratio'] = float(merge['ratio'].iloc[0])

  merge['GD_a_AL'] = [round(((merge['Bid_x'] / merge['Offer_y'])-1)*100, 2)]
  merge['GD_a_AL'] = float(merge['GD_a_AL'].iloc[0])

  merge['AL_a_GD'] = [round(((merge['Offer_x'] / merge['Bid_y'])-1)*100, 2)]
  merge['AL_a_GD'] = float(merge['AL_a_GD'].iloc[0])
  
  merge['Day'] = datetime.datetime.now(hora_arg).strftime('%d-%m-%Y')
  
  column_order = ['Day','Time', 'Bid_x', 'Offer_x', 'Last_x','Bid_y', 'Offer_y', 'Last_y', 'ratio','GD_a_AL','AL_a_GD']

  merge = merge[column_order]

  return(merge)

while True:
  if datetime.datetime.now(hora_arg).strftime('%H:%M:%S') > '11:01:00':
      break
  time.sleep(20)


### Limpiar la información del día anterior ###
if AL_GD.acell('L1').value != datetime.datetime.now(hora_arg).strftime('%d-%m-%Y'):
    datos_ayer = AL_GD.get('A1:K')
    ult = len(datos_ayer)
    ante_ult = ult - 180
    recap = AL_GD.get(f'A{ante_ult}:K{ult}')
    df = pd.DataFrame(recap)
    for i in range(2, 10):
       df[i] = pd.to_numeric(df[i], errors='coerce')
    AL_GD.batch_clear(["A2:K6000"])
    AL_GD.update('A2',df.values.tolist())
    AL_GD.update('L1', datetime.datetime.now(hora_arg).strftime('%d-%m-%Y'))
    AL_GD.update('AF2', 1)
  
values_list = AL_GD.col_values(1)

x = len(values_list) + 1

while True:
  if datetime.datetime.now(hora_arg).strftime('%H:%M:%S') > '17:00:00':
      print('DING DING DING')
      break
  else:
    ratio1 = ratio("MERV - XMEV - GD30 - 48hs", "MERV - XMEV - AL30 - 48hs")
    AL_GD.update(f'A{x}',ratio1.values.tolist())

    x = x + 1
  time.sleep(5)

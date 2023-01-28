#Importar las libreria
import ccxt
import numpy as np
import pandas as pd
pd.set_option('display.max_rows', None)
from datetime import datetime
from ta.trend import MACD
from ta.momentum import RSIIndicator
import warnings
warnings.filterwarnings('ignore')
import schedule as schedule
import time
import mysql.connector
import os
from dotenv import load_dotenv

#Iniciar las variables de entorno
load_dotenv()
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pwd = os.getenv('DB_PWD')


#Conectar con los datos del exchange
exchange = ccxt.binance()

#Señales
def technical_signals(df):

    # MACD
    indicator_macd = MACD(df['close'])
    df['MACD'] = indicator_macd.macd()
    df['Signal']= indicator_macd.macd_signal()
    df['MACD Histogram']= indicator_macd.macd_diff()
    df['MACD_Signal'] = False
   
    # RSI
    indicator_rsi = RSIIndicator(df['close'], window=14)
    df['RSI_Signal'] = False
    df['RSI'] = indicator_rsi.rsi()

    #Logica
    for current in range(1, len(df.index)):
        previous = current - 1
        if (df['MACD'][current] > df['Signal'][current]) and (df['MACD'][previous] <  df['Signal'][previous]) and (df['MACD'][current]<0):
            df['MACD_Signal'][current] = True
        elif (df['MACD'][current] < df['Signal'][current]) and (df['MACD'][previous] >  df['Signal'][previous]):
            df['MACD_Signal'][current] = False
        else:
            df['MACD_Signal'][current] = df['MACD_Signal'][previous]
    return df


#EJECUTAR LAS ORDENES

#Estado para saber si tengo alguna posicion abierta
in_position = False
last_buy_price = 0
last_sell_price = 0
last_buy_time = '2023-07-23 00:00:00'
last_sell_time = '2023-07-23 00:00:00'

def read_position(position):
    position_info = 'No hay informacion sobre la posicion'
    if position:
        position_info = 'En posicion '
    return position_info

def save_report():
    mydb = mysql.connector.connect(
        host=db_host,
        user=db_user,
        passwd=db_pwd,
        database='backtest'
    )
    mycursor = mydb.cursor()
    sql = f"INSERT INTO btc_usdt (buy_time, buy_price, sell_price, sell_time) VALUES (%s, %s, %s, %s)"
    val = (last_buy_time, last_buy_price, last_sell_price, last_sell_time)
    mycursor.execute(sql, val)
    mydb.commit()
    print('Se ha registrado la operación')

#Logica para ver cuando cambian las señales y ejecutar la orden
def reading_market(df):
    global in_position
    global last_buy_price
    global last_sell_price
    global last_buy_time
    global last_sell_time
    global session_results

    price_now = df['close'][98] 

    print("Buscando señales...")
    print(" ")
    print(df.tail(4))
    last_row = len(df.index) - 1
    previous_row = last_row - 1

    if not df['MACD_Signal'][previous_row] and df['MACD_Signal'][last_row]:
        print(" ")
        print("SEÑAL DE COMPRA")
        print("Uptrend activated according MACD, BUY SIGNAL triggered")
        if not in_position:
            order_buy = 'Compra simulada' #exchange.create_market_buy_order('BTC/USDT', 1)
            in_position = True
            last_buy_price = price_now 
            now = datetime.now()
            last_buy_time = now   
                
            print(" ")
            print("COMPRA REALIZADA")
            print(order_buy)
            print(price_now)
        else:
            print("Se saltó esta señal porque ya hay una posicion abierta")
    
    if df['MACD_Signal'][previous_row] and not df['MACD_Signal'][last_row]:
        print(" ")
        print("SEÑAL DE VENTA")
        if in_position:
            print("Downtrend activated, SELL SIGNAL triggered")
            order_sell = 'Venta simulada' # exchange.create_market_sell_order('BTC/USDT', 1)
            in_position = False
            last_sell_price = price_now
            now = datetime.now()
            last_sell_time = now   

            #Imprimir venta

            print(" ")
            print("VENTA REALIZADA")
            print(order_sell)
            print(price_now)

            #Guardar venta en el reporte
            save_report()
        else:
            print("Se saltó esta señal porque no hay posiciones abiertas")
            

#Pedir datos a la api

def execute_connection(symbol='BTC/USDT', timeframe='1m'):
    
    print(f'-------------------------------------------------------------------     POSICION[{in_position}]     ULTIMA COMPRA[{last_buy_price}]     ULTIMA VENTA[{last_sell_price}]')
    print('Comenzando ciclo de analisis de mercado')
    raw_data = exchange.fetch_ohlcv(symbol, timeframe, limit=100)
    df = pd.DataFrame(raw_data[:-1], columns=['date', 'open', 'high', 'low', 'close', 'volume'])
    df['date'] = pd.to_datetime(df['date'], unit='ms')
    print(f"Ejecutando la conección y procesamiento de datos en... {datetime.now().isoformat()}")
    complete_df = technical_signals(df)
    reading_market(complete_df)
    print(" ")
    print("Ciclo de analisis de mercado finalizado")
    print(" ")
    print(" ")

schedule.every(10).seconds.do(execute_connection)

while True:
    schedule.run_pending()
    time.sleep(1)



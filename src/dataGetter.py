#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import quandl
import pandas as pd
from builtins import print

quandl.ApiConfig.api_key = 'pRmiVtdKTYpQUdHzJsyG'


def getMicrosoft(type):
    if type == 'normal' or type == 'all':
        data = quandl.get('EOD/MSFT', start_date='2005-01-01', collapse='daily')
        data.to_csv(path_or_buf='../data/MicrosoftNormal', sep=',')
    if type == 'change' or type == 'all':
        data = quandl.get('EOD/MSFT', start_date='2005-01-01', collapse='daily', transform="diff")
        data.to_csv(path_or_buf='../data/MicrosoftChange', sep=',')
    if type == 'rchange' or type == 'all':
        data = quandl.get('EOD/MSFT', start_date='2005-01-01', collapse='daily', transform="rdiff")
        data.to_csv(path_or_buf='../data/MicrosoftRChange', sep=',')
    if type == 'normalize' or type == 'all':
        data = quandl.get('EOD/MSFT', start_date='2005-01-01', collapse='daily', transform="normalize")
        data.to_csv(path_or_buf='../data/MicrosoftNormalize', sep=',')
    print('Microsoft done')


def getDisney(type):
    if type == 'normal' or type == 'all':
        data = quandl.get('EOD/DIS', start_date='2005-01-01', collapse='daily')
        data.to_csv(path_or_buf='../data/DisneyNormal', sep=',')
    if type == 'change' or type == 'all':
        data = quandl.get('EOD/DIS', start_date='2005-01-01', collapse='daily', transform="diff")
        data.to_csv(path_or_buf='../data/DisneyChange', sep=',')
    if type == 'rchange' or type == 'all':
        data = quandl.get('EOD/DIS', start_date='2005-01-01', collapse='daily', transform="rdiff")
        data.to_csv(path_or_buf='../data/DisneyRChange', sep=',')
    if type == 'normalize' or type == 'all':
        data = quandl.get('EOD/DIS', start_date='2005-01-01', collapse='daily', transform="normalize")
        data.to_csv(path_or_buf='../data/DisneyNormalize', sep=',')
    print('Disney done')


def getApple(type):
    if type == 'normal' or type == 'all':
        data = quandl.get('EOD/AAPL', start_date='2005-01-01', collapse='daily')
        data.to_csv(path_or_buf='../data/AppleNormal', sep=',')
    if type == 'change' or type == 'all':
        data = quandl.get('EOD/AAPL', start_date='2005-01-01', collapse='daily', transform="diff")
        data.to_csv(path_or_buf='../data/AppleChange', sep=',')
    if type == 'rchange' or type == 'all':
        data = quandl.get('EOD/AAPL', start_date='2005-01-01', collapse='daily', transform="rdiff")
        data.to_csv(path_or_buf='../data/AppleRChange', sep=',')
    if type == 'normalize' or type == 'all':
        data = quandl.get('EOD/AAPL', start_date='2005-01-01', collapse='daily', transform="normalize")
        data.to_csv(path_or_buf='../data/AppleNormalize', sep=',')
    print('Apple done')


def getNike(type):
    if type == 'normal' or type == 'all':
        data = quandl.get('EOD/NKE', start_date='2005-01-01', collapse='daily')
        data.to_csv(path_or_buf='../data/NikeNormal', sep=',')
    if type == 'change' or type == 'all':
        data = quandl.get('EOD/NKE', start_date='2005-01-01', collapse='daily', transform="diff")
        data.to_csv(path_or_buf='../data/NikeChange', sep=',')
    if type == 'rchange' or type == 'all':
        data = quandl.get('EOD/NKE', start_date='2005-01-01', collapse='daily', transform="rdiff")
        data.to_csv(path_or_buf='../data/NikeChange', sep=',')
    if type == 'normalize' or type == 'all':
        data = quandl.get('EOD/NKE', start_date='2005-01-01', collapse='daily', transform="normalize")
        data.to_csv(path_or_buf='../data/NikeNormalize', sep=',')
    print('LondonStock done')

def getAll(type):
    if type == 'normal' or type == 'all':
        data = quandl.get(['EOD/MSFT.4', 'EOD/DIS.4', 'EOD/NKE.4', 'EOD/AAPL.4'], start_date='2000-01-01', collapse='daily')
        data.to_csv(path_or_buf='../data/CloseNormal', sep=',')
    if type == 'change' or type == 'all':
        data = quandl.get(['EOD/MSFT.4', 'EOD/DIS.4', 'EOD/NKE.4', 'EOD/AAPL.4'], start_date='2000-01-01',
                          collapse='daily', transform="diff")
        data.to_csv(path_or_buf='../data/CloseChange', sep=',')
    if type == 'rchange' or type == 'all':
        data = quandl.get(['EOD/MSFT.4', 'EOD/DIS.4', 'EOD/NKE.4', 'EOD/AAPL.4'], start_date='2000-01-01',
                          collapse='daily', transform="rdiff")
        data.to_csv(path_or_buf='../data/CloseRChange', sep=',')
    if type == 'normalize' or type == 'all':
        data = quandl.get(['EOD/MSFT.4', 'EOD/DIS.4', 'EOD/NKE.4', 'EOD/AAPL.4'], start_date='2000-01-01',
                          collapse='daily', transform="normalize")
    print('All in one done')

def closeAll(type):
    """ Metodo que creara dos csv, uno con los datos de cierre de las bolsas a estudiar y otro con los resultados
        del MSFT5 de ese mismo dia, que posteriomente correremos una posicion para tomarlo como prediccion
    :param type: Los tipos de datos a obtener, ya sean valores normales o cambio
    :return: csv con los datos
    """
    """ Guardamos los datos de cierre del MSFT5, que es nuestro oobjetivo """
    next_day = quandl.get('EOD/MSFT.4', start_date='2000-01-01', collapse='daily', transform='diff')
    next_day.to_csv(path_or_buf='../data/NextDay', sep=',')
    """ Guardamos los datos de cierre de las demas bolsas de estudio """
    if type == 'normal' or type == 'all':
        data = quandl.get(['EOD/MSFT.4', 'EOD/DIS.4', 'EOD/NKE.4', 'EOD/AAPL.4'], start_date='2000-01-01', collapse='daily')
        data.to_csv(path_or_buf='../data/CloseNormal', sep=',')
    if type == 'change' or type == 'all':
        data = quandl.get(['EOD/MSFT.4', 'EOD/DIS.4', 'EOD/NKE.4', 'EOD/AAPL.4'], start_date='2000-01-01',
                          collapse='daily', transform="diff")
        data.to_csv(path_or_buf='../data/CloseChange', sep=',')
    if type == 'rchange' or type == 'all':
        data = quandl.get(['EOD/MSFT.4', 'EOD/DIS.4', 'EOD/NKE.4', 'EOD/AAPL.4'], start_date='2000-01-01',
                          collapse='daily', transform="rdiff")
        data.to_csv(path_or_buf='../data/CloseRChange', sep=',')
    if type == 'normalize' or type == 'all':
        data = quandl.get(['EOD/MSFT.4', 'EOD/DIS.4', 'EOD/NKE.4', 'EOD/AAPL.4'], start_date='2000-01-01',
                          collapse='daily', transform="normalize")
        data.to_csv(path_or_buf='../data/CloseNormalize', sep=',')
    print('Closing done')


def final_data_creator(type):
    """
    Este metodo cambiara los resultados de los datos del dia siguiente a 1 si sube y 0 si baja o se mantiene igual
    que el dia anterior. Ademas une las predicciones a los datos anteriores
    :param type:
    :return:
    """
    next_day = pd.read_csv('../data/NextDay', index_col='Date', parse_dates=True, na_values=['nan'])
    """ Cambiamos los resultados por 1 si sube y 0 si baja """
    next_day.loc[next_day['Close'] > 0, 'Close'] = 1
    next_day.loc[next_day['Close'] <= 0, 'Close'] = 0
    """ Subimos los resultados un dia para que pasen a ser predicciones del dia siguiente """
    next_day.Close = next_day.Close.shift(-1)
    if type == 'normal' or type == 'all':
        closings = pd.read_csv('../data/CloseNormal', index_col='Date', parse_dates=True, na_values=['nan'])
        closings = closings.join(next_day)
        """ Con esto eliminamos los datos de los dias en los que el MSFT5 no opera """
        closings = closings.dropna(subset=['Close'])
        """ Si algun mercado secundario no abre un dia se rellenan sus datos con el dia anterior para la prediccion """
        closings.fillna(method='ffill', inplace=True)
        closings.fillna(method='bfill', inplace=True)
        """ Guardamos """
        closings.to_csv(path_or_buf='../data/ProcessedNormal.csv', sep=',')
    if type == 'change' or type == 'all':
        closings = pd.read_csv('../data/CloseChange', index_col='Date', parse_dates=True, na_values=['nan'])
        closings = closings.join(next_day)
        """ Con esto eliminamos los datos de los dias en los que el MSFT5 no opera """
        closings = closings.dropna(subset=['Close'])
        """ Si algun mercado secundario no abre un dia se rellenan sus datos con el dia anterior para la prediccion """
        closings.fillna(method='ffill', inplace=True)
        closings.fillna(method='bfill', inplace=True)
        """ Guardamos """
        closings.to_csv(path_or_buf='../data/ProcessedChange.csv', sep=',')
    if type == 'rchange' or type == 'all':
        closings = pd.read_csv('../data/CloseRChange', index_col='Date', parse_dates=True, na_values=['nan'])
        closings = closings.join(next_day)
        """ Con esto eliminamos los datos de los dias en los que el MSFT5 no opera """
        closings = closings.dropna(subset=['Close'])
        """ Si algun mercado secundario no abre un dia se rellenan sus datos con el dia anterior para la prediccion """
        closings.fillna(method='ffill', inplace=True)
        closings.fillna(method='bfill', inplace=True)
        """ Guardamos """
        closings.to_csv(path_or_buf='../data/ProcessedRChange.csv', sep=',')
    if type == 'normalize' or type == 'all':
        closings = pd.read_csv('../data/CloseNormalize', index_col='Date', parse_dates=True, na_values=['nan'])
        closings = closings.join(next_day)
        """ Con esto eliminamos los datos de los dias en los que el MSFT5 no opera """
        closings = closings.dropna(subset=['Close'])
        """ Si algun mercado secundario no abre un dia se rellenan sus datos con el dia anterior para la prediccion """
        closings.fillna(method='ffill', inplace=True)
        closings.fillna(method='bfill', inplace=True)
        """ Guardamos """
        closings.to_csv(path_or_buf='../data/ProcessedNormalized.csv', sep=',')
    print('Data processing done')


def five_final_data_creator(type):
    """
    Este metodo cambiara los resultados de los datos del dia siguiente a 1 si sube y 0 si baja o se mantiene igual
    que el dia anterior tomando los datos de los 5 dias anteriores Ademas une las predicciones a los datos anteriores
    :param type:
    :return:
    """
    next_day = pd.read_csv('../data/NextDay', index_col='Date', parse_dates=True, na_values=['nan'])
    """ Cambiamos los resultados por 1 si sube y 0 si baja """
    next_day.loc[next_day['Close'] > 0, 'Close'] = 1
    next_day.loc[next_day['Close'] <= 0, 'Close'] = 0
    """ Subimos los resultados un dia para que pasen a ser predicciones del dia siguiente """
    next_day.Close = next_day.Close.shift(-5)
    if type == 'normal' or type == 'all':
        closings = pd.read_csv('../data/CloseNormal', index_col='Date', parse_dates=True, na_values=['nan'])
        """ Empezamos con los 5 dias """
        closings = closings.rename(columns={'EOD/MSFT - Close':'MSFT', 'EOD/DIS - Close':'Disney',
                                            'EOD/NKE - Close':'Nike', 'EOD/AAPL - Close':'Apple'
                                            })
        to_take = closings
        """ 5 DIA """
        closings = to_take.rename(columns={'MSFT':'MSFT5', 'Disney':'Disney5', 'Nike':'Nike5', 'Apple':'Apple5'})
        """ 4 DIA subimos los valores un dia """
        closings4 = to_take.rename(columns={'MSFT':'MSFT4', 'Disney':'Disney4', 'Nike':'Nike4',
                                            'Apple':'Apple4'})
        #print(closings4.columns)
        closings4.MSFT4 = closings4.MSFT4.shift(-1)
        closings4.Disney4 = closings4.Disney4.shift(-1)
        closings4.Nike4 = closings4.Nike4.shift(-1)
        closings4.Apple4 = closings4.Apple4.shift(-1)
        closings = closings.join(closings4)
        """ 3 DIA subimos dos"""
        closings3 = to_take.rename(columns={'MSFT':'MSFT3', 'Disney':'Disney3', 'Nike':'Nike3',
                                             'Apple':'Apple3'})
        closings3.MSFT3 = closings3.MSFT3.shift(-2)
        closings3.Disney3 = closings3.Disney3.shift(-2)
        closings3.Nike3 = closings3.Nike3.shift(-2)
        closings3.Apple3 = closings3.Apple3.shift(-2)
        closings = closings.join(closings3)
        """ 2 DIA subimos dos"""
        closings2 = to_take.rename(columns={'MSFT':'MSFT2', 'Disney':'Disney2', 'Nike':'Nike2',
                                             'Apple':'Apple2'})
        closings2.MSFT2 = closings2.MSFT2.shift(-3)
        closings2.Disney2 = closings2.Disney2.shift(-3)
        closings2.Nike2 = closings2.Nike2.shift(-3)
        closings2.Apple2 = closings2.Apple2.shift(-3)
        closings = closings.join(closings2)
        """ 1 DIA subimos dos"""
        closings1 = to_take.rename(columns={'MSFT':'MSFT1', 'Disney':'Disney1', 'Nike':'Nike1',
                                             'Apple':'Apple1'})
        closings1.MSFT1 = closings1.MSFT1.shift(-4)
        closings1.Disney1 = closings1.Disney1.shift(-4)
        closings1.Nike1 = closings1.Nike1.shift(-4)
        closings1.Apple1 = closings1.Apple1.shift(-4)
        closings = closings.join(closings1)
        """ Añadimos la prediccion """
        closings = closings.join(next_day)
        """ Con esto eliminamos los datos de los dias en los que el MSFT5 no opera """
        closings = closings.dropna(subset=['Close'])
        """ Si algun mercado secundario no abre un dia se rellenan sus datos con el dia anterior para la prediccion """
        closings.fillna(method='ffill', inplace=True)
        closings.fillna(method='bfill', inplace=True)
        """ Guardamos """
        closings.to_csv(path_or_buf='../data/5DayNormal.csv', sep=',')
    if type == 'change' or type == 'all':
        closings = pd.read_csv('../data/CloseChange', index_col='Date', parse_dates=True, na_values=['nan'])
        """ Empezamos con los 5 dias """
        closings = closings.rename(columns={'EOD/MSFT - Close':'MSFT', 'EOD/DIS - Close':'Disney',
                                            'EOD/NKE - Close':'Nike', 'EOD/AAPL - Close':'Apple'})
        to_take = closings
        """ 5 DIA """
        closings = to_take.rename(columns={'MSFT':'MSFT5', 'Disney':'Disney5', 'Nike':'Nike5',
                                           'Apple':'Apple5'})
        """ 4 DIA subimos los valores un dia """
        closings4 = to_take.rename(columns={'MSFT':'MSFT4', 'Disney':'Disney4', 'Nike':'Nike4',
                                            'Apple':'Apple4'})
        closings4.MSFT4 = closings4.MSFT4.shift(-1)
        closings4.Disney4 = closings4.Disney4.shift(-1)
        closings4.Nike4 = closings4.Nike4.shift(-1)
        closings4.Apple4 = closings4.Apple4.shift(-1)
        closings = closings.join(closings4)
        """ 3 DIA subimos dos"""
        closings3 = to_take.rename(columns={'MSFT':'MSFT3', 'Disney':'Disney3', 'Nike':'Nike3',
                                            'Apple':'Apple3'})
        closings3.MSFT3 = closings3.MSFT3.shift(-2)
        closings3.Disney3 = closings3.Disney3.shift(-2)
        closings3.Nike3 = closings3.Nike3.shift(-2)
        closings3.Apple3 = closings3.Apple3.shift(-2)
        closings = closings.join(closings3)
        """ 2 DIA subimos dos"""
        closings2 = to_take.rename(columns={'MSFT':'MSFT2', 'Disney':'Disney2', 'Nike':'Nike2',
                                            'Apple':'Apple2', 'Francia':'Francia2', 'Alemania':'Alemania2'})
        closings2.MSFT2 = closings2.MSFT2.shift(-3)
        closings2.Disney2 = closings2.Disney2.shift(-3)
        closings2.Nike2 = closings2.Nike2.shift(-3)
        closings2.Apple2 = closings2.Apple2.shift(-3)
        closings = closings.join(closings2)
        """ 1 DIA subimos dos"""
        closings1 = to_take.rename(columns={'MSFT':'MSFT1', 'Disney':'Disney1', 'Nike':'Nike1',
                                            'Apple':'Apple1'})
        closings1.MSFT1 = closings1.MSFT1.shift(-4)
        closings1.Disney1 = closings1.Disney1.shift(-4)
        closings1.Nike1 = closings1.Nike1.shift(-4)
        closings1.Apple1 = closings1.Apple1.shift(-4)
        closings = closings.join(closings1)
        """ Añadimos la prediccion """
        closings = closings.join(next_day)
        """ Con esto eliminamos los datos de los dias en los que el MSFT5 no opera """
        closings = closings.dropna(subset=['Close'])
        """ Si algun mercado secundario no abre un dia se rellenan sus datos con el dia anterior para la prediccion """
        closings.fillna(method='ffill', inplace=True)
        closings.fillna(method='bfill', inplace=True)
        """ Guardamos """
        closings.to_csv(path_or_buf='../data/5DayChange.csv', sep=',')
    if type == 'rchange' or type == 'all':
        closings = pd.read_csv('../data/CloseRChange', index_col='Date', parse_dates=True, na_values=['nan'])
        """ Empezamos con los 5 dias """
        closings = closings.rename(columns={'EOD/MSFT - Close':'MSFT', 'EOD/DIS - Close':'Disney',
                                            'EOD/NKE - Close':'Nike', 'EOD/AAPL - Close':'Apple'})
        to_take = closings
        """ 5 DIA """
        closings = to_take.rename(columns={'MSFT':'MSFT5', 'Disney':'Disney5', 'Nike':'Nike5',
                                           'Apple':'Apple5'})
        """ 4 DIA subimos los valores un dia """
        closings4 = to_take.rename(columns={'MSFT':'MSFT4', 'Disney':'Disney4', 'Nike':'Nike4',
                                            'Apple':'Apple4'})
        closings4.MSFT4 = closings4.MSFT4.shift(-1)
        closings4.Disney4 = closings4.Disney4.shift(-1)
        closings4.Nike4 = closings4.Nike4.shift(-1)
        closings4.Apple4 = closings4.Apple4.shift(-1)
        closings = closings.join(closings4)
        """ 3 DIA subimos dos"""
        closings3 = to_take.rename(columns={'MSFT':'MSFT3', 'Disney':'Disney3', 'Nike':'Nike3',
                                            'Apple':'Apple3'})
        closings3.MSFT3 = closings3.MSFT3.shift(-2)
        closings3.Disney3 = closings3.Disney3.shift(-2)
        closings3.Nike3 = closings3.Nike3.shift(-2)
        closings3.Apple3 = closings3.Apple3.shift(-2)
        closings = closings.join(closings3)
        """ 2 DIA subimos dos"""
        closings2 = to_take.rename(columns={'MSFT':'MSFT2', 'Disney':'Disney2', 'Nike':'Nike2',
                                            'Apple':'Apple2'})
        closings2.MSFT2 = closings2.MSFT2.shift(-3)
        closings2.Disney2 = closings2.Disney2.shift(-3)
        closings2.Nike2 = closings2.Nike2.shift(-3)
        closings2.Apple2 = closings2.Apple2.shift(-3)
        closings = closings.join(closings2)
        """ 1 DIA subimos dos"""
        closings1 = to_take.rename(columns={'MSFT':'MSFT1', 'Disney':'Disney1', 'Nike':'Nike1',
                                            'Apple':'Apple1'})
        closings1.MSFT1 = closings1.MSFT1.shift(-4)
        closings1.Disney1 = closings1.Disney1.shift(-4)
        closings1.Nike1 = closings1.Nike1.shift(-4)
        closings1.Apple1 = closings1.Apple1.shift(-4)
        closings = closings.join(closings1)
        """ Añadimos la prediccion """
        closings = closings.join(next_day)
        """ Con esto eliminamos los datos de los dias en los que el MSFT5 no opera """
        closings = closings.dropna(subset=['Close'])
        """ Si algun mercado secundario no abre un dia se rellenan sus datos con el dia anterior para la prediccion """
        closings.fillna(method='ffill', inplace=True)
        closings.fillna(method='bfill', inplace=True)
        """ Guardamos """
        closings.to_csv(path_or_buf='../data/5DayRChange.csv', sep=',')
    if type == 'normalize' or type == 'all':
        closings = pd.read_csv('../data/CloseNormalize', index_col='Date', parse_dates=True, na_values=['nan'])
        """ Empezamos con los 5 dias """
        closings = closings.rename(columns={'EOD/MSFT - Close':'MSFT', 'EOD/DIS - Close':'Disney',
                                            'EOD/NKE - Close':'Nike', 'EOD/AAPL - Close':'Apple'})
        to_take = closings
        """ 5 DIA """
        closings = to_take.rename(columns={'MSFT':'MSFT5', 'Disney':'Disney5', 'Nike':'Nike5',
                                           'Apple':'Apple5'})
        """ 4 DIA subimos los valores un dia """
        closings4 = to_take.rename(columns={'MSFT':'MSFT4', 'Disney':'Disney4', 'Nike':'Nike4',
                                            'Apple':'Apple4'})
        closings4.MSFT4 = closings4.MSFT4.shift(-1)
        closings4.Disney4 = closings4.Disney4.shift(-1)
        closings4.Nike4 = closings4.Nike4.shift(-1)
        closings4.Apple4 = closings4.Apple4.shift(-1)
        closings = closings.join(closings4)
        """ 3 DIA subimos dos"""
        closings3 = to_take.rename(columns={'MSFT':'MSFT3', 'Disney':'Disney3', 'Nike':'Nike3',
                                            'Apple':'Apple3'})
        closings3.MSFT3 = closings3.MSFT3.shift(-2)
        closings3.Disney3 = closings3.Disney3.shift(-2)
        closings3.Nike3 = closings3.Nike3.shift(-2)
        closings3.Apple3 = closings3.Apple3.shift(-2)
        closings = closings.join(closings3)
        """ 2 DIA subimos dos"""
        closings2 = to_take.rename(columns={'MSFT':'MSFT2', 'Disney':'Disney2', 'Nike':'Nike2',
                                            'Apple':'Apple2'})
        closings2.MSFT2 = closings2.MSFT2.shift(-3)
        closings2.Disney2 = closings2.Disney2.shift(-3)
        closings2.Nike2 = closings2.Nike2.shift(-3)
        closings2.Apple2 = closings2.Apple2.shift(-3)
        closings = closings.join(closings2)
        """ 1 DIA subimos dos"""
        closings1 = to_take.rename(columns={'MSFT':'MSFT1', 'Disney':'Disney1', 'Nike':'Nike1',
                                            'Apple':'Apple1'})
        closings1.MSFT1 = closings1.MSFT1.shift(-4)
        closings1.Disney1 = closings1.Disney1.shift(-4)
        closings1.Nike1 = closings1.Nike1.shift(-4)
        closings1.Apple1 = closings1.Apple1.shift(-4)
        closings = closings.join(closings1)
        """ Añadimos la prediccion """
        closings = closings.join(next_day)
        """ Con esto eliminamos los datos de los dias en los que el MSFT5 no opera """
        closings = closings.dropna(subset=['Close'])
        """ Si algun mercado secundario no abre un dia se rellenan sus datos con el dia anterior para la prediccion """
        closings.fillna(method='ffill', inplace=True)
        closings.fillna(method='bfill', inplace=True)
        """ Guardamos """
        closings.to_csv(path_or_buf='../data/5DayNormalized.csv', sep=',')
    print('Data processing done')


def main():
    getMicrosoft('all')
    getDisney('all')
    getApple('all')
    getNike('all')
    getAll('all')
    closeAll('all')
    final_data_creator('all')
    five_final_data_creator('all')

if __name__ == '__main__':
    main()
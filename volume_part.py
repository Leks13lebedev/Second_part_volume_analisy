from All_files import Futures_output_data
import pandas as pd
import numpy as np
import requests
import time

data_loader = Futures_output_data(type_data='candels',
                                  type_history='all_securities',
                                  tikers= 'all',
                                  start_time= '2022-04-01',
                                  end_time= '2026-02-15',
                                  interval= 1)
df = data_loader.find_data()
df_group = df.groupby(['ASSETCODE','begin'])['volume'].sum().reset_index()
df_group.to_csv('volume.csv',index = False)
df = pd.read_csv('volume.csv')
assets_tiker_forts = {'currency': {'Si', 'CNY', 'Eu', 'HKD', 'TRY', 'AED', 'INR', 'AMD', 'KZT','BYN', 'ED', 'GBPU', 
                                   'AUDU', 'UJPY', 'UCHF', 'UTRY', 'UCAD', 'UCNY', 'UKZT', 'EGBP', 'ECAD', 'EJPY'},
                        'fond':{'SBRF', 'GAZR', 'SIBN', 'LKOH', 'RTKMP', 'ROSN', 'TATN', 'MTSI', 'NOTKM', 'VTBR', 'SNGR',
                            'NLMK', 'HYDR', 'FEES', 'CHMF', 'GMKN', 'MOEX', 'MGNT', 'ALRS', 'AFLT', 'MAGN', 'AFKS',
                            'PLZLM', 'IRAO', 'PIKK', 'SPBE', 'RUAL', 'PHOR', 'YDEX', 'BELU', 'SBPR', 'SNGP', 'TRNF','TATP', 'SMLT', 
                            'MTLR', 'CBOM', 'POSI', 'ISKJ', 'MVID', 'WUSH', 'SGZH', 'FLOT', 'BSPB', 'BANE', 'KMAZ', 'VKCO', 'ASTR', 
                            'SOFL', 'SVCB', 'T', 'RASP', 'FESH', 'RNFT', 'LEAS', 'TCSI', 'HEAD', 'MDMG', 'RENI', 'UPRO','IVAT', 'ENPG',
                            'BELUGA','RTKM','SFIN','X5'},
                        'international_fond':{'INDIA', 'SPYF', 'NASD', 'HANG', 'STOX', 'DAX', 'NIKK', 'R2000', 'DJ30', 'EM','SOXQ', 
                                              'XIA', 'TENCENT','ARGT', 'AFRICA', 'SAUDI', 'CHINA', 'BRAZIL', 'TLT','ALIBABA', 'BAIDU'},
                        'comodities':{'TTF','BR', 'BRM', 'NG', 'NGM', 'GL', 'GOLD', 'SILV', 'SILVM', 'PLT', 'PLD', 'COPPER', 
                                      'ALUM', 'ZINC', 'NICKEL', 'SUGR','WHEAT', 'SUGAR', 'COCOA',  'COFFEE', 'ORANGE',},
                        'index':{'RTS', 'RTSM', 'MIX', 'MXI', 'MOEXCNY', 'HOME', 'OGI', 'MMI', 'FNI', 'CNI', 'RGBI', 'IPO', 'RVI',},
                        'procent':{'RUON', '1MFR',},
                        'perpetual':{'SBERF', 'GAZPF', 'USDRUBTOM', 'EURRUBTOM', 'CNYRUBTOM', 'IMOEX', 'GLDRUBTOM', 'RGBIF'},
                        'crypto':{ 'IBIT','ETHA', 'BTC', 'ETH'}
}

reverse_dict = {}
for key, value_set in assets_tiker_forts.items():
    for value in value_set:
        reverse_dict[value] = key

# Добавляем новый столбец
df['NEW_COLUMN'] = df['ASSETCODE'].map(reverse_dict)

df.to_csv('volume.csv',index=False)

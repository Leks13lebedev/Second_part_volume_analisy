import pandas as pd
import requests
import time
from datetime import datetime, timedelta



class Futures_output_data():

    MONTH_CODES = {
        1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
        7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
    }

    PERPETUAL_CONTRACTS = {'USDRUBTOM':'USDRUBF','CNYRUBTOM':'CNYRUBF', 
                           'EURRUBTOM':'EURRUBF', 'IMOEX':'IMOEXF', 
                           'SBERF':'SBERF', 'GAZPF':'GAZRF', 
                           'RGBIF':'RGBIF', 'GLDRUBTOM':'GLDRUBF'}


    def __init__ (self, type_data, type_history, start_time, end_time, interval, tikers):
        self.type_data = type_data
        if self.type_data == 'history':
            self.start_time = start_time
            self.end_time = end_time
            self.type_history = type_history
        elif self.type_data == 'candels':
            self.start_time = start_time
            self.end_time = end_time
            self.interval = interval
            self.type_history = type_history
        else:
            self.start_time = None
            self.end_time = None
            self.interval = None

        self.tikers = tikers
        self.assetcode_mapping = {}

    def _safe_request(self, url, delay=3):
        while True:
            try:
                return requests.get(url, timeout=6)
            except:
                print(111111)
                time.sleep(delay)

    def get_base_asset(self):
        if self.tikers == "all":
            link = "https://iss.moex.com/iss/engines/futures/markets/forts/securities.json?securities.columns=SECID,ASSETCODE,SECTYPE,LASTTRADEDATE"
            response = self._safe_request(link)
            data = response.json()
            columns = data['securities']['columns']
            row = data['securities']['data']
            df = pd.DataFrame(row, columns=columns)
            self.assetcode_mapping = dict(zip(df['SECTYPE'], df['ASSETCODE']))
            return df
        elif isinstance(self.tikers, str):
            try:
                self.tikers = [ticker.strip() for ticker in self.tikers.split(',')]
            except:
                print('Неправильный формат ввода. Тикеры должны быть записаны через ", " ')
                return []
        else:
            raise ValueError("Неправильный формат тикеров. Используйте 'all', строку с тикером или список тикеров")
        link = "https://iss.moex.com/iss/engines/futures/markets/forts/securities.json?securities.columns=SECID,ASSETCODE,SECTYPE,LASTTRADEDATE"
        response = self._safe_request(link)
        data = response.json()
        columns = data['securities']['columns']
        row = data['securities']['data']
        df = pd.DataFrame(row, columns=columns)
        df = df[df['ASSETCODE'].isin(self.tikers)]
        self.assetcode_mapping = dict(zip(df['SECTYPE'], df['ASSETCODE']))
        return df

    def get_candels_last_df(self):
        df_all = []
        doble_tikers = list(self.get_base_asset()[['ASSETCODE', 'SECTYPE']].apply(
            lambda x: x[0] if x[0] in self.PERPETUAL_CONTRACTS else x[1], axis=1).unique())
        
        for short_tiker in doble_tikers:
            if short_tiker in self.PERPETUAL_CONTRACTS:
                df_candles = self.candels_download(self.PERPETUAL_CONTRACTS[short_tiker], self.start_time)
                df_candles.insert(0, 'TIKERS_NAME', self.PERPETUAL_CONTRACTS[short_tiker])
                df_candles.insert(0, 'ASSETCODE', short_tiker)
                df_all.append(df_candles)
            else:
                current_start = self.start_time
                month = int(current_start.split('-')[1])
                year_start = int(current_start.split('-')[0][-1])
                year_past = int(self.end_time.split('-')[0][-1])
                tikers_list = self.find_all_tikers(short_tiker, month, year_start, year_past)
                secid_data = []
                
                # Ищем первый непустой датафрейм
                found_first_valid = False
                empty_count = 0
                max_empty_in_row = 6
                
                while current_start < self.end_time and tikers_list:
                    found_data = False
                    
                    for ticker in tikers_list[:]:  # Используем копию списка для безопасного удаления
                        df_candles = self.candels_download(ticker, current_start)
                        
                        if not df_candles.empty:
                            df_candles.insert(0, 'TIKERS_NAME', ticker)
                            df_candles.insert(0, 'ASSETCODE', self.assetcode_mapping.get(ticker[:2], ticker[:2]))
                            secid_data.append(df_candles)
                            df_candles['end'] = pd.to_datetime(df_candles['end'])
                            last_date = df_candles['end'].max()
                            current_start = (last_date+ timedelta(days=1)).strftime('%Y-%m-%d')
                            found_data = True
                            tikers_list.remove(ticker)
                            found_first_valid = True  # Нашли первый валидный
                            empty_count = 0  # Сбрасываем счетчик
                            break
                        else:
                            # Если еще не нашли первый валидный - просто удаляем тикер
                            if not found_first_valid:
                                tikers_list.remove(ticker)
                            else:
                                # Увеличиваем счетчик пустых тикеров только после первого валидного
                                empty_count += 1
                                tikers_list.remove(ticker)
                                
                                # Если достигли предела пустых тикеров подряд - выходим
                                if empty_count >= max_empty_in_row:
                                    print(f"Достигнут предел в {max_empty_in_row} пустых тикеров подряд. Прерываем обработку {short_tiker}")
                                    tikers_list.clear()  # Очищаем список для выхода из цикла
                                    break
                    
                    if not found_data:
                        break

                if secid_data:
                    df_concat = pd.concat(secid_data, ignore_index=True)
                    df_all.append(df_concat)
            
            print(f'Обработали: {short_tiker}. Осталось: {len(doble_tikers) - doble_tikers.index(short_tiker) - 1}')
        
        if df_all:
            return pd.concat(df_all, ignore_index=True)
        else:
            return pd.DataFrame()
    
    def get_history_df(self):
        df = self.history_dowload(self.start_time,self.end_time)
        history_df_list = list(set(self.get_base_asset()['ASSETCODE'].to_list()))
        return df[df['ASSETCODE'].isin(history_df_list)]

    def get_history_last_df(self):
        df_all = []
        doble_tikers = list(self.get_base_asset()[['ASSETCODE', 'SECTYPE']].apply(
            lambda x: x[0] if x[0] in self.PERPETUAL_CONTRACTS else x[1], axis=1).unique())
        
        for short_tiker in doble_tikers:
            if short_tiker in self.PERPETUAL_CONTRACTS:
                df_candles = self.history_last_dowload(self.PERPETUAL_CONTRACTS[short_tiker], self.start_time, self.end_time)
                df_candles.insert(0, 'TIKERS_NAME', self.PERPETUAL_CONTRACTS[short_tiker])
                df_all.append(df_candles)
            else:
                current_start = self.start_time
                month = int(current_start.split('-')[1])
                year_start = int(current_start.split('-')[0][-1])
                year_past = int(self.end_time.split('-')[0][-1])
                tikers_list = self.find_all_tikers(short_tiker, month, year_start, year_past)
                secid_data = []
                
                # Ищем первый непустой датафрейм
                found_first_valid = False
                empty_count = 0
                max_empty_in_row = 6
                
                for ticker in tikers_list[:]:  # Используем копию списка для безопасного удаления
                    df_history = self.history_last_dowload(ticker, current_start,self.end_time)
                    print(f'Дата начала выгрузки данных{current_start}')
                    
                    if not df_history.empty:
                        df_history['TRADEDATE'] = pd.to_datetime(df_history['TRADEDATE'])
                        df_history = df_history[df_history['TRADEDATE'] == pd.to_datetime(self.end_time)]
                        
                        if not df_history.empty:
                            df_history.insert(0, 'TIKERS_NAME', ticker)
                            secid_data.append(df_history)
                            last_date = df_history['TRADEDATE'].max()
                            found_first_valid = True  # Нашли первый валидный
                            empty_count = 0  # Сбрасываем счетчик
                            current_start = (last_date+ timedelta(days=1)).strftime('%Y-%m-%d')
                            if last_date >= pd.to_datetime(self.end_time):
                                break
                    else:
                        # Если еще не нашли первый валидный - просто пропускаем
                        if not found_first_valid:
                            continue  # Просто продолжаем искать первый валидный
                        else:
                            # Увеличиваем счетчик пустых тикеров только после первого валидного
                            empty_count += 1
                            
                            # Если достигли предела пустых тикеров подряд - выходим
                            if empty_count >= max_empty_in_row:
                                print(f"Достигнут предел в {max_empty_in_row} пустых тикеров подряд. Прерываем обработку {short_tiker}")
                                break
                
                if secid_data:
                    df_concat = pd.concat(secid_data, ignore_index=True)
                    df_all.append(df_concat)
            
            print(f'Обработали: {short_tiker}. Осталось: {len(doble_tikers) - doble_tikers.index(short_tiker) - 1}')
        
        if df_all:
            return pd.concat(df_all, ignore_index=True)
        else:
            return pd.DataFrame()
    
    def get_candels_df(self):
        df_all = []
        doble_tikers = list(self.get_base_asset()[['ASSETCODE', 'SECTYPE']].apply(
            lambda x: x[0] if x[0] in self.PERPETUAL_CONTRACTS else x[1], axis=1).unique())
        
        for short_tiker in doble_tikers:
            if short_tiker in self.PERPETUAL_CONTRACTS:
                df_candles = self.candels_download(self.PERPETUAL_CONTRACTS[short_tiker],self.start_time)
                if not df_candles.empty:
                    df_candles.insert(0, 'TIKERS_NAME', self.PERPETUAL_CONTRACTS[short_tiker])
                    df_candles.insert(0, 'ASSETCODE', short_tiker)
                    df_all.append(df_candles)
            else:
                month = int(self.start_time.split('-')[1])
                year_start = int(self.start_time.split('-')[0][-1])
                year_past = int(self.end_time.split('-')[0][-1])
                tikers_list = self.find_all_tikers(short_tiker, month, year_start, year_past)
                for ticker in tikers_list:
                    df_candles = self.candels_download(ticker, self.start_time)
                    if not df_candles.empty:
                        df_candles.insert(0, 'TIKERS_NAME', ticker)
                        df_candles.insert(0, 'ASSETCODE', self.assetcode_mapping[ticker[:2]])
                        df_all.append(df_candles)
            print(f'Обработали: {short_tiker}. Осталось: {len(doble_tikers) - doble_tikers.index(short_tiker)}')
        if df_all:
            return pd.concat(df_all, ignore_index=True)
        else:
            return pd.DataFrame()
        
    def get_securities_df(self):
        link = f'https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities.json'
        response = self._safe_request(link)
        data = response.json()
        columns = data['securities']['columns']
        rows = data['securities']['data']
        df = pd.DataFrame(rows, columns=columns)
        securities_df_list = list(set(self.get_base_asset()['ASSETCODE'].to_list()))
        return df[df['ASSETCODE'].isin(securities_df_list)]

    def find_data(self):
        if self.type_data == 'history':
            if self.type_history == 'last_securities':
                return self.get_history_last_df()
            elif self.type_history == 'all_securities':
                return self.get_history_df()
        elif self.type_data == 'candels':
            if self.type_history == 'last_securities':
                return self.get_candels_last_df()
            elif self.type_history == 'all_securities':
                return self.get_candels_df()
        elif self.type_data == 'securities':
            return self.get_securities_df()
    
    def candels_download(self, secid, start_day):
        all_data = []
        start_idx = 0
        while True:
            link = f'https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/{secid}/candles.json?from={start_day}&till={self.end_time}&interval={self.interval}&start={start_idx}'
            response = self._safe_request(link)
            if response is None:
                break
            data = response.json()
            columns = data['candles']['columns']
            rows = data['candles']['data']
            if not rows:
                break
            df = pd.DataFrame(rows, columns=columns)
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
            all_data.append(df)
            if len(rows) < 500:
                break
            start_idx += 500
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        else:
            return pd.DataFrame()

    def find_all_tikers(self, short_tiker, month, year_start, year_past):
        tikers_list = []
        current_year = year_start
        max_year = year_past +2
        while year_start < max_year :
            if year_start == current_year:
                for month in range(month,13):
                    month_code = self.MONTH_CODES[month]
                    tikers_list.append(f'{short_tiker}{month_code}{year_start}')
            else:
                for month in range(1,13):
                    month_code = self.MONTH_CODES[month]
                    tikers_list.append(f'{short_tiker}{month_code}{year_start}')
            year_start += 1
        return tikers_list

    def history_last_dowload(self,secid,start_date,end_date):
        all_data = []
        start_idx = 0
        while True:
            link = f'https://iss.moex.com/iss/history/engines/futures/markets/forts/securities/{secid}.json?from={start_date}&till={end_date}&start={start_idx}'
            response = self._safe_request(link)
            if response is None:
                break
            data = response.json()
            columns = data['history']['columns']
            rows = data['history']['data']
            if not rows:
                break
            df = pd.DataFrame(rows, columns=columns)
            all_data.append(df)
            if len(rows) < 100:
                break
            start_idx += 100
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        else:
            return pd.DataFrame()
        
    def history_dowload(self,start_day, end_date):
        all_data = []
        current_date = pd.to_datetime(start_day)
        end_date = pd.to_datetime(end_date)
        
        while current_date <= end_date:
            start_idx = 0
            date_str = current_date.strftime('%Y-%m-%d')
            
            print(f"Загружаем данные на дату {date_str}")
            while True:
                link = f'https://iss.moex.com/iss/history/engines/futures/markets/forts/securities.json?date={date_str}&start={start_idx}'
                response = self._safe_request(link)
                if response is None:
                    break
                data = response.json()
                columns = data['history']['columns']
                rows = data['history']['data']
                if not rows:
                    break
                df = pd.DataFrame(rows, columns=columns)
                all_data.append(df)
                if len(rows) < 100:
                    break
                start_idx += 100
            current_date += pd.Timedelta(days=1)
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        else:
            return pd.DataFrame()

some = Futures_output_data(
    type_data = 'history',           # Доступны: иторические - history, свечи - candels, текущие - securities
    type_history = 'last_securities', # Только ближайшие - last_securities. Все - all_securities
    tikers = 'AFKS, AFLT, ALRS, ASTR, BANE, BELUGA, BSPB, CBOM, CHMF, ENPG, FEES, FESH, FLOT, GAZR, GMKN, HEAD, HYDR, IRAO, ISKJ, IVAT, KMAZ, LEAS, LKOH, MAGN, MDMG, MGNT, MOEX, MTLR, MTSI, MVID, NLMK, NOTK, NOTKM, OZON, PHOR, PIKK, PLZL, PLZLM, POLY, POSI, RASP, RENI, RNFT, ROSN, RTKM, RTKMP, RUAL, SBPR, SBRF, SFIN, SGZH, SIBN, SMLT, SNGP, SNGR, SOFL, SPBE, SVCB, T, TATN, TATP, TCSI, TRNF, UPRO, VKCO, VTBR, WUSH, X5, YDEX',                  # all - все тикеры, "через запятую" - определененные тикеры (название базового актива)
    start_time = '2020-01-01',       # Начало отсчета (не используется для securiteis)
    end_time = '2026-02-01',         # Конец отсчета включительно (не используется для sedurities)
    interval= 24                     # Интервал свечей: 1, 10, 60, 24
)

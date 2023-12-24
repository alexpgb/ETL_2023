import pandas as pd
import numpy as np
import logging
from pathlib import Path


def load_scv_file_to_dataframe(full_name: str) -> pd.DataFrame:
    return pd.read_csv(full_name)


def separate_val(df: pd.DataFrame, col_name: str, new_col_name: str, func)-> None:
    if col_name in df.columns:
        if new_col_name in df.columns:
            df.drop(columns=[new_col_name], inplace=True)
        df[new_col_name] =  df[col_name].apply(func)#str[:-3].astype(float)
    else:
        print(f'column {col_name} not exists.')


def process_file_hotel(path_to_file, file_name):    
    df = load_scv_file_to_dataframe(Path(path_to_file) / file_name)

    renamed_col = ['name', 'address', 'min_price', 'max_price']
    prefix = 'hotel_'
    df.rename(columns={el:prefix + el  for el in renamed_col}, inplace=True)

    col = prefix + 'min_price'
    suffix = '_sum'
    separate_val(df, col, col + suffix, lambda x: str(x)[:-3]) 
    df[col + suffix] =  df[col + suffix].astype(float)
    suffix = '_cur'
    separate_val(df, col, col + suffix, lambda x: str(x)[-3:]) 

    col = prefix + 'max_price'
    suffix = '_sum'
    separate_val(df, col, col + suffix, lambda x: str(x)[:-3]) 
    df[col + suffix] =  df[col + suffix].astype(float)
    suffix = '_cur'
    separate_val(df, col, col + suffix, lambda x: str(x)[-3:]) 

    df['stars'] = df['stars'].apply(lambda x: len(x.strip()))
    df['stars'].astype(int)
    logging.info(df.info())
    return df


def process_file_clients(path_to_file, file_name):    
    df = load_scv_file_to_dataframe(Path(path_to_file) / file_name)

    prefix = 'client_'
    df.rename(columns={col: prefix + col for col in df.columns if col != 'id'}, inplace=True)
    logging.info(df.info())
    return df


def process_file_booking(path_to_file, file_name):    
    df = load_scv_file_to_dataframe(Path(path_to_file) / file_name)
    df.rename(columns={'id':'booking_id', 
                       'from': 'booking_date_from',
                       'to': 'booking_date_to',
                       'price': 'booking_price',
                       'total_sum':'booking_total_sum'
                       }, inplace=True)

    col = 'booking_price'
    suffix = '_sum'
    df[col + '_null_tag'] = df[col].apply(lambda x: 1 if x is np.nan else 0)    
    separate_val(df, col, col + suffix, lambda x: 0 if x is np.nan else str(x)[:-3]) 
    df[col + suffix] =  df[col + suffix].astype(float)
    suffix = '_cur'
    separate_val(df, col, col + suffix, lambda x: np.nan if x is np .nan else str(x)[-3:]) 

    col = 'booking_total_sum'
    suffix = '_sum'
    df[col + '_null_tag'] = df[col].apply(lambda x: 1 if x is np.nan else 0)    
    separate_val(df, col, col + suffix, lambda x: 0 if x is np.nan else str(x)[:-3]) 
    df[col + suffix] =  df[col + suffix].astype(float)
    suffix = '_cur'
    separate_val(df, col, col + suffix, lambda x: np.nan if x is np .nan else str(x)[-3:]) 

    # df.rename(columns={'id':'booking_id', 'from': 'date_from', 'to': 'date_to'}, inplace=True)
    logging.info(df.info())
    return df

def merge_files(df_hotel: pd.DataFrame, df_clients: pd.DataFrame, df_booking: pd.DataFrame)-> pd.DataFrame:
    return df_booking.merge(df_hotel, how='left', left_on='hotel_id', right_on='id').drop(columns=['id'])\
                .merge(df_clients, how='left', left_on='client_id', right_on='id').drop(columns=['id'])


def add_col_sum_rub(df: pd.DataFrame, df_cur_rate:pd.DataFrame, col_name_prefix: str, col_name_suffix: str, is_null_tag_exists: bool, null_tag_suffix:str ='_null_tag'):
#     col_name_prefix = 'booking_price'
    if is_null_tag_exists:
        cond = df[col_name_prefix + null_tag_suffix] == 0
    else:
        cond = df[df.columns[0]] == False
        cond.replace(False, True, inplace=True)
    # добавим колонку с курсом в датафрейм
    col_name_suffix_rate = '_rate'
    if col_name_prefix + col_name_suffix_rate in df.columns: # удалим колонку с курсом, если она есть в датафрейме.
            df.drop(columns=col_name_prefix + col_name_suffix_rate, inplace=True)
    df.loc[cond, [col_name_prefix + col_name_suffix_rate]] = \
    df.loc[cond, [col_name_prefix + '_cur']].\
                merge(df_cur_rate, how='left', left_on=col_name_prefix + '_cur', right_on='cur_name')['cur_rate'].to_list()
    # добавим и рассчитаем колонку с суммой в рублях
    col_name_suffix = '_sum_rub'
    if col_name_prefix + col_name_suffix in df.columns: # удалим колонку с суммой.руб., если она есть в датафрейме.
        df.drop(columns=col_name_prefix + col_name_suffix, inplace=True)
    # создадим и рассчитаем значение колонки
    df.loc[cond, [col_name_prefix + col_name_suffix]] = \
        (df.loc[cond, col_name_prefix + '_sum'] * df.loc[cond, col_name_prefix + '_rate']).round(2)
        
def transform(path_to_source_files: str, source_files: dict, \
              path_to_target_file: str, target_file: str) ->str:
    """
    Обрабатывает файлы и сохранет результат в формата csv.
    """
    result_code = 0
    message = ''
    map_file_name_to_proc={'hotel': process_file_hotel,
                           'clients': process_file_clients,
                           'booking': process_file_booking}
    logging.info(map_file_name_to_proc)
    for item in source_files:
        source_files[item]['df'] = map_file_name_to_proc[item](path_to_source_files, source_files[item]['file'])
    df = merge_files(source_files['hotel']['df'],\
                     source_files['clients']['df'],\
                     source_files['booking']['df'])
    # пересчитаем суммы в рубли
    df_cur_rate = load_scv_file_to_dataframe(Path('/home/gbss/course_de_etl/HW8/currency_rates') / 'cur_rate.csv')
    
    logging.info('processing booking_price')
    add_col_sum_rub(df, df_cur_rate,
                col_name_prefix='booking_price', 
                col_name_suffix='_sum_rub', 
                is_null_tag_exists=True, 
                null_tag_suffix='_null_tag') 

    logging.info('booking_total_sum')    
    add_col_sum_rub(df, df_cur_rate, 
                col_name_prefix='booking_total_sum', 
                col_name_suffix='_sum_rub', 
                is_null_tag_exists=True, 
                null_tag_suffix='_null_tag')
    
    logging.info('hotel_min_price')
    add_col_sum_rub(df, df_cur_rate, 
                col_name_prefix='hotel_min_price', 
                col_name_suffix='_sum_rub', 
                is_null_tag_exists=False, 
                null_tag_suffix='_null_tag')
    
    logging.info('hotel_max_price')
    add_col_sum_rub(df, df_cur_rate, 
                col_name_prefix='hotel_max_price', 
                col_name_suffix='_sum_rub', 
                is_null_tag_exists=False, 
                null_tag_suffix='_null_tag')

    df.to_csv(Path(path_to_target_file) / target_file, encoding='utf-8', index=False)
    logging.info(df.info())
    return 0, 'OK'

def main():
    path_to_files = '/home/gbss/course_de_etl/HW8/downloads'
    path_to_target_file = '/home/gbss/course_de_etl/HW8/merged'
    FILES_NAME_FOR_DOWNLOAD = {
                            'hotel':{'file': 'hotel.csv'},
                               'clients': {'file': 'clients.csv'},
                               'booking': {'file': 'booking.csv'},
                                }
    
    transform(path_to_files, FILES_NAME_FOR_DOWNLOAD, path_to_target_file, 'booking_merged.csv')

if __name__ == '__main__':
    main()

#  #   Column                      Non-Null Count   Dtype  
# ---  ------                      --------------   -----  
#  0   booking_id                  752663 non-null  int64  
#  1   hotel_id                    752663 non-null  int64  
#  2   room_id                     752663 non-null  int64  
#  3   booking_price               743676 non-null  object 
#  4   booking_date_from           743676 non-null  object 
#  5   booking_date_to             752663 non-null  object 
#  6   total_days                  743676 non-null  float64
#  7   booking_total_sum           752663 non-null  object 
#  8   client_id                   752663 non-null  int64  
#  9   booking_price_null_tag      752663 non-null  int64  
#  10  booking_price_sum           752663 non-null  float64
#  11  booking_price_cur           743676 non-null  object 
#  12  booking_total_sum_null_tag  752663 non-null  int64  
#  13  booking_total_sum_sum       752663 non-null  float64
#  14  booking_total_sum_cur       752663 non-null  object 
#  15  hotel_name                  752663 non-null  object 
#  16  total_rooms                 752663 non-null  int64  
#  17  hotel_min_price             752663 non-null  object 
#  18  hotel_max_price             752663 non-null  object 
#  19  city                        752663 non-null  object 
#  20  hotel_address               752663 non-null  object 
#  21  stars                       752663 non-null  int64  
#  22  hotel_min_price_sum         752663 non-null  float64
#  23  hotel_min_price_cur         752663 non-null  object 
#  24  hotel_max_price_sum         752663 non-null  float64
#  25  hotel_max_price_cur         752663 non-null  object 
#  26  client_name                 752663 non-null  object 
#  27  client_profession           752663 non-null  object 
#  28  client_gender               752663 non-null  object 
#  29  client_email                752663 non-null  object 
#  30  client_address              752663 non-null  object 
#  31  client_age                  752663 non-null  int64  
#  32  client_phone                752663 non-null  object 
#  33  booking_price_rate          743676 non-null  float64
#  34  booking_price_sum_rub       743676 non-null  float64
#  35  booking_total_sum_rate      752663 non-null  float64
#  36  booking_total_sum_sum_rub   752663 non-null  float64
#  37  hotel_min_price_rate        752663 non-null  float64
#  38  hotel_min_price_sum_rub     752663 non-null  float64
#  39  hotel_max_price_rate        752663 non-null  float64
#  40  hotel_max_price_sum_rub     752663 non-null  float64
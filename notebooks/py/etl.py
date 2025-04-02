import pandas as pd
import numpy as np
import geopandas as gpd 
from mypackage import dir


# Environment variables
modality = 'u'
project = 'Ciencia de los datos'
data = dir.make_dir_line(modality, project) 
raw = data('raw')
processed = data('processed')


# Función para cargar datos csv
def cargar_datos(table_name: str) -> pd.DataFrame:
    df = pd.read_csv(raw / f'{table_name}.csv', sep = ',', decimal = '.', header = 0)
    print(f'Loaded table: {table_name}')
    return df


# Función para cargar datos sh
def cargar_datos_sh(table_name: str) -> pd.DataFrame:
    geo = gpd.read_file(raw / f'{table_name}.shp', encoding='utf-8')
    print(f'Loaded table: {table_name}')
    geo.geometry = geo.geometry.to_crs(epsg = 4326)
    return geo


# Función para transformar el censo
def transformar_censo(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    df['itter107'] = df['itter107'].astype(str)
    df = df[(df['sexistat1'] != 9) & (df['statciv2'] != 99) & (df['age'] != 'total')]
    df[['age', 'age2']] = df['age'].str.split(n=1, expand=True)
    df['age'] = df['age'].astype(int)
    df.drop(['tipo_dato15', 'demographic_data_type', 'eta1', 'time', 'select_time', 'flag_codes', 'flags', 'age2'], axis=1, inplace=True)
    return df


# Función para transformar los territorios
def transformar_territorios(df: pd.DataFrame) -> pd.DataFrame:
    df = df.loc[:,['itter107', 'territory']]
    df['territory'] = df['territory'].str.title()
    df = df.drop_duplicates()

    df['territory'] = df['territory'].str.replace('Centro (I)','Centro')
    df['territory'] = df['territory'].str.replace('Trentino Alto Adige / Südtirol','Trentino-Alto Adige')
    df['territory'] = df['territory'].str.replace('Friuli-Venezia Giulia','Friuli Venezia Giulia')
    df['territory'] = df['territory'].str.replace('Provincia Autonoma Trento','Trentino-Alto Adige')
    df['territory'] = df['territory'].str.replace('Pesaro E Urbino','Pesaro e Urbino')
    df['territory'] = df['territory'].str.replace('Monza E Della Brianza','Monza e della Brianza')

    df.loc[(df['territory'] == 'Valle D\'Aosta / Vallée D\'Aoste') 
           & (df['itter107'] == 'ITC2'), 'territory'] = 'Valle d\'Aosta'
    df.loc[(df['territory'] == 'Bolzano / Bozen') 
           & (df['itter107'] == 'ITD10'), 'territory'] = 'Bolzano'
    df.loc[(df['territory'] == 'Valle D\'Aosta / Vallée D\'Aoste') 
           & (df['itter107'] == 'ITC20'), 'territory'] = 'Aosta'
    df.loc[(df['territory'] == 'Reggio Nell\'Emilia')
           & (df['itter107'] == 'ITD53'), 'territory'] = 'Reggio nell\'Emilia'
    df.loc[(df['territory'] == 'Forlì-Cesena') 
           & (df['itter107'] == 'ITD58'), 'territory'] = 'Forli\'-Cesena'
    df.loc[(df['territory'] == 'Massa-Carrara') 
           & (df['itter107'] == 'ITE11'), 'territory'] = 'Massa Carrara'
    df.loc[(df['territory'] == 'Reggio Di Calabria') 
           & (df['itter107'] == 'ITF65'), 'territory'] = 'Reggio di Calabria'
    return df  


# Función para cargar los datos en la base de datos
def cargar_en_db_geo(df: pd.DataFrame, table_name: str) -> None:
    df.to_file(processed/f'{table_name}.geojson', driver='GeoJSON')  
    print(f'Saved table: {table_name}')


def cargar_en_db(df: pd.DataFrame, table_name: str) -> None:
    df.to_parquet(processed/f'{table_name}.parquet.gzip', compression='gzip') 
    print(f'Saved table: {table_name}')


if __name__ == '__main__':
    
    # ETL censo
    df = cargar_datos('DCIS_POPRES')
    censo_tranformado = transformar_censo(df)
    territorios_transformados = transformar_territorios(censo_tranformado)
    cargar_en_db(censo_tranformado, 'censo_tranformado')
    cargar_en_db(territorios_transformados, 'territorios_transformados')
    

    # ETL comunas
    geo_comunas = cargar_datos_sh('Limiti01012023_g/Com01012023_g/Com01012023_g_WGS84')
    df_geo = geo_comunas.copy()
    df_geo = df_geo.loc[:,['geometry','COD_CM']]
    df_texto = geo_comunas.copy()
    df_texto = df_texto.loc[:,['COD_CM','COMUNE']]
    df_texto = df_texto.merge(territorios_transformados, left_on=['COMUNE'], right_on=['territory'], how='left')
    df_texto.drop(['territory'], axis=1, inplace=True)
    cargar_en_db_geo(df_geo, 'comunas')
    cargar_en_db(df_texto, 'comunas')

    # ETL provincias
    geo_provinces = cargar_datos_sh('Limiti01012023_g/ProvCM01012023_g/ProvCM01012023_g_WGS84')
    df_geo = geo_provinces.copy()
    df_geo = df_geo.loc[:,['geometry','COD_PROV']]
    df_texto = geo_provinces.copy()
    df_texto['DEN_PROV'] = np.where(df_texto['DEN_PROV'] == '-', df_texto['DEN_CM'], df_texto['DEN_PROV'])
    df_texto = df_texto.loc[:,['COD_PROV','DEN_PROV']]
    df_texto = df_texto.merge(territorios_transformados, left_on=['DEN_PROV'], right_on=['territory'], how='left')
    df_texto.drop(['territory'], axis=1, inplace=True)
    cargar_en_db_geo(df_geo, 'provincias')
    cargar_en_db(df_texto, 'provincias')

    # ETL regiones
    geo_regions = cargar_datos_sh('Limiti01012023_g/Reg01012023_g/Reg01012023_g_WGS84')
    df_geo = geo_regions.copy()
    df_geo = df_geo.loc[:,['geometry','COD_REG']]
    df_texto = geo_regions.copy()
    df_texto = df_texto.loc[:,['COD_REG','DEN_REG']]
    df_texto = df_texto.merge(territorios_transformados, left_on=['DEN_REG'], right_on=['territory'], how='left')
    df_texto.drop(df_texto[df_texto['itter107'] == 'ITDA'].index, inplace = True)
    df_texto.drop(['territory'], axis=1, inplace=True)
    cargar_en_db_geo(df_geo, 'regiones')
    cargar_en_db(df_texto, 'regiones')

    # ETL regiones agrupadas
    geo_ripregions = cargar_datos_sh('Limiti01012023_g/RipGeo01012023_g/RipGeo01012023_g_WGS84')
    df_geo = geo_ripregions.copy()
    df_geo = df_geo.loc[:,['geometry','COD_RIP']]
    df_texto = geo_ripregions.copy()
    df_texto = df_texto.loc[:,['COD_RIP','DEN_RIP']]
    df_texto = df_texto.merge(territorios_transformados, left_on=['DEN_RIP'], right_on=['territory'], how='left')
    df_texto.drop(['territory'], axis=1, inplace=True)
    cargar_en_db_geo(df_geo, 'ripregiones')
    cargar_en_db(df_texto, 'ripregiones')

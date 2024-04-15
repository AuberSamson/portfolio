import cdsapi
import zipfile
from netCDF4 import Dataset
import pandas as pd
import os
import xarray as xr
import numpy as np
from shapely.geometry import Point
import geopandas as gpd
from math import cos, sin, radians

# Fonction pour ajuster la colonne 'time'
def adjust_time_column(year, time_column):
    # Remplacer l'année "2043" par l'année correcte
    new_date = np.char.replace(time_column.astype(str), '2043', str(year))
    
    return new_date

# Fonction pour extraire l'année à partir du nom du fichier
def extract_year_from_filename(filename):
    # Supposons que le nom du fichier suit le format 'rcp85_mid_century-msl-2050-v0.0.nc'
    parts = filename.split('-')
    # L'année est l'avant-dernière partie avant l'extension '.nc'
    year_part = parts[-2].split('.')[0]
    return int(year_part)


# Étape 1 : Connexion à l'API Copernicus et récupération des fichiers "sea level" au format zip
c = cdsapi.Client(key="260541:49c5eb05-d60f-432a-a99d-65d274478ddf", url="https://cds.climate.copernicus.eu/api/v2")

c.retrieve(
    'sis-water-level-change-timeseries',
    {
        'format': 'zip',
        'variable': 'mean_sea_level',
        'experiment': 'rcp8_5',
        'year': [
            '2041', '2042', '2043',
            '2044', '2045', '2046',
            '2047', '2048', '2049',
            '2050'
        ],
    },
    'copernicus_extract/sea_level/copernicus_sea_level_1.zip')

c.retrieve(
    'sis-water-level-change-timeseries',
    {
        'format': 'zip',
        'variable': 'mean_sea_level',
        'experiment': 'rcp8_5',
        'year': [
            '2051', '2052', '2053',
            '2054', '2055', '2056',
            '2057', '2058', '2059',
            '2060'
        ],
    },
    'copernicus_extract/sea_level/copernicus_sea_level_2.zip')

c.retrieve(
    'sis-water-level-change-timeseries',
    {
        'format': 'zip',
        'variable': 'mean_sea_level',
        'experiment': 'rcp8_5',
        'year': [
            '2061', '2062', '2063',
            '2064', '2065', '2066',
            '2067', '2068', '2069',
            '2070'
        ],
    },
    'copernicus_extract/sea_level/copernicus_sea_level_3.zip')

c.retrieve(
    'sis-water-level-change-timeseries',
    {
        'format': 'zip',
        'variable': 'mean_sea_level',
        'experiment': 'rcp4_5',
        'year': [
            '2071', '2072', '2073',
            '2074', '2075', '2076',
            '2077', '2078', '2079',
            '2080'
        ],
    },
    'copernicus_extract/sea_level/copernicus_sea_level_4.zip')

c.retrieve(
    'sis-water-level-change-timeseries',
    {
        'format': 'zip',
        'variable': 'mean_sea_level',
        'experiment': 'rcp4_5',
        'year': [
            '2081', '2082', '2083',
            '2084', '2085', '2086',
            '2087', '2088', '2089',
            '2090'
        ],
    },
    'copernicus_extract/sea_level/copernicus_sea_level_5.zip')

c.retrieve(
    'sis-water-level-change-timeseries',
    {
        'format': 'zip',
        'variable': 'mean_sea_level',
        'experiment': 'rcp4_5',
        'year': [
            '2091', '2092', '2093',
            '2094', '2095', '2096',
            '2097', '2098', '2099',
            '2100'
        ],
    },
    'copernicus_extract/sea_level/copernicus_sea_level_6.zip')


# Liste des noms de fichiers ZIP
zip_file_paths = ['copernicus_extract/sea_level/copernicus_sea_level_1.zip',
                  'copernicus_extract/sea_level/copernicus_sea_level_2.zip',
                  'copernicus_extract/sea_level/copernicus_sea_level_3.zip',
                  'copernicus_extract/sea_level/copernicus_sea_level_4.zip',
                  'copernicus_extract/sea_level/copernicus_sea_level_5.zip',
                  'copernicus_extract/sea_level/copernicus_sea_level_6.zip']

# Chemin de destination pour extraire le contenu
extract_path = 'copernicus_extract/sea_level'

# Etape 2 : Boucle pour extraire le contenu des fichiers ZIP
for zip_path in zip_file_paths:
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)    

# Liste des années
years_1 = list(range(2041, 2071))
years_2 = list(range(2071, 2101))

# Liste pour stocker les jeux de données
datasets = []

# Etape 3 : Charger les fichiers NetCDF et ajuster la colonne 'time'
for year in years_1:
    file_path = f'copernicus_extract/sea_level/rcp85_mid_century-msl-{year}-v0.0.nc'
    ds_1 = xr.open_dataset(file_path)
    
    # Ajuster la colonne 'time' en utilisant l'année extraite du nom du fichier
    ds_1['time'] = xr.apply_ufunc(adjust_time_column, year, ds_1['time'], dask='allowed')
    
    # Ajouter le jeu de données à la liste
    datasets.append(ds_1)

for year in years_2:
    file_path = f'copernicus_extract/sea_level/rcp45_end_century-msl-{year}-v0.0.nc'
    ds_2 = xr.open_dataset(file_path)
    
    # Ajuster la colonne 'time' en utilisant l'année extraite du nom du fichier
    ds_2['time'] = xr.apply_ufunc(adjust_time_column, year, ds_2['time'], dask='allowed')
    
    # Ajouter le jeu de données à la liste
    datasets.append(ds_2)    

# Concaténer les jeux de données
ds = xr.concat(datasets, dim='time')    

# Étape 4 : Enregistrer le sous-ensemble dans un nouveau fichier netCDF
ds.to_netcdf('copernicus_extract/sea_level/copernicus_sealevel.nc')

# Étape 5 : Convertir le sous-ensemble en DataFrame
dataset = ds.to_dataframe().reset_index()

# Renommer les colonnes 'station_y_coordinate' en 'lat' et 'station_x_coordinate' en 'lon'
dataset = dataset.rename(columns={'station_y_coordinate': 'lat', 'station_x_coordinate': 'lon'})

# Ajuster les longitudes de 0 à 360 vers -180 à 180
dataset['lon'] = np.where(dataset['lon'] > 180, dataset['lon'] - 360, dataset['lon'])

# Filtrer les latitudes et longitudes autour de la France
dataset = dataset[(dataset['lat'] >= 40) & (dataset['lat'] <= 52) & (dataset['lon'] >= -7) & (dataset['lon'] <= 11)]

# Étape 6 : Enregistrer le DataFrame au format CSV
dataset.to_csv('copernicus_sealevel.csv', index=False)

# Charger le fichier GeoJSON des communes
communes = gpd.read_file('communes-version-simplifiee.geojson')

# Créer une colonne 'geometry' contenant les cercles de 1km autour des points 'lat' et 'lon'
geometry = [Point(lon, lat).buffer(0.025) for lon, lat in zip(dataset['lon'], dataset['lat'])]

# Convertir le DataFrame en GeoDataFrame
gdf_dataset = gpd.GeoDataFrame(dataset, geometry=geometry, crs='EPSG:4326')

# Effectuer une jointure spatiale avec les communes
points_communes = gpd.sjoin(gdf_dataset, communes, how='left', predicate='intersects')

# Supprimer les doublons dans le résultat en utilisant l'index
points_communes = points_communes[~points_communes.index.duplicated(keep='first')]

# Ajouter la colonne 'commune' dans le fichier CSV des points
dataset['commune'] = points_communes['nom']
dataset['code'] = points_communes['code']

# Supprimer les lignes où les valeurs de la colonne 'commune' sont nulles
dataset = dataset.dropna(subset=['commune'])

# Enregistrer le résultat au format CSV
dataset.to_csv('copernicus_sealevel_geo.csv', index=False)

# Afficher les premières lignes du dataset
print(dataset.head())


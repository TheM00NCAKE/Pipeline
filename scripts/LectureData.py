
import sqlite3
import sys
import pandas as pd
import copernicusmarine
import xarray as xr
from pathlib import Path
import time

chemin=str((Path(__file__).parent).parent / 'data'/'db')
sys.path.append(chemin)

def fds():
  """Récupère les données statiques de la base de données db."""
  d=time.time()
  con=sqlite3.connect(chemin) 
  df2=pd.read_sql_query('select * from ValsStatiques',con) # sélectionner un océan précis : where ID_TypeOcean=valeur
  df2.set_index(['longitude', 'latitude'], inplace=True) 
  con.close()
  f=time.time()
  print('données statiques:', f-d)
  return df2

def fdd(min,max,valeur):
  """Récupère les données dynamiques sur copernicusmarine.
  Utilise des méthodes de cette librairies afin de récupérer les données sur leur API externe, 
  et modifie le résultat (qui est une dataset Xarray) pour qu'elle puisse mieux correspondre à 
  ce dont nous avons besoin"""
  d=time.time()
  cred = Path.home() / ".copernicusmarine" / ".copernicusmarine-credentials"
  if not cred.exists():
      copernicusmarine.login('rramdane', 'Ab12345@')
  #va chercher les données et le retourne sous forme d'un dataset Xarray
  df=copernicusmarine.open_dataset(
    dataset_id="cmems_mod_glo_phy_my_0.083deg_P1M-m",
    variables=[valeur], #thetao : température , so : salinité
    minimum_longitude=min,    
    maximum_longitude=max,   
    minimum_latitude=-80,       
    maximum_latitude=80,        
    start_datetime="2001-09-01 00:00:00",          #le début de la mesure sous forme annee-mois-JourTheures:minutes:secondes
    end_datetime="2001-09-01 00:00:00",            #la fin de la mesure sous forme annee-mois-JourTheures:minutes:secondes
    minimum_depth=0.49402499198913574,    
    maximum_depth=0.49402499198913574,          
  )[valeur].isel(time=0, depth=0) #les vals dépendent de long, lat, depth et time, mais depth et time ne servent à rien ici donc on les enleve
  f=time.time()
  print('données dynamique :', f-d)
  return df

def prepa(dfs):
  """Permet de récupérer les données sous forme de liste Numpy à 
  partir d'une DataFrame : Numpy traite les données plus rapidement, car elles sont vectorielles
  (contrairement à une boucle for, elle traite sur plusieurs données en même temps)"""
  lon=dfs.index.get_level_values("longitude").to_numpy() #on récupère les données statiques de dfs et on les attribut à lon et lat
  lat=dfs.index.get_level_values("latitude").to_numpy()
  dfs=dfs.reset_index()
  return lon,lat,dfs

def test(dfs,lon,lat,dfd,valeur):
  """Permet de fusionner les deux ensembles de données en attribuant une nouvelle colonne 
  à la DataFrame statique. Celle ci est attribué à l'aide d'une méthode qui permet de positionner 
  les valeurs en fonction de leur longitude et latitude. """
  #on créer a qui va attribuer les valeurs aux lat et lon appropriés
  dfs[valeur]=dfd.sel(longitude=xr.DataArray(lon),latitude=xr.DataArray(lat),)
  return dfs

from concurrent.futures import ThreadPoolExecutor
import LectureData
import time
import numpy as np
from PIL import Image
from pathlib import Path
import sys
import psutil, os

chemin=str((Path(__file__).parent).parent/'www'/'assets')
sys.path.append(chemin)

cache={} #dictionnaire qui fait office de "cache"

def changecoords(points,width,height):
    """Modifie les coordonnées de sorte à ce que celles ci puissent désigner correctement 
    les pixels d'une image en fonction des valeurs des coordonnées."""
    points['longitude']=((points['longitude'].astype(float)+180)*width/360).astype(int)
    points['latitude']=((90-points['latitude'].astype(float))*height/180).astype(int)

def colorer(points,canvas,val):
    """Colore chaque pixel d'une image en fonction de ses coordonnées et de sa valeur. 
    En fonction des indicateurs, l'échelle des couleurs et les calculs à réaliser pour attribuer
    des couleurs seront différentes. Utilise une image transformée en liste Numpy contenant les coordonnées et 
    la valeur (couleur rgb) de chaque pixel, et attribue donc chaque couleur à chaque pixel de la liste"""
    x=points['longitude'].to_numpy().astype(int)
    y=points['latitude'].to_numpy().astype(int)
    if val=='so':
        so=np.array(points['so'])
        r=np.where(so>30,127-((so-29)*7).astype(int),255)  #>-2 pour thetao (temperature), >30 pour so (salinité)
        g=np.where(so>30,216-((so-29)*8).astype(int),255)
        b=np.where(so>30,256-((so-29)*9).astype(int),255)
    else:
        thetao=np.array(points['thetao'])
        r=np.where(thetao<14,0,np.where(thetao<20,16+(thetao-14)*35,237+(thetao-16) ))  #>-2 pour thetao (temperature), >30 pour so (salinité)
        g=np.where(thetao<14,23+(thetao*11).astype(int),np.where(thetao<20,242,230-(thetao-19)*16))
        b=np.where(thetao<14,255,np.where(thetao<20,39-(thetao-14)*4,0))
        #max : 33.198858596384525 min : -2.3547166138887405 moyenne : 15.133930898373574
        #très froid : 0, 34, 255 , moins froid : 0 222 255  
        #froid : 16 242 39, moyen : 227 242 16 
        #chaud : 243 160 18, très chaud : 243 18 18     127 
    clrs=np.stack([r,g,b],axis=1)
    canvas[y,x]=clrs

def processus(min,max,valeur):
    """Processus qui permet d'exécuter l'entiereté des étapes afin de modéliser les données.
    Utilise des Threads afin d'exécuter différents processus en même temps, et utilise un système de 
    cache pour simuler la mise en cache des données statiques"""
    if 'ValsStatiques' not in cache: #si le cache est vide alors on exec fds, après on aura plus besoin de l'exec
        cache['ValsStatiques']=LectureData.fds()
    with ThreadPoolExecutor() as executeur:
        future_dfd=executeur.submit(LectureData.fdd,min,max,valeur) 
        future_coords=executeur.submit(LectureData.prepa,cache['ValsStatiques'])
        img=Image.open(chemin+f'/ImgTemp{valeur.upper()}.png')
        canvas=np.array(img)[:,:,:3]
        width,height=img.size
        lon,lat,dfs=future_coords.result()
    with ThreadPoolExecutor(max_workers=9) as executeur2:
        points=LectureData.test(dfs,lon,lat,future_dfd.result(),valeur)
        changecoords(points,width,height)
        unsx=len(points)//6
        executeur2.submit(colorer,points[0:unsx],canvas,valeur)
        executeur2.submit(colorer,points[unsx:unsx*2],canvas,valeur)
        executeur2.submit(colorer,points[unsx*2:unsx*3],canvas,valeur)
        executeur2.submit(colorer,points[unsx*3:unsx*4],canvas,valeur)
        executeur2.submit(colorer,points[unsx*4:unsx*5],canvas,valeur)
        executeur2.submit(colorer,points[unsx*5:],canvas,valeur)
    Image.fromarray(canvas).save(chemin+f"/ImgTest{valeur.upper()}.png",compress_level=0)


#Je mettrais ca dans un main plus tard (+ je modifierais le code en mettant certaines fonctions dans des fichiers différents etc...
def main():
    for i in range(0,3):
        debut=time.time() 
        valeur="thetao"
        processus(-180,180,valeur)
        fin2=time.time()
        print(fin2-debut)
        process = psutil.Process(os.getpid())
        print("Mémoire utilisée (Mo):", process.memory_info().rss / (1024 * 1024))

main()
"""
Récupération des données dynamiques :
      read_dataframe (récup les données et les transforme en DataFrame) : 11.269212245941162 - 13.964303731918335
      open_dataset (format Xarray, peu de transformation) quand on fait autre chose en attendant : 7.9101502895355225 - 8.26500153541565
      open_dataset quand on reste sur VSCODE ou quand ce sera déployé sur serveur : 3.883848190307617t - 5s
Debit sur Eduroam environ 130 mbps (quand y'a pas bcp de gens), exec total : 8.344956159591675s
"""

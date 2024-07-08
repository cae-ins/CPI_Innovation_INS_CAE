
#Exportation de la donnée Finale
import pandas as pd
import os
from datetime import datetime
catExcel = pd.read_excel('C:/Users/Dell/Documents/UB/IPC/CODE_IPC/GBEKE_postulant.xlsx')
chemin_fichier_collecte = os.path.join('C:/Users/Dell/Documents/UB/IPC/CODE_IPC/COLLECTE_JOURNALIERE','DATA_TEST'+datetime.now().strftime('%d%m%Y')+'.xlsx')
catExcel.to_excel(chemin_fichier_collecte, index=False)

#C:/Users/Dell/Documents/UB/IPC/CODE_IPC/COLLECTE_JOURNALIERE

import pandas as pd
import os
import calendar

base_dir5 = os.path.join("/mnt","c","airflow","dags","WorkFlow_IHPC") 

def rapport_prix_moyen(annee_en_cours) :    

    def generer_mois_list(annee, mois_debut=1, mois_fin=12):
        mois_list = []
        for mois in range(mois_debut, mois_fin + 1):
            mois_list.append(f'{mois:02}{annee}')
        return mois_list

    def format_mois_annee(mois_annee):
        mois_num = int(mois_annee[:2])
        annee = mois_annee[2:]
        mois_nom = calendar.month_name[mois_num].lower()  
        return f'{mois_nom}_{annee}'

    
    #Répertoire de base où se trouvent les dossiers mensuels
    #base_dir = 'dossier_imputation'

    #Liste des mois (nom des dossiers)
    #mois_list = ['mars', 'Avril', 'mai', 'juin', 'juillet']

    mois_list = generer_mois_list(annee_en_cours)

    #Liste des catégories
    #categories = ['A_Data_Scrapping_']

    #Dictionnaire pour stocker les DataFrames par catégorie
    #data_dict = {cat: pd.DataFrame() for cat in categories}

    # Initialiser un DataFrame vide pour stocker les données consolidées
    #consolidated_df = pd.DataFrame()
    file_path = os.path.join(base_dir5, 'nommenclature_IHPC_ITS_V2.xlsx')

    consolidated_df = pd.read_excel(file_path, sheet_name='elementaire')

    #Boucle pour chaque mois et chaque catégorie
    for mois in mois_list:
        mois_dir = os.path.join(base_dir5,'traitement4', mois)
        mois_annee = format_mois_annee(mois)
        
        file_path = os.path.join(mois_dir, f'A_Data_Scrapping_{mois}.xlsx') 
        
        if os.path.exists(file_path):
            df = pd.read_excel(file_path)

            code_col = [col for col in df.columns if 'Code' in col and 'produit' in col]
            indice_col = [col for col in df.columns if 'Prix_du_produit' in col]
            
            #On suppose que les fichiers contiennent une colonne 'Code' et une colonne 'Indice'
            df = df[[code_col[0], indice_col[0]]]
            df = df.rename(columns={code_col[0]: 'Code'})
            df = df.rename(columns={indice_col[0]: mois_annee})  
            
            print("hablo")
            consolidated_df = pd.merge(consolidated_df, df, on='Code', how='left')


    output_path =os.path.join(base_dir5, 'Indices_Consolides_PM.xlsx') 

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:

        libelle_col = [col for col in consolidated_df.columns if 'libelle' in col]
        consolidated_df = consolidated_df.rename(columns={libelle_col[0]: 'libelle'})
        
        # Supprimer la colonne 'Code' et déplacer 'Libellé' au début
        consolidated_df.drop(columns=['Code'], inplace=True)
        libelle_col = consolidated_df.pop('libelle')
        consolidated_df.insert(0, 'libelle', libelle_col)                                                                                                                                                         
        consolidated_df.to_excel(writer, sheet_name='Prix_Moyen', index=False)

   


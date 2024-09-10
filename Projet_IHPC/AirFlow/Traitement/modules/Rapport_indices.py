import pandas as pd
import os
import calendar

base_dir4 = os.path.join("/mnt","c","airflow","dags","WorkFlow_IHPC") 

def rapport_indices(annee_en_cours) :

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


    #code_libelle_df = pd.read_excel("nommenclature IHPC ITS.xlsx")

    #Liste des mois (nom des dossiers)
    #mois_list = ['janvier','fevrier','mars', 'avril', 'mai', 'juin', 'juillet','aout','septembre','octobre','novembre','decembre']  

    mois_list = generer_mois_list(annee_en_cours)

    #Liste des catégories
    #categories = ['Indice_elementaire', 'Indice_Poste', 'Indice_SG', 'Indice_groupe','Indice_fonction']

    correspondance_path = os.path.join(base_dir4, 'nommenclature_IHPC_ITS_V2.xlsx' )

    correspondance_dict = pd.read_excel(correspondance_path, sheet_name=None)

    # Liste de catégories à traiter
    categories = ['fonction','groupe','SG','Poste','elementaire']

    #Dictionnaire pour stocker les DataFrames par catégorie
    #data_dict = {cat: pd.DataFrame() for cat in categories}

    data_dict = {cat: correspondance_dict[cat].copy() for cat in categories}

    #Boucle pour chaque mois et chaque catégorie
    for mois in mois_list:
        mois_dir = os.path.join(base_dir4, 'traitement4',mois)
        mois_annee = format_mois_annee(mois)
        
        for category in categories:
            file_path = os.path.join(mois_dir, f'Indice_{category}.xlsx') 
            
            if os.path.exists(file_path): 

                df = pd.read_excel(file_path)
                code_col = [col for col in df.columns if 'Code' in col and 'produit' in col]
                indice_col = [col for col in df.columns if 'Indice' in col]
                
                #On suppose que les fichiers contiennent une colonne 'Code' et une colonne 'Indice'
                df = df[[code_col[0], indice_col[0]]]
                df = df.rename(columns={code_col[0]: 'Code'})
                df = df.rename(columns={indice_col[0]: mois_annee})  

                df[mois_annee] = df[mois_annee] * 100

                # Lire le DataFrame de correspondance pour la catégorie actuelle
                #correspondance_df = data_dict[category]

                #correspondance_df["Code"] = correspondance_df["Code"].astype(str)
                #df["Code"] = df["Code"].astype(str)
                #df.columns = df.columns.str.strip()
                data_dict[category].columns = data_dict[category].columns.str.strip()
    
                # Fusionner avec le DataFrame des libellés
                #df = pd.merge(df, correspondance_df, left_on='Code', right_on='Code', how='left')
                
                #Fusionner les données pour cette catégorie 
                if data_dict[category].empty:
                    data_dict[category] = df
                else:
                    data_dict[category] = pd.merge(data_dict[category], df, on='Code', how='left')

    # Calcul des variations et ajout des colonnes de variation
    for category, df in data_dict.items():
        mois_columns = [col for col in df.columns if f'_{annee_en_cours}' in col]
        df[mois_columns] = df[mois_columns].astype(float)
        
        if len(mois_columns) >= 2:
            # Variation sur 1 mois
            df['Evolution_1_Mois'] = ((df[mois_columns[-1]] / df[mois_columns[-2]]) - 1) * 100
        if len(mois_columns) >= 3:
            # Variation sur 3 mois
            df['Evolution_3_Mois'] = ((df[mois_columns[-1]] / df[mois_columns[-3]]) - 1) * 100
        if len(mois_columns) >= 12:
            # Variation sur 12 mois
            df['Evolution_12_Mois'] = ((df[mois_columns[-1]] / df[mois_columns[0]]) - 1) * 100


    #Créer un classeur Excel avec des feuilles distinctes pour chaque catégorie
    output_path =os.path.join(base_dir4, 'Indices_Consolides.xlsx') 

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        for category, df in data_dict.items():
            libelle_col = [col for col in df.columns if 'libelle' in col]
            df = df.rename(columns={libelle_col[0]: 'libelle'})
            df.drop(columns=['Code'], inplace=True)
            libelle_col = df.pop('libelle')
            df.insert(0, 'libelle', libelle_col)
            df.to_excel(writer, sheet_name=category, index=False)
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import re
import numpy as np
from Correction import Correction_Prix
from Imputation import Imputation_Prix
from Rapport_indices import rapport_indices
from Rapport_prix_moyen import rapport_prix_moyen
import os

from dateutil.relativedelta import relativedelta

date_debut = datetime.strptime("01-05-2024", "%d-%m-%Y")  
date_fin = datetime.strptime("31-05-2024", "%d-%m-%Y") 
mois_en_cours = date_debut.strftime("%m%Y")
annee_en_cours = date_debut.strftime("%Y")
mois_precedentV1 = date_debut - relativedelta(months=1)
mois_precedentV2 = mois_precedentV1.strftime("%m%Y")

base_dir = os.path.join("/mnt","c","airflow","dags","WorkFlow_IHPC") 

# ETAPE 1 -------------------------------------------------------------------------------------------------------------------------------------------------------------

def correspondance():
    
    panier_df_path = os.path.join(base_dir,"Echantillon_Data_Scrapping_09032024_valide_code.xlsx")
    panier_df = pd.read_excel(panier_df_path)
    colonnes_a_comparer = ["Libelle_du_produit"] 

    #date_debut = datetime.strptime("01-07-2024", "%d-%m-%Y")  
    #date_fin = datetime.strptime("31-07-2024", "%d-%m-%Y")  

    # Traiter les intervalles de prix pour obtenir le plus petit prix
    def extract_min_price(value):
        """
        Extraire la valeur de gauche dans l'interval.
        """
        if isinstance(value, str):
            if "-" in value:
                parts = value.split("-")
                if len(parts) >= 2:
                    return int(parts[0].strip())
            elif " " in value:
                return int(value.split()[0])
        return value

    def extraction_nom_site(url):
        """
        Extrait le nom du site d'une URL complète.
        """
        sites = ['auchan', 'kevajo', 'ivoirshop']
        
        for site in sites:
            if site in url:
                return site
            
        return None
    
    global date_debut, date_fin

    while date_debut <= date_fin:
        date_str = date_debut.strftime("%d%m%Y")
        fichier_journalier = os.path.join(base_dir,"Data_Collecte",mois_en_cours, f"Data_Scrapping_{date_str}.xlsx")
        try:
            df_journalier = pd.read_excel(fichier_journalier)
            
            lignes_correspondantes = pd.merge(df_journalier, panier_df[['Libelle_du_produit', 'Code produit', 'Code']], how="inner", on=colonnes_a_comparer)

            lignes_correspondantes['Code_site'] = lignes_correspondantes['Code_site'].apply(lambda x: extraction_nom_site(str(x)) if pd.notnull(x) and x != 'adjovan' else x)
            lignes_correspondantes['Code_site'].fillna('adjovan', inplace=True)

            lignes_correspondantes["Prix_du_produit"] = lignes_correspondantes["Prix_du_produit"].fillna("")
            lignes_correspondantes["Prix_du_produit"] = lignes_correspondantes["Prix_du_produit"].apply(lambda x: x.replace(".", "") if isinstance(x, str) else x)
            lignes_correspondantes["Prix_du_produit"] = lignes_correspondantes["Prix_du_produit"].apply(lambda x: x.replace(",", "") if isinstance(x, str) else x)
            #lignes_correspondantes["Prix_du_produit"] = pd.to_numeric(lignes_correspondantes["Prix_du_produit"].str.replace("[^\d-]", "", regex=True), errors='coerce')
            #lignes_correspondantes["Prix_du_produit"] = lignes_correspondantes["Prix_du_produit"].apply(lambda x: float(x.split("-")[0]) if isinstance(x, str) and "-" in x else x)
            lignes_correspondantes["Prix_du_produit"] = lignes_correspondantes["Prix_du_produit"].apply(extract_min_price)
            
            fichier_sortie = os.path.join(base_dir,"traitement1",mois_en_cours,f"Data_Scrapping_{date_str}.xlsx")
            os.makedirs(os.path.dirname(fichier_sortie), exist_ok=True)
            lignes_correspondantes.to_excel(fichier_sortie, index=False)
            
        except FileNotFoundError:
            print(f"Le fichier {fichier_journalier} n'existe pas.")
    
        date_debut += timedelta(days=1)

# ETAPE 2 -------------------------------------------------------------------------------------------------------------------------------------------------------------


def Ajout_Variete(): 

    
    panier_df2_path = os.path.join(base_dir,"Panier_renomme.xlsx")
    panier_df2 = pd.read_excel(panier_df2_path)
    def clean_and_convert(value):
        if pd.isna(value):
            return np.nan
        elif isinstance(value, (int, float)):
            return int(value)  
        elif isinstance(value, str):
            try:
                numeric_part = re.search(r'\d+', value).group()  
                return int(numeric_part) 
            except (TypeError, AttributeError, ValueError):
                return np.nan  
    '''  
    def mean_ignore_nan(values):
        if values.isnull().all():
            return np.nan
        return np.nanmean(values)
    '''

    date_debut = datetime.strptime("01-05-2024", "%d-%m-%Y")  
    date_fin = datetime.strptime("31-05-2024", "%d-%m-%Y")

    while date_debut <= date_fin:
        date_str = date_debut.strftime("%d%m%Y")
        fichier_journalier = os.path.join(base_dir,"traitement1",mois_en_cours, f"Data_Scrapping_{date_str}.xlsx")
        try:
            df_journalier = pd.read_excel(fichier_journalier)
            
            df_journalier['Prix_du_produit'] = df_journalier['Prix_du_produit'].apply(clean_and_convert)
            df_journalier['Unite_monetaire'] ='CFA'
            #df_journalier['Date_de_collecte'] = pd.to_datetime(df_journalier['Date_de_collecte'])
            premiere_date = df_journalier.loc[0, 'Date_de_collecte'].split()[0]
            df_journalier['Date_de_collecte'] = premiere_date
            #donnees_groupees = df_journalier.groupby(['Date_de_collecte','Unite_monetaire','Libelle_du_produit']).agg({'Prix_du_produit': 'mean'}).reset_index()
            donnees_groupees2 = pd.merge(df_journalier, panier_df2[['Code produit','Type variété (HE, O1,O2,O3)']], how="inner", on="Code produit")
            #donnees_groupees = df_journalier.groupby(['Date_de_collecte', 'Unite_monetaire', 'Code produit']).agg({'Prix_du_produit': mean_ignore_nan}).reset_index()
            #print(f"Data_Scrapping_{date_str}.xlsx")
            fichier_sortie = os.path.join(base_dir,"traitement2",mois_en_cours,f"Data_Scrapping_{date_str}.xlsx")
            os.makedirs(os.path.dirname(fichier_sortie), exist_ok=True)
            donnees_groupees2.to_excel(fichier_sortie, index=False)
                
        except FileNotFoundError:
            print(f"Le fichier {fichier_journalier} n'existe pas.")
    
        date_debut += timedelta(days=1)


#ETAPE 3 --------------------------------------------------------------------------------------------------------------------------------------------------------------

def Ajout_Fourchette():

    # Calculer les fourchettes pour chaque produit
    def calculate_price_ranges(row):
        price = row['Prix_du_produit']
        
        if row['Type variété (HE, O1,O2,O3)'] == 'Heterogene':
            lower_bound = price * 0.85
            upper_bound = price * 1.15
        elif row['Type variété (HE, O1,O2,O3)'] == 'Homogene':
            lower_bound = price * 0.70
            upper_bound = price * 1.30
        else:
            raise ValueError(f"Variété non reconnue pour le produit {row['Libelle_du_produit']}")
        
        return f"{lower_bound:.2f}-{upper_bound:.2f}"

    path1 = os.path.join(base_dir,"traitement4",mois_precedentV2,"A_Data_Scrapping_"+mois_precedentV2+".xlsx")
    df_FDSM = pd.read_excel(path1)
    df_FDSM['fourchette'] =df_FDSM.apply(calculate_price_ranges, axis=1)
    path_out = os.path.join(base_dir, "traitement2",mois_precedentV2,"A_Data_Scrapping_"+mois_precedentV2+".xlsx")
    os.makedirs(os.path.dirname(path_out), exist_ok=True)
    df_FDSM.to_excel(path_out, index=False)

#ETAPE 4(CONTROL) -----------------------------------------------------------------------------------------------------------------------------------------------------------------

#ETAPE 5(IMPUTATION) --------------------------------------------------------------------------------------------------------------------------------------------------------------


#ETAPE 6 --------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def Fusion_Agragation_Mois_en_cours():

    #FUSION DE TOUS LES FICHIERS DU MOIS EN COURS
    dossier = os.path.join(base_dir,"traitement4",mois_en_cours)
    dfs = []

    for fichier in os.listdir(dossier):
            
            chemin_complet = os.path.join(dossier, fichier)
            df = pd.read_excel(chemin_complet)
            dfs.append(df)

    df_FDSM = pd.concat(dfs, ignore_index=True)

    #AGGREGATION DE TOUS LES FICHIERS DU MOIS EN COURS
    df_FDSM['Unite_monetaire'] ='CFA'
    df_FDSM['Prix_du_produit'] = pd.to_numeric(df_FDSM['Prix_du_produit'], errors='coerce')
    df_FDSM_valide = df_FDSM.dropna(subset=['Prix_du_produit'])
    df_FDSM_valide['Code produit'] = df_FDSM_valide['Code produit'].astype(str).str.slice(stop=-4)
    df_FDSM_A = df_FDSM_valide.groupby(['Code_site','Libelle_du_produit','Unite_monetaire','Code produit','Type variété (HE, O1,O2,O3)']).agg({'Prix_du_produit': 'mean'}).reset_index()
    #df_FDSM_A['fourchette'] =df_FDSM_A.apply(calculate_price_ranges, axis=1)
    path_out = os.path.join(base_dir,"traitement4", mois_en_cours ,"A_Data_Scrapping_"+mois_en_cours+".xlsx")
    df_FDSM_A.to_excel(path_out, index=False)

#ETAPE 7 -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def Calcul_Indice_Elemenetaire() :

    
    path2 = os.path.join(base_dir,"traitement4", mois_en_cours,"A_Data_Scrapping_"+mois_en_cours+".xlsx")
    df_FDSM_valide = pd.read_excel(path2)
    
    df_ref_path = os.path.join(base_dir,"Prix_Reference.xlsx")
    df_ref = pd.read_excel(df_ref_path)
    df_ref['Code variete'] = df_ref['Code variete'].astype(str)
    df_FDSM_valide['Code produit'] = df_FDSM_valide['Code produit'].astype(str)

    moyenne_prix_produits = df_FDSM_valide.groupby('Code produit')['Prix_du_produit'].mean().reset_index()
    moyenne_prix_produits.columns = ['Code produit', 'Moyenne_Prix_Produits']

    #moyenne_prix_produits = moyenne_prix_produits[moyenne_prix_produits['Code produit'].str.startswith(('10','20'))]
    #moyenne_prix_produits['Code produit'] = moyenne_prix_produits['Code produit'].astype(str).str.slice(stop=-4)

    moyenne_prix_produits_tron = moyenne_prix_produits.groupby('Code produit')['Moyenne_Prix_Produits'].mean().reset_index()
    #moyenne_prix_produits_tron.columns = ['Code produit', 'Moyenne_Prix_Produits']

    #df_combined = pd.merge(moyenne_prix_produits, df_ref, on='Code produit')
    df_combined = pd.merge(moyenne_prix_produits_tron, df_ref, left_on='Code produit', right_on='Code variete')
    df_combined['Indice'] = df_combined['Moyenne_Prix_Produits'] / df_combined['prix de base']

    out_final = os.path.join(base_dir,"traitement4",mois_en_cours,"Indice_elementaire.xlsx")
    df_combined.to_excel(out_final, index=False)


#ETAPE 8 ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def Calcul_Indice_Laspeyres():

   
    #POSTE
    Path_Indice_Elementaire = os.path.join(base_dir,"traitement4",mois_en_cours,"Indice_elementaire.xlsx")
    Indice_Elementaire = pd.read_excel(Path_Indice_Elementaire)
    path_pon = os.path.join(base_dir,"pondération produit alimentaire panier IHPC.xlsx")
    pon = pd.read_excel(path_pon)

    df_Elementaire= pd.merge(Indice_Elementaire, pon, left_on='Code produit', right_on='code var 2014')

    df_Elementaire['Produit'] = df_Elementaire['Indice'] * df_Elementaire['pond var']
    
    df_Elementaire['Code produit'] = df_Elementaire['Code produit'].astype(str).str.slice(stop=-2)
    #Indice_Post = df_post.groupby('Code produit')['Produit'].sum().reset_index()
    Indice_Post = df_Elementaire.groupby('Code produit').apply(lambda x: x['Produit'].sum() / x['pond var'].sum()).reset_index()
    Indice_Post.columns = ['Code_produit_poste', 'Indice_Poste']

    out_final_poste= os.path.join(base_dir,"traitement4",mois_en_cours,"Indice_Poste.xlsx")

    Indice_Post.to_excel(out_final_poste, index=False)


    #SOUS GROUPE
    Path_Indice_Poste = os.path.join(base_dir,"traitement4",mois_en_cours,"Indice_Poste.xlsx")
    Indice_Poste = pd.read_excel(Path_Indice_Poste)
    path_pon = os.path.join(base_dir,"Ponderation_Poste.xlsx")
    pon = pd.read_excel(path_pon)

    df_post= pd.merge(Indice_Poste, pon, left_on='Code_produit_poste', right_on='Code_produit_poste')

    df_post['Produit'] = df_post['Indice_Poste'] * df_post['Pon_Poste']

    df_post['Code_produit_poste'] = df_post['Code_produit_poste'].astype(str).str.slice(stop=-2)
    Indice_SG = df_post.groupby('Code_produit_poste').apply(lambda x: x['Produit'].sum() / x['Pon_Poste'].sum()).reset_index()
    Indice_SG.columns = ['Code_produit_SG', 'Indice_SG']

    out_final_SG = os.path.join(base_dir,"traitement4",mois_en_cours,"Indice_SG.xlsx")

    Indice_SG.to_excel(out_final_SG, index=False)

    #GROUPE
    Path_Indice_SG = os.path.join(base_dir,"traitement4",mois_en_cours,"Indice_SG.xlsx")
    Indice_SG = pd.read_excel(Path_Indice_SG)
    path_pon = os.path.join(base_dir,"Ponderation_SG.xlsx")
    pon = pd.read_excel(path_pon)

    df_SG= pd.merge(Indice_SG , pon, left_on='Code_produit_SG', right_on='Code_produit_SG')

    df_SG['Produit'] = df_SG['Indice_SG'] * df_SG['Pon_SG']

    df_SG['Code_produit_SG'] = df_SG['Code_produit_SG'].astype(str).str.slice(stop=-2)
    #Indice_Post = df_post.groupby('Code_produit_SG')['Produit'].sum().reset_index()
    Indice_Groupe = df_SG.groupby('Code_produit_SG').apply(lambda x: x['Produit'].sum() / x['Pon_SG'].sum()).reset_index()
    Indice_Groupe.columns = ['Code_produit_groupe', 'Indice_groupe']

    out_final_Groupe = os.path.join(base_dir,"traitement4",mois_en_cours,"Indice_groupe.xlsx")

    Indice_Groupe.to_excel( out_final_Groupe, index=False)


    #FONCTION
    Path_Indice_G = os.path.join(base_dir,"traitement4",mois_en_cours,"Indice_Groupe.xlsx")
    Indice_G = pd.read_excel(Path_Indice_G)
    path_pon = os.path.join(base_dir,"ponderation_groupe.xlsx")
    pon = pd.read_excel(path_pon)

    df_G= pd.merge(Indice_G, pon, left_on='Code_produit_groupe', right_on='Code_produit_groupe')

    df_G['Produit'] = df_G['Indice_groupe'] * df_G['Pon_groupe']

    df_G['Code_produit_groupe'] = df_G['Code_produit_groupe'].astype(str).str.slice(stop=-2)
    #Indice_Post = df_post.groupby('Code_produit_groupe')['Produit'].sum().reset_index()
    Indice_Fonction = df_G.groupby('Code_produit_groupe').apply(lambda x: x['Produit'].sum() / x['Pon_groupe'].sum()).reset_index()
    Indice_Fonction .columns = ['Code_produit_fonction', 'Indice_fonction']

    out_final_fonction = os.path.join(base_dir,"traitement4",mois_en_cours,"Indice_fonction.xlsx")

    Indice_Fonction.to_excel(out_final_fonction, index=False)

#ETAPE 9 (PRODUCTION DU RAPPORT DES INDICES) -----------------------------------------------------------------------------------------------------------------------------------------------------------------

#ETAPE 10 (PRODUCTION DU RAPPORT DES PRIX MOYENS) --------------------------------------------------------------------------------------------------------------------------------------------------------------


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,  # Désactiver les emails par défaut d'Airflow
    'email_on_retry': False,
    'email': ['bakayokoabdoulaye2809@gmail.com'],
    'start_date': datetime(2024, 5, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Workflow_IHPC',
    default_args=default_args,
    description='Ce workflow fait office de flux de traitement pour le calcul de l indice de prix',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

task1 = PythonOperator(
    task_id='CORRESPONDANCE',
    python_callable=correspondance,
    dag=dag,
)


task2 = PythonOperator(
    task_id='AJOUT_DE_VARIETE',
    python_callable=Ajout_Variete,
    dag=dag,
)

task3 = PythonOperator(
    task_id='AJOUT_DES_FOURCHETTES',
    python_callable=Ajout_Fourchette,
    dag=dag,
)

task4 = PythonOperator(
    task_id='COORECTION_DES_PRIX',
    python_callable=Correction_Prix,
    op_kwargs = {'mois_precedent': mois_precedentV2, 'mois_en_cours': mois_en_cours},
    dag=dag,
)

task5 = PythonOperator(
    task_id='IMPUTATION_DES_PRIX',
    python_callable=Imputation_Prix,
    op_kwargs = {'mois_precedent': mois_precedentV2, 'mois_en_cours': mois_en_cours},
    dag=dag,
)

task6 = PythonOperator(
    task_id='FUSION_AGREGATION_MOIS_EN_COURS',
    python_callable=Fusion_Agragation_Mois_en_cours,
    dag=dag,
)

task7 = PythonOperator(
    task_id='CALCUL_INDICE_ELEMENTAIRE',
    python_callable=Calcul_Indice_Elemenetaire,
    dag=dag,
)

task8 = PythonOperator(
    task_id='CALCUL_INDICE_LASPEYRES',
    python_callable=Calcul_Indice_Laspeyres,
    dag=dag,
)


task9 = PythonOperator(
    task_id='PRODUCTION_DU_RAPPORT_INDICES',
    python_callable=rapport_indices,
    op_kwargs = {'annee_en_cours': annee_en_cours},
    dag=dag,
)

task10 = PythonOperator(
    task_id='PRODUCTION_DU_RAPPORT_PM',
    python_callable=rapport_prix_moyen,
    op_kwargs = {'annee_en_cours': annee_en_cours},
    dag=dag,
)
task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7 >> task8 >> [task9, task10]

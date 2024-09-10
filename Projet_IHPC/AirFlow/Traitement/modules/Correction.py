import pandas as pd
import os

base_dir2 = os.path.join("/mnt","c","airflow","dags","WorkFlow_IHPC") 

def Correction_Prix(mois_precedent, mois_en_cours):

        #MISE A JOUR DU PROGRAMME DE CORRECTION DES PRIX : VERSION2
    previous_month_file = os.path.join(base_dir2,"traitement2",mois_precedent,"A_Data_Scrapping_"+mois_precedent+".xlsx")

    aggregated_df = pd.read_excel(previous_month_file)

    # Créer un dictionnaire pour un accès rapide aux fourchettes
    price_ranges = {}

    for _, row in aggregated_df.iterrows():
        lower_bound, upper_bound = map(float, row['fourchette'].split('-')) 
        price_ranges[row['Libelle_du_produit']] = (lower_bound, upper_bound)

    # Fonction pour vérifier si le prix est dans la fourchette
    def is_within_range(product, price):
        if product not in price_ranges:
            return None
        lower_bound, upper_bound = price_ranges[product]
        return lower_bound <= price <= upper_bound

    daily_files_directory= os.path.join(base_dir2,"traitement2",mois_en_cours)

    # Parcourir chaque fichier journalier
    for daily_file in os.listdir(daily_files_directory):
        if daily_file.endswith('.xlsx') : 
            daily_file_path = os.path.join(daily_files_directory, daily_file)
            daily_df = pd.read_excel(daily_file_path)
            daily_df['Status'] = None
            daily_df['Valeur_Atypique'] = None

            # Créer une liste des lignes à conserver
            rows_to_keep = []

            for _, row in daily_df.iterrows():
                product = row['Libelle_du_produit']
                if product in price_ranges:
                    price = row['Prix_du_produit']
                    if pd.isna(price):
                        row['Status'] = 'Missing'
                    elif not is_within_range(product, price):
                        row['Status'] = 'Atypique'
                        row['Valeur_Atypique'] = f"Valeur atypique:{price}"
                        row['Prix_du_produit'] = None 
                    else:
                        row['Status'] = 'Valide'
                    rows_to_keep.append(row)
                else:
                    # Produit non présent dans le fichier du mois précédent
                    continue

            cleaned_df = pd.DataFrame(rows_to_keep, columns=daily_df.columns)

            cleaned_file_path = os.path.join(base_dir2,"traitement3",mois_en_cours,daily_file)
            os.makedirs(os.path.dirname(cleaned_file_path), exist_ok=True)
            cleaned_df.to_excel(cleaned_file_path, index=False)

import logging
import pandas as pd
import os
import statistics as st

base_dir2 = os.path.join("/mnt","c","airflow","dags","WorkFlow_IHPC") 

def Imputation_Prix(mois_precedent, mois_en_cours):

    #MISE A JOUR DU PROGRAMME D'IMPUTATION DES PRIX
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    #logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levellevel)s - %(message)s')

    fichier_mois_precedent = os.path.join(base_dir2,"traitement2", mois_precedent, "A_Data_Scrapping_"+mois_precedent+".xlsx")
    df_mois_precedent = pd.read_excel(fichier_mois_precedent)

    logging.info("Fichier agrégé du mois précédent chargé.")

    # Fonction pour l'imputation des prix manquants
    def imputer_prix_manquants(df_journalier, df_mois_precedent):
        for index, row in df_journalier.iterrows():
            if pd.isna(row["Prix_du_produit"]):
                produit = row["Libelle_du_produit"]
                site = row["Code_site"]
                
                ligne_prix_precedent = df_mois_precedent[(df_mois_precedent["Libelle_du_produit"] == produit) & (df_mois_precedent["Code_site"] == site)]
                if not ligne_prix_precedent.empty:
                    prix_precedent = ligne_prix_precedent["Prix_du_produit"].values[0]
                    variete = ligne_prix_precedent["Type variété (HE, O1,O2,O3)"].values[0]
                    
                    # Filtrer les produits de la même variété dans le fichier journalier
                    produits_meme_variete = df_journalier[(df_journalier["Type variété (HE, O1,O2,O3)"] == variete) & (df_journalier["Code_site"] == site)]
                    # Exclure les produits avec des prix manquants
                    produits_meme_variete = produits_meme_variete.dropna(subset=["Prix_du_produit"])
                    
                    if not produits_meme_variete.empty:
                        # Calculer les variations individuelles de chaque produit de la même variété
                        taux_variations = []
                        for _, ligne_meme_variete in produits_meme_variete.iterrows():
                            produit_meme_variete = ligne_meme_variete["Libelle_du_produit"]
                            ligne_prix_meme_variete_precedent = df_mois_precedent[(df_mois_precedent["Libelle_du_produit"] == produit_meme_variete) & (df_mois_precedent["Code_site"] == site)]
                            if not ligne_prix_meme_variete_precedent.empty:
                                prix_meme_variete_precedent = ligne_prix_meme_variete_precedent["Prix_du_produit"].values[0]
                                prix_actuel = ligne_meme_variete["Prix_du_produit"]
                                taux_variation = (prix_actuel - prix_meme_variete_precedent) / prix_meme_variete_precedent
                                taux_variations.append(taux_variation)
                        
                        # Calculer la variation moyenne
                        if taux_variations:
                            variation_moyenne_taux = st.mean(taux_variations)
                            # Appliquer la variation moyenne en pourcentage au prix du mois précédent
                            nouveau_prix = prix_precedent * (1 + variation_moyenne_taux)
                            df_journalier.at[index, "Prix_du_produit"] = nouveau_prix
                            logging.info(f"Imputation faite pour {produit} avec le nouveau prix {nouveau_prix}.")
                    else:
                        logging.info(f"Aucun produit similaire trouvé pour la variété {variete}.")
                else:
                    logging.info(f"le produit {produit} n'est pas parmi ceux du mois précédent.")

    dossier_fichiers_journaliers = os.path.join(base_dir2,"traitement3", mois_en_cours)
    dossier_sortie = os.path.join(base_dir2,"traitement4")

    #if not os.path.exists(dossier_sortie):
        #os.makedirs(dossier_sortie)

    logging.info("Début du traitement des fichiers journaliers.")

    for nom_fichier in os.listdir(dossier_fichiers_journaliers):
        if nom_fichier.endswith(".xlsx"):
            chemin_fichier_journalier = os.path.join(dossier_fichiers_journaliers, nom_fichier)
            df_journalier = pd.read_excel(chemin_fichier_journalier)

            logging.info(f"Traitement du fichier {nom_fichier}.")

            imputer_prix_manquants(df_journalier, df_mois_precedent)
            
            chemin_fichier_corrige = os.path.join(dossier_sortie,mois_en_cours,nom_fichier)
            os.makedirs(os.path.dirname(chemin_fichier_corrige), exist_ok=True)
            df_journalier.to_excel(chemin_fichier_corrige, index=False)

   

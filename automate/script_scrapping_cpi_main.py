#IMPORTATION DES MODULES :--------------------------------------------------------------------------------------------------------------

from selenium import webdriver

import pandas as pd

from datetime import datetime

from selenium.common.exceptions import StaleElementReferenceException

from selenium.common.exceptions import NoSuchElementException

from selenium.webdriver.common.by import By

# Spécifiez le chemin du pilote ChromeDriver
chrome_driver_path = 'C:\\Program Files (x86)\\chromedriver.exe'  # Assurez-vous d'échapper correctement les caractères '\'

# Initialisez le service du navigateur Chrome avec le chemin du pilote
chrome_service = webdriver.chrome.service.Service(chrome_driver_path)

# Initialisez les options du navigateur Chrome
chrome_options = webdriver.ChromeOptions()

from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains

from bs4 import BeautifulSoup
import requests

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re
import numpy as np

#Importation du module d'envoi de mail
from send_mail import send_mail_success, send_mail_error

#Importation du module de Scrapping
from Script_Scrapping_cpi import scrapping_AIK

try :

    #--------------------------------------------------------- CODE AIK ---------------------------------------------------------------------

    df1 = scrapping_AIK()
    df1.drop(columns=['N_ordre', 'Code_ID_PE','Prix du produit'], inplace=True)
    # Renommer les colonnes
    df1 = df1.rename(columns={'code_site': 'Code_site', 'date de collecte': 'Date_de_collecte', 'Libellé du produit': 'Libelle_du_produit','Caractéristiques du produit':'Caracteristique','Prix Réel':'Prix_du_produit','Quantité':'Quantite','unite de mesure':'Unite'})
    # Réaffectation des colonnes dans le nouvel ordre
    df1 = df1[['Date_de_collecte', 'Code_site', 'Libelle_du_produit','Quantite','Prix_du_produit','Caracteristique','Unite','Unite_monetaire']]
    #------------------------------------------------------- CODE ADJOVAN ------------------------------------------------------------------

    #LES DIFFERENTES FONCTIONS D'USAGE DU SCRAPPING :---------------------------------------------------------------------------------------

    def get_description_text(product_url):
        # Envoyer une requête GET pour récupérer le contenu HTML de la page
        
        driver.get(product_url)
        #response = requests.get(product_url)

        # Vérifier si la requête a réussi (statut 200)
        #if response.status_code == 200:
            
        # Utiliser BeautifulSoup pour analyser le contenu HTML
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        #soup = BeautifulSoup(response.content, 'html.parser')

        # Trouver la balise 'h2' avec le texte "Description"
        description_heading = soup.find('h2', text='Description')

        # Vérifier si la balise 'h2' a été trouvée
        if description_heading:
            # Trouver la balise 'p' suivante après la balise 'h2'
            description_paragraph = description_heading.find_next('p')

            # Récupérer le texte de la balise 'p'
            description_text = description_paragraph.get_text(strip=True) if description_paragraph else None

            # Retourner le texte de la description
            return description_text
        else:
            return "Pas de Caracteristique"
    #else:
        #return f"Échec de la requête. Statut : {response.status_code}"

    # Définition de la fonction pour extraire les chiffres
    def extraire_chiffres(value):
        chiffres = re.findall(r'\d+', value)
        if chiffres:
            return int(chiffres[0])
        else:
            return np.nan
        
    #Fonction pour supprimer les zéros après les entiers
    def supprimer_zeros(value):
        if isinstance(value, float) and not pd.isnull(value):
            return re.sub(r'\.0$', '', str(value))
        return value

    #RECUPERATION DES DONNEES SUR LE SITE :------------------------------------------------------------------------------------------------

    Category_Links = [
        
        'https://www.adjovan.com/product-category/fruits-legumes/',
        'https://www.adjovan.com/product-category/viande-volaille/',
        'https://www.adjovan.com/product-category/articles-pour-bebe/',
        'ttps://www.adjovan.com/product-category/boulangerie-patisserie/',
        'https://www.adjovan.com/product-category/charcuterie/',
        'https://www.adjovan.com/product-category/croustilles-collations/',
        'https://www.adjovan.com/product-category/epices-sauces-huiles/',
        'https://www.adjovan.com/product-category/sante-et-bien-etre/',
        'https://www.adjovan.com/product-category/poissons/',
        'https://www.adjovan.com/product-category/produits-laitiers-et-oeuf/',
        'https://www.adjovan.com/product-category/produits-menagers/',
        'https://www.adjovan.com/product-category/hygiene-beaute/'    
    ]


    data = []


    for category_url in Category_Links :

        # Utilisation de Selenium avec le navigateur Chrome
        driver = webdriver.Chrome()
        driver.get(category_url)

        # Attendre quelques secondes pour permettre le chargement complet de la page
        driver.implicitly_wait(5)

        while True:
            
            
            # Récupérer le contenu de la page après le chargement dynamique
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            #Récuperer l'URL Courant
            current_url = driver.current_url
            # Extraire les informations des produits sur la page actuelle
            products = soup.find_all('li', class_='product')

            for product in products:
                # Extraire le titre du produit
                title = product.find('h3', class_='product-name').text.strip()

                # Extraire le prix du produit
                #price = product.find('span', class_='price').text.strip()
                
                # Extraire le prix du produit s'il est disponible
                price_element = product.find('span', class_='price')
                price = price_element.text.strip() if price_element else "Prix non disponible"
                
                quantity = title.split('[')[-1].split(']')[0]
                
                # Obtenir la date de collecte actuelle
                date_de_collecte = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                product_link = product.find('div', class_='image-block').find('a')['href']
            
                caracteristique = get_description_text(product_link)

                # Ajouter les informations à la liste de données
                data.append({
                
                "Date_de_collecte": date_de_collecte,  # Vous devrez attribuer une date
                "Code_site":"adjovan",
                "Libelle_du_produit": title,
                "Quantite": quantity,
                "Prix_du_produit": price,
                "Caracteristique" : caracteristique
                    
                })
        
            try:
                
                driver.get(current_url)
                driver.implicitly_wait(5)
                
                # Trouver le lien vers la page suivante (pagination)
                next_page_link = driver.find_element(By.CSS_SELECTOR, 'a.next.page-numbers')
                #next_page_link = driver.find_element_by_css_selector('a.next.page-numbers')

                # Vérifier s'il y a une page suivante
                if 'disabled' in next_page_link.get_attribute('class'):
            
                    break  # Sortir de la boucle s'il n'y a pas de page suivante

                # Cliquer sur le lien vers la page suivante
                #next_page_link.click()
                
                # Utiliser execute_script pour cliquer sur le bouton "Next"
                driver.execute_script("arguments[0].click();", next_page_link)

                # Attendre quelques secondes pour permettre le chargement complet de la page suivante
                driver.implicitly_wait(5)
            
            except NoSuchElementException:
            
                print("Dernière page atteinte.")
                break
                
    # Créer un DataFrame à partir des données
    df = pd.DataFrame(data)
    df['Unite'] = df['Quantite'].str.extract('([A-Za-z]+)')

    # Extraction des chiffres de la colonne 'Quantite'
    df['Quantite'] = df['Quantite'].apply(extraire_chiffres)

    # Application de la fonction à la colonne 'Quantite'
    df['Quantite'] = df['Quantite'].apply(supprimer_zeros)

    df['Unite_monetaire'] = "CFA"
    df['Prix_du_produit'] = df['Prix_du_produit'].str.replace('CFA', '').str.strip()

    #Exportation de la donnée Finale
    df_final = pd.concat([df, df1], axis=0, ignore_index=True)
    df_final.to_excel("Data_Scrapping_"+datetime.now().strftime('%Y-%m-%d %H:%M:%S')+".xlsx", index=False)


    #ENVOIE DU MAIL:

    send_mail_success("Donnee_Scrapping_Adjovan", "bakayokoabdoulaye2809@gmail.com", "abdoulayebakayoko265@gmail.com", "Donnee_Scrapping_Adjovan.xlsx")

except Exception as e :

    send_mail_error("Donnee_Scrapping_Adjovan", "bakayokoabdoulaye2809@gmail.com", "abdoulayebakayoko265@gmail.com", "Donnee_Scrapping_Adjovan.xlsx")




from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import requests
from urllib.parse import urljoin
#import pandas as pd
import time
import re
import requests
from selenium.common.exceptions import WebDriverException

def scrapping_AIK():

    # Liste des URLs à traiter
    urls = ["https://www.ivoirshop.ci/",
            "https://www.ivoirshop.ci/categorie-produit/supermache",
            "https://www.ivoirshop.ci/categorie-produit/maison-bureau",
            "https://www.ivoirshop.ci/categorie-produit/telephonie",
            "https://www.ivoirshop.ci/categorie-produit/beaute-hygiene",
            "https://www.ivoirshop.ci/categorie-produit/electronique",
            "https://www.ivoirshop.ci/categorie-produit/produits-adultes",
            "https://www.ivoirshop.ci/categorie-produit/mode/mode-femme",
            "https://www.ivoirshop.ci/categorie-produit/mode/mode-homme",
            "https://www.ivoirshop.ci/categorie-produit/produits-pour-bebes",
            "https://www.ivoirshop.ci/categorie-produit/informatique",
            "https://www.ivoirshop.ci/categorie-produit/sport-bien-etre",
            "https://www.ivoirshop.ci/categorie-produit/jouets-et-jeux-videos"
        ]

    # Liste pour stocker les DataFrames de chaque site
    all_dfs = []
    products = []
    # Boucle à travers chaque URL
    for url in urls:
        # Étape 1 : Envoyer une requête HTTP à la page web et récupérer le contenu HTML
        response = requests.get(url)
        html_content = response.text

        # Étape 2 : Utiliser BeautifulSoup pour parser le contenu HTML
        soup = BeautifulSoup(html_content, "html.parser")

        # Étape 3 : Extraire les informations des produits
        
        for product_tag in soup.find_all("li", class_="product"):
            product_info = {}
            
            product_info['N_ordre']=""
            product_info["Code_ID_PE"]=""
            
            product_info['code_site']= url
            product_info['date de collecte']= datetime.now().strftime('%Y-%m-%d')

            # Extraire le lien du produit
            product_info["lien_produit"] = product_tag.find("a")["href"]

            # Extraire le titre du produit
            product_info["Libellé du produit"] = product_tag.find("h2", class_="woo-loop-product__title").text.strip()

            # Extraire le prix du produit
            price_tag = product_tag.find("span", class_="price")
            product_info["Prix du produit"] = price_tag.find("ins").text.strip() if price_tag and price_tag.find("ins") else None

            # Ajouter les informations du produit à la liste
            products.append(product_info)

        # Étape 4 : Créer un DataFrame à partir de la liste de produits
        df_ivoirshop = pd.DataFrame(products)

        # Ajouter le DataFrame à la liste globale
        all_dfs.append(df_ivoirshop)

#--------------------------------------------------------------------------------------------------------------------
    #import requests
    #from bs4 import BeautifulSoup
    #import pandas as pd

    def scrape_product_info(url):
        try:
            response = requests.get(url, timeout=500)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Erreur de connexion à {url} : {e}")
            return pd.DataFrame()

        soup = BeautifulSoup(response.text, 'html.parser')

        price_tag = soup.find('p', class_='price')
        promo_price_tag = price_tag.find('ins')
        promo_price = promo_price_tag.text.strip() if promo_price_tag else None

        regular_price_tag = price_tag.find('del')
        regular_price = regular_price_tag.text.strip() if regular_price_tag else None

        stock_tag = soup.find('p', class_='stock in-stock')
        stock = stock_tag.text.strip() if stock_tag else None

        description_tag = soup.find('h1', class_='wt-text-body-03')
        description = description_tag.text.strip() if description_tag else None
        
        url_lien = url

        # Create a DataFrame with the extracted information
        df_product = pd.DataFrame({
            
            'Prix Réel': [promo_price],
            'code_siteDescription': url_lien,
            'Quantité': [stock],
            'Caractéristiques du produit': [description]
        })

        return df_product

    # List of URLs for individual product pages
    urls = list(df_ivoirshop["lien_produit"])
    # Initialize an empty DataFrame to store the results
    combined_df_ivoirshop = pd.DataFrame()

    # Scrape product information for each URL and concatenate the results
    for url in urls:
        df_product = scrape_product_info(url)
        combined_df_ivoirshop = pd.concat([combined_df_ivoirshop, df_product], ignore_index=True)

#--------------------------------------------------------------------------------------------------------------------------------------
    df_ivoirshop['Prix Réel'] = combined_df_ivoirshop['Prix Réel']
    df_ivoirshop['code_siteDescription'] = combined_df_ivoirshop['code_siteDescription'] 
    df_ivoirshop['Quantité'] = combined_df_ivoirshop['Quantité']
    df_ivoirshop['Caractéristiques du produit'] = combined_df_ivoirshop['Caractéristiques du produit']
#-------------------------------------------------------------------------------------------------------------------------------------
    # Réorganiser les colonnes selon vos besoins
    df_ivoirshop = df_ivoirshop[[
    'N_ordre', 'code_site', 'Code_ID_PE', 'date de collecte', 'Libellé du produit', 'Caractéristiques du produit',
    'Prix Réel', 'Quantité', "Prix du produit"]]
#-------------------------------------------------------------------------------------------------------------------------------------
    df_ivoirshop[["Prix du produit", 'Unite_monetaire']] = df_ivoirshop["Prix du produit"].str.extract(r"([0-9.]+)\s*([a-zA-Z]+)")

# CODE KEVAJO ------------------------------------------------------------------------------------------------------------------------
    #import requests
    #from bs4 import BeautifulSoup
    #import pandas as pd

    def scrape_product_info(product_div):
        title_tag = product_div.find('h3', class_='wd-entities-title')
        title = title_tag.text.strip() if title_tag else None

        price_tag = product_div.find('span', class_='price')
        price = price_tag.text.strip() if price_tag else None

        promo_tag = product_div.find('span', class_='woocommerce-Price-amount amount')
        promo = promo_tag.text.strip() if promo_tag else None

        real_price_tag = product_div.find('ins', class_='woocommerce-Price-amount amount')
        real_price = real_price_tag.text.strip() if real_price_tag else None

        image_tag = product_div.find('img', class_='attachment-600x498')
        image_url = image_tag['nitro-lazy-src'] if image_tag and 'nitro-lazy-src' in image_tag.attrs else None

        label_tag = product_div.find('span', class_='awl-inner-text')
        label = label_tag.text.strip() if label_tag else None
        
        
        brand_tag = product_div.find("div", class_="col-12 mt-1 my-md-3 text-center text-md-start jt-max-line-size-3")
        brand = brand_tag.text.strip() if brand_tag else None
        
        
        
        quantity_tag = product_div.find('input', class_='js-item-qty')
        quantity = quantity_tag['value'] if quantity_tag else None

        product_url = product_div.find('a', class_='product-image-link')['href']
        date_new=datetime.now().strftime('%Y-%m-%d')

        return {
            'date de collecte': date_new,
            'Libellé du produit': title,
            'Prix du produit': price,
            'Promo': promo,
            'Image URL': image_url,
            'URL': product_url
        }

    products_data = []
    def scrape_page(url):
        try:
            response = requests.get(url, timeout=500)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Erreur de connexion à {url} : {e}")
            return pd.DataFrame()

        soup = BeautifulSoup(response.text, 'html.parser')

        product_divs = soup.find_all('div', class_='wd-product')
        
        
        for product_div in product_divs:
            product_info = scrape_product_info(product_div)
            products_data.append(product_info)

        return pd.DataFrame(products_data)

    # URL de la page
    urls = [
        "https://kevajo.com/", "https://kevajo.com/product-category/pour-bebe/",
        "https://kevajo.com/product-category/mode-2/modefemme/",
    "https://kevajo.com/product-category/mode-2/mode-homme/",
    "https://kevajo.com/product-category/maison-et-cuisine/",
    "https://kevajo.com/product-category/fournitures-de-bureau-et-scolaires/",
    "https://kevajo.com/product-category/telephones-et-tablettes/",
    "https://kevajo.com/product-category/jeux-video-consoles-et-accessoires/",
    "https://kevajo.com/product-category/electronique/",
    "https://kevajo.com/product-category/lunettes-de-vue/",
    "https://kevajo.com/product-category/beaute-et-hygiene/",
    "https://kevajo.com/product-category/informatique/",
    "https://kevajo.com/product-category/auto-et-moto/",
    "https://kevajo.com/product-category/mode-2/bagages-et-sacs-de-voyage/",
    "https://kevajo.com/#"]

    # Scrape product information for each URL and concatenate the results
    for url in urls:
        df_product = scrape_page(url)
        df_kevajo = pd.concat([ df_product], ignore_index=True)

#---------------------------------------------------------------------------------------------------------------------------------------
    #import requests
    #from bs4 import BeautifulSoup
    #import pandas as pd
    #from datetime import datetime

    def extract_text(element, tag_name=None):
        if element and tag_name:
            tag = element.find(tag_name)
            return tag.text.strip() if tag else ""
        return ""

    def clean_text(text):
        return text.replace('\r\n', '').replace('\xa0', '')

    def scrape_kevajo_page(page_urls):
        product_data_list = []
        
        for page_url in page_urls:
            url_lien=page_url
            try:
                response = requests.get(page_url, timeout=500)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"Erreur de connexion à {page_url} : {e}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')

            # Extraction des éléments de la page
            breadcrumbs = [a.text.strip() for a in soup.select('.woocommerce-breadcrumb a')]
            product_title = extract_text(soup.find('h1', class_='product_title'))
            

            # Extraction de la description
            description_tag = soup.find('div', class_='jt-description-content-wrapper')
            description = description_tag.find('p').text.strip() if description_tag else None
            

            attributes = {}
            attribute_rows = soup.select('.woocommerce-product-attributes tr')
            for row in attribute_rows:
                label = row.find('th').text.strip()
                value = row.find('td').text.strip()
                attributes[label] = value

            data = {
                'N_ordre': 'index',# Modify as needed
                'code_site': url_lien,  # Modify as needed
                'Code_ID_PE': 'YourCodeIDPE',  # Modify as needed
                'date de collecte': datetime.now().strftime('%Y-%m-%d'),
                'Breadcrumbs': breadcrumbs,
                'ProductTitle': product_title,
                'Attributes': attributes,
            }

            product_data_list.append(data)

        return pd.DataFrame(product_data_list)

    # Liste des URLs de pages produits
    page_urls = list(df_kevajo['URL'])

    # Scrape des détails de chaque page
    df_product_details = scrape_kevajo_page(page_urls)

    # Assuming 'Attributes' column contains dictionaries
    df_product_details['Quantité'] = df_product_details['Attributes'].apply(lambda x: x.get('Poids', ''))
    df_product_details['Caractéristiques du produit'] = df_product_details['Attributes'].apply(lambda x: x.get('Marque', ''))

    # Extract 'Poids', 'Unite', and 'Marque' columns from the 'Poids' column
    df_product_details[['Quantité', 'Unite']] = df_product_details['Quantité'].str.extract(r"([0-9.]+)\s*([a-zA-Z]+)")

    # Drop the original 'Poids' column
    #df.drop('Poids', axis=1, inplace=True)

    #----------------------------------------------------------------------------------------------------------------------------------
    df_kevajo['N_ordre'] = list(df_product_details["N_ordre"])
    df_kevajo['code_site'] = list(df_product_details["code_site"])
    df_kevajo['Code_ID_PE'] = list(df_product_details["Code_ID_PE"])
    df_kevajo['date de collecte'] = list(df_product_details["date de collecte"])
    df_kevajo['Caractéristiques du produit'] = list(df_product_details['Caractéristiques du produit'])
    df_kevajo['Quantité'] = list(df_product_details["Quantité"])
    df_kevajo['unite de mesure'] = list(df_product_details["Unite"])
    df_kevajo['Intitule'] = list(df_product_details["Breadcrumbs"])
    df_kevajo['Entreprise'] = list(df_product_details["ProductTitle"])

    df_kevajo['Attributes'] = list(df_product_details["Attributes"])
    
   #-----------------------------------------------------------------------------------------------------------------------------------
   # Réorganiser les colonnes selon vos besoins
    df_kevajo = df_kevajo[[
    'N_ordre', 'code_site', 'Code_ID_PE', 'date de collecte', 'Libellé du produit', 'Caractéristiques du produit',
    'unite de mesure', 'Quantité', "Prix du produit"]]
   #-----------------------------------------------------------------------------------------------------------------------------------
    df_kevajo[["Prix du produit", 'Unite_monetaire']] = df_kevajo["Prix du produit"].str.extract(r"([0-9.]+)\s*([a-zA-Z]+)")
   
   #CODE AUCHAN ----------------------------------------------------------------------------------------------------------------------
    #from selenium import webdriver
    #from selenium.webdriver.common.by import By
    #from selenium.webdriver.support.ui import WebDriverWait
    #from selenium.webdriver.support import expected_conditions as EC
    #from bs4 import BeautifulSoup
    #import pandas as pd
    #from datetime import datetime

    # Liste des URLs
    urls = [ "https://www.auchan.ci/les-indispensables/1J56N7JC/cp",
        "https://www.auchan.ci/mes-courses/gel-douche-et-bain/UJ3B2N7N",
        "https://www.auchan.ci/mes-courses/parapharmacie/18PEAELE", 
        
        
        ] 

    # Configuration du navigateur Selenium
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Pour exécuter le navigateur en arrière-plan
    driver = webdriver.Chrome(options=options)

    # Initialiser une liste pour stocker les données
    data_list = []

    # Parcourir la liste des URLs
    for url in urls:
        driver.get(url)
        # Attendre que la page se charge complètement
        wait = WebDriverWait(driver, 5000)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.js-jt-product-card')))

        # Parser le contenu HTML avec BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Sélectionner tous les produits sur la page
        products = soup.select('.js-jt-product-card')

        # Boucle sur chaque produit
        for product in products:
            # Extraire les informations nécessaires
            product_pid = product['cy-product-pid']
            product_title = product.select_one('.js-title-line').text.strip()
            product_brand = product.select_one('.js-brand-line').text.strip()
            product_image = product.select_one('.js-image-line')['data-src']
            product_price = product.select_one('.js-price-line').text.strip()
            product_old_price = product.select_one('.js-wasPrice-line')

            url_lien = url

            # Vérifier si le prix d'origine existe
            if product_old_price:
                product_old_price = product_old_price.text.strip()
            else:
                product_old_price = None

            # Ajouter les données à la liste
            data_list.append({
                'N_ordre': product_pid,
                'Libellé': product_title,
                'code_site': url_lien,
                'Code_ID_PE': product_brand,
                'Product Image': product_image,
                'Prix du produit': product_price,
                'Product Old Price': product_old_price,
                'date de collecte': datetime.now().strftime('%Y-%m-%d'),
            })

            # Cliquer sur l'élément pour obtenir plus de détails
            product_link = product.select_one('.js-product-anchor')
            if product_link and product_link.has_attr('href'):
                product_details_url = product_link['href']

                # Ouvrir un nouvel onglet pour charger la page de détails
                driver.execute_script(f"window.open('{product_details_url}', '_blank');")

                # Passer au nouvel onglet
                driver.switch_to.window(driver.window_handles[1])

                # Attendre que la page de détails se charge complètement
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.jt-breadcrumb-title-ellipsis span:last-child')))

                # Parser le contenu HTML de la page de détails
                detail_soup = BeautifulSoup(driver.page_source, 'html.parser')

                # Extraire des informations supplémentaires de la page de détails
                product_description = detail_soup.select_one('.jt-description-content-wrapper p').text.strip()

                # Ajouter les informations supplémentaires à la liste
                data_list[-1]['Caractéristiques du produit'] = product_description

                # Fermer le nouvel onglet
                driver.close()

                # Revenir à l'onglet principal
                driver.switch_to.window(driver.window_handles[0])

    # Fermer le navigateur Selenium
    driver.quit()

    # Créer un DataFrame à partir de la liste
    #df_auchan = pd.DataFrame(data_list)

    # Imprimer le DataFrame
    #df_auchan

    #---------------------------------------------------------------------------------------------------------------------------------
    #import re
    #import pandas as pd

    # Fonction pour extraire le nom du produit, la quantité et l'unité de mesure
    def extract_info(product_title):
        # Utiliser une expression régulière pour extraire les informations
        match = re.match(r'(?P<Libellé_du_produit>.*?)(?P<Quantité>\d+)(?P<Unité_de_mesure>[a-zA-Z]+)', product_title)
        
        # Vérifier si la correspondance a été trouvée
        if match:
            return match.group('Libellé_du_produit').strip(), match.group('Quantité'), match.group('Unité_de_mesure')
        else:
            return None, None, None

    # Appliquer la fonction sur la colonne "Product Title"
    df_auchan[['Libellé du produit', 'Quantité', 'unite de mesure']] = df_auchan["Libellé"].apply(extract_info).apply(pd.Series)

    #---------------------------------------------------------------------------------------------------------------------------------
    # Réorganiser les colonnes selon vos besoins
    df_auchan = df_auchan[[
        'N_ordre', 'code_site', 'Code_ID_PE', 'date de collecte', 'Libellé du produit', 'Caractéristiques du produit',
        'unite de mesure', 'Quantité', "Prix du produit"]]
    #---------------------------------------------------------------------------------------------------------------------------------
    df_auchan[["Prix du produit", 'Unite_monetaire']] = df_auchan["Prix du produit"].str.extract(r"([0-9.]+)\s*([a-zA-Z]+)")
    #--------------------------------------------------------------------------------------------------------------------------------
    

    # Supposons que df_auchan, df_ivoirshop et df_kevajo sont vos trois DataFrames

    # Concaténation verticale (ajout des lignes)
    Web_scraping_Auchan_Kevajo_Ivoirshop = pd.concat([df_ivoirshop,df_kevajo, df_auchan], ignore_index=True)

    # Si vous ne souhaitez pas réinitialiser les index, vous pouvez laisser ignore_index=False

    # Afficher le résultat
    return Web_scraping_Auchan_Kevajo_Ivoirshop












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

from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException, WebDriverException


def scrapping_AIK2(): 
    

    # Liste des URLs à traiter


   #CODE AUCHAN ----------------------------------------------------------------------------------------------------------------------
    #from selenium import webdriver
    #from selenium.webdriver.common.by import By
    #from selenium.webdriver.support.ui import WebDriverWait
    #from selenium.webdriver.support import expected_conditions as EC
    #from bs4 import BeautifulSoup
    #import pandas as pd
    #from datetime import datetime

    # Liste des URLs
    urls = [ "https://www.auchan.ci/mes-courses/cuisine-et-arts-de-la-table/1X78GLOL",
"https://www.auchan.ci/selection-hygiene-beaute/CQQZ2P9E/cp",
      "https://www.auchan.ci/mes-courses/tampons/C3Z06PLP",
     "https://www.auchan.ci/mes-courses/fromages/CV5SB7BU",
      "https://www.auchan.ci/mes-courses/produits-laitiers-oeufs-fromages/UNBA9591",
      "https://www.auchan.ci/mes-courses/bieres-et-cidres/UR7QGB7B",
      "https://www.auchan.ci/mes-courses/viandes-charcuterie-et-poissons/COEBIN7N",
      "https://www.auchan.ci/mes-courses/produits-laitiers-oeufs-fromages/UNBA9591",
      "https://www.auchan.ci/mes-courses/fruits-et-legumes/CH0J2N7N",
      "https://www.auchan.ci/mes-courses/conserves-et-plats-cuisines/UBO4Q0Q1",
      "https://www.auchan.ci/mes-courses/rechercher?q=riz&queryId=a54ef1df64907375ef73a3313b5e2cce",
    "https://www.auchan.ci/mes-courses/hygiene-soins-homme/UED6QWQ1",
    "https://www.auchan.ci/mes-courses/thes-et-infusions/1JTX6N7N",
     "https://www.auchan.ci/mes-courses/dentifrices-et-bains-de-bouche/19D0SPBP",  
     "https://www.auchan.ci/mes-courses/cafes-moulus-grains-et-chicorees/1HRRAN7N",
    "https://www.auchan.ci/mes-courses/accessoires-de-cuisine-salle-de-bains-et-wc/U0DBAPBP",
    "https://www.auchan.ci/mes-courses/vins/CP7F2B7B",
    "https://www.auchan.ci/mes-courses/eaux/19Z32PBP",
    "https://www.auchan.ci/mes-courses/produits-laitiers-oeufs-fromages/UNBA9591",
    "https://www.auchan.ci/mes-courses/chiens/UZZ9GLOL",
    "https://www.auchan.ci/mes-courses/javel-et-produits-multi-usages/19XT4PBP",
    "https://www.auchan.ci/mes-courses/corps/CZ02LOLU",
    "https://www.auchan.ci/mes-courses/brioches-et-pains-de-mie/1LQB4N7N",
    "https://www.auchan.ci/mes-courses/soins-du-visage-et-maquillage/CX99MLOL",
    "https://www.auchan.ci/mes-courses/chocolat-en-poudre/1RB9SB7B",
    "https://www.auchan.ci/mes-courses/boissons-gazeuses-et-sirops/175K2PLP",
    "https://www.auchan.ci/mes-courses/bieres-et-cidres/UR7QGB7B",
    "https://www.auchan.ci/mes-courses/bonbons/1RNO6B7B",
    "https://www.auchan.ci/mes-courses/legumes-et-fruits/CRPYAB7B",
    "https://www.auchan.ci/les-nouveautes/CYXMB7NU/cp",
    "https://www.auchan.ci/mes-courses/boissons/UFRY2VYV",
    "https://www.auchan.ci/mes-courses/savons/UENW2QWQ",
    "https://www.auchan.ci/mes-courses/cereales/UPNJ4B7B",
    "https://www.auchan.ci/mes-courses/couches-et-couches-culottes/1ZOYSLOL",
    "https://www.auchan.ci/mes-courses/cafes/U0FDIPBP",
    "https://www.auchan.ci/mes-courses/soins-des-cheveux/UZZNGLOL",
    "https://www.auchan.ci/mes-courses/laits-et-boissons-lactees/C7NZMPLP",
    "https://www.auchan.ci/mes-courses/serviettes-hygieniques/1EOXAQWQ",
    "https://www.auchan.ci/special-de-2000fcfa/C5ESPLRU/cp",
    "https://www.auchan.ci/mes-courses/legumes-et-fruits/CRPYAB7B",
    "https://www.auchan.ci/mes-courses/cafes-moulus-grains-et-chicorees/1HRRAN7N",
    "https://www.auchan.ci/mes-courses/accessoires-de-cuisine-salle-de-bains-et-wc/U0DBAPBP",
    "https://www.auchan.ci/mes-courses/vins/CP7F2B7B",
    "https://www.auchan.ci/mes-courses/eaux/19Z32PBP",
    "https://www.auchan.ci/mes-courses/produits-laitiers-oeufs-fromages/UNBA9591",
    "https://www.auchan.ci/mes-courses/chiens/UZZ9GLOL",
    "https://www.auchan.ci/mes-courses/javel-et-produits-multi-usages/19XT4PBP",
    "https://www.auchan.ci/mes-courses/corps/CZ02LOLU",
    "https://www.auchan.ci/mes-courses/dosettes-et-capsules/C3DR2PLP",
    "https://www.auchan.ci/mes-courses/wc-et-canalisations/CHHW6N7N",
    "https://www.auchan.ci/mes-courses/cereales-et-petit-dejeuner-bebe/1Y9Z6B7B",
    "https://www.auchan.ci/mes-courses/colas-et-boissons-gazeuses/1NOJI959",
    "https://www.auchan.ci/mes-courses/volaille-gibier-lapin-et-autres/UZ9ZSLOL",
    "https://www.auchan.ci/mes-courses/plats-cuisines-en-conserve/1YLVIB7B",
    "https://www.auchan.ci/mes-courses/shampoings/URVNSB7B",
    "https://www.auchan.ci/mes-courses/gel-douche-et-bain/UEKIQWQ1",
    "https://www.auchan.ci/mes-courses/oeufs-beurres-et-creme/1LOLAN7N",
    "https://www.auchan.ci/mes-courses/produits-menagers-et-accessoires-de-la-maison/C3QIPLPU",
    "https://www.auchan.ci/mes-courses/cremes-fraiches/CBOBGQ0Q",
    "https://www.auchan.ci/mes-courses/ustensiles-de-cuisine/1Z7JILOL",
    "https://www.auchan.ci/mes-courses/conserves-et-plats-cuisines/UBO4Q0Q1",
    "https://www.auchan.ci/mes-courses/cremes-et-soins/1H0R6N7N",
    "https://www.auchan.ci/mes-courses/vaisselle-jetable/18LR6ELE",
    "https://www.auchan.ci/mes-courses/lessives-repassage-et-soin-du-linge/1KOW2N5N",
    "https://www.auchan.ci/mes-courses-avec-livraison/1V3AB7FC/cp",
    "https://www.auchan.ci/mes-courses-du-mois/CVYIB7FU/cp",
    "https://www.auchan.ci/mes-courses/ketchup-mayonnaise-moutarde-et-sauces/UO8ZSN7N",
    "https://www.auchan.ci/mes-courses/panaches/CRXB4B7B",
    "https://www.auchan.ci/mes-courses/fruits-et-legumes/CH0J2N7N",
    "https://www.auchan.ci/tout-a-moins-de-1000f/C5ESPLRU/cp",
    "https://www.auchan.ci/mes-courses/epicerie-salee/CENGQWQU",
    "https://www.auchan.ci/mes-courses/mouchoirs/CZVNGLOL",
    "https://www.auchan.ci/mes-courses/laits-en-poudre/C0PE2PBP",
    "https://www.auchan.ci/promos-can/U5DH4PLR/cp",
    "https://www.auchan.ci/mes-courses/surgeles/UYB4B7B1",
    "https://www.auchan.ci/mes-courses/beurres-margarines-et-autres/UJBB6N7N",
    "https://www.auchan.ci/mes-courses/eaux-minerales/C7ORAPLP",
    "https://www.auchan.ci/mes-courses/bebe/U8P06ELE",
    "https://www.auchan.ci/mes-courses/huiles-epices-condiments/CQQDSP9P",
    "https://www.auchan.ci/mes-courses/pates/10EAPBPC",
    "https://www.auchan.ci/mes-courses/riz/1QH6P9PC",
    "https://www.auchan.ci/mes-courses/vinaigres-vinaigrettes-et-sauces-crudites/UZXQSLOL",
    "https://www.auchan.ci/mes-courses/brioches-et-pains-de-mie/1LQB4N7N",
    "https://www.auchan.ci/mes-courses/soins-du-visage-et-maquillage/CX99MLOL",
    "https://www.auchan.ci/mes-courses/chocolat-en-poudre/1RB9SB7B",
    "https://www.auchan.ci/mes-courses/boissons-gazeuses-et-sirops/175K2PLP",
    "https://www.auchan.ci/mes-courses/bieres-et-cidres/UR7QGB7B",
    "https://www.auchan.ci/mes-courses/bonbons/1RNO6B7B",
    "https://www.auchan.ci/mes-courses/jambons-et-saucissons-de-porc/UVZP6B7B",
    "https://www.auchan.ci/mes-courses/farines/1J0Y2N7N",
    "https://www.auchan.ci/mes-courses/papiers-toilette/UVWF2B7B",
    "https://www.auchan.ci/mes-courses/aperitif/1EN9IQWQ",
    "https://www.auchan.ci/mes-courses/cuisine/CTPXSB5B",
    "https://www.auchan.ci/mes-courses/charcuterie/13NXAPLP"


      
        ]

    # Configuration du navigateur Selenium
    

    #options = webdriver.ChromeOptions()
    #options.add_argument("--headless")  # Pour exécuter le navigateur en arrière-plan
    #options.add_argument("--disable-gpu")  # Désactiver l'accélération GPU en mode headless
    #chrome_driver_path = '/usr/bin/chromedriver'
    #options.binary_location = "/usr/bin/google-chrome"  # Remplacez par l'emplacement réel de votre Chrome binary
    #options.add_argument(f"webdriver.chrome.driver={chrome_driver_path}")
    #driver = webdriver.Chrome(options=options)

    chrome_driver_path = '/usr/bin/chromedriver'
    chrome_service = webdriver.chrome.service.Service(chrome_driver_path)
    chrome_options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(service=chrome_service, options=chrome_options)
    

    # Initialiser une liste pour stocker les données
    data_list = []

    # Parcourir la liste des URLs
    for url in urls:
        driver.get(url)
        try:
            wait = WebDriverWait(driver, 10)  # Reduce wait time for better performance
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.js-jt-product-card')))

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            products = soup.select('.js-jt-product-card')

            for product in products:
                product_pid = product.get('cy-product-pid')
                product_title = product.select_one('.js-title-line').text.strip()
                product_brand = product.select_one('.js-brand-line').text.strip()
                product_image = product.select_one('.js-image-line').get('data-src')
                product_price = product.select_one('.js-price-line').text.strip()
                product_old_price = product.select_one('.js-wasPrice-line')
                url_lien = url

                if product_old_price:
                    product_old_price = product_old_price.text.strip()
                else:
                    product_old_price = None

                data = {
                    'N_ordre': product_pid,
                    'Libellé': product_title,
                    'code_site': url_lien,
                    'Code_ID_PE': product_brand,
                    'Product Image': product_image,
                    'Prix du produit': product_price,
                    'Product Old Price': product_old_price,
                    'date de collecte': datetime.now().strftime('%Y-%m-%d'),
                }

                product_link = product.select_one('.js-product-anchor')
                if product_link and product_link.has_attr('href'):
                    product_details_url = product_link['href']
                    driver.execute_script(f"window.open('{product_details_url}', '_blank');")
                    driver.switch_to.window(driver.window_handles[1])

                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.jt-breadcrumb-title-ellipsis span:last-child')))
                    detail_soup = BeautifulSoup(driver.page_source, 'html.parser')
                    description_element = detail_soup.select_one('.jt-description-content-wrapper p')
                    if description_element:
                        product_description = description_element.text.strip()
                    else:
                        product_description = None

                    data['Caractéristiques du produit'] = product_description

                    driver.close()
                    driver.switch_to.window(driver.window_handles[0])

                data_list.append(data)

        except WebDriverException as e:
            print(f"An error occurred while interacting with the URL {url}: {str(e)}")
            continue  # Continue to the next URL

    # Fermer le navigateur Selenium
    driver.quit()
     # Créer un DataFrame à partir de la liste
    df_auchan = pd.DataFrame(data_list)

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
    
   # Web_scraping_Auchan_Kevajo_Ivoirshop = pd.concat([df_kevajo, df_auchan], ignore_index=True)

    # Si vous ne souhaitez pas réinitialiser les index, vous pouvez laisser ignore_index=False

    # Afficher le résultat
    return df_auchan




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

from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException, WebDriverException


def scrapping_AIK1(): 
    

    # Liste des URLs à traiter

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
            'Prix réel': price,
            'Prix du produit': promo,
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
        "https://kevajo.com/#" ]

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
            
            try:
                response = requests.get(page_url, timeout=500)
                response.raise_for_status()
                url_lien=page_url
            except requests.exceptions.RequestException as e:
                print(f"Erreur de connexion à {page_url} : {e}")
                url_lien=page_url
                #continue

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
    df_product_details[['Quantité', 'Unite']] = df_product_details['Quantité'].str.extract(r"([0-9,]+)\s*([a-zA-Z]+)")

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
    df_kevajo[["Prix du produit", 'Unite_monetaire']] = df_kevajo["Prix du produit"].str.extract(r"([0-9,]+)\s*([a-zA-Z]+)")
   
   #CODE AUCHAN ----------------------------------------------------------------------------------------------------------------------
    #from selenium import webdriver
    #from selenium.webdriver.common.by import By
    #from selenium.webdriver.support.ui import WebDriverWait
    #from selenium.webdriver.support import expected_conditions as EC
    #from bs4 import BeautifulSoup
    #import pandas as pd
    #from datetime import datetime

    # Liste des URLs
    urls = [ "https://www.auchan.ci/mes-courses/cuisine-et-arts-de-la-table/1X78GLOL",
"https://www.auchan.ci/selection-hygiene-beaute/CQQZ2P9E/cp",
      "https://www.auchan.ci/mes-courses/tampons/C3Z06PLP",
     "https://www.auchan.ci/mes-courses/fromages/CV5SB7BU",
      "https://www.auchan.ci/mes-courses/produits-laitiers-oeufs-fromages/UNBA9591",
      "https://www.auchan.ci/mes-courses/bieres-et-cidres/UR7QGB7B",
      "https://www.auchan.ci/mes-courses/viandes-charcuterie-et-poissons/COEBIN7N",
      "https://www.auchan.ci/mes-courses/produits-laitiers-oeufs-fromages/UNBA9591",
      "https://www.auchan.ci/mes-courses/fruits-et-legumes/CH0J2N7N",
      "https://www.auchan.ci/mes-courses/conserves-et-plats-cuisines/UBO4Q0Q1",
      "https://www.auchan.ci/mes-courses/rechercher?q=riz&queryId=a54ef1df64907375ef73a3313b5e2cce",
    "https://www.auchan.ci/mes-courses/hygiene-soins-homme/UED6QWQ1",
    "https://www.auchan.ci/mes-courses/thes-et-infusions/1JTX6N7N",
     "https://www.auchan.ci/mes-courses/dentifrices-et-bains-de-bouche/19D0SPBP",  
     "https://www.auchan.ci/mes-courses/cafes-moulus-grains-et-chicorees/1HRRAN7N",
    "https://www.auchan.ci/mes-courses/accessoires-de-cuisine-salle-de-bains-et-wc/U0DBAPBP",
    "https://www.auchan.ci/mes-courses/vins/CP7F2B7B",
    "https://www.auchan.ci/mes-courses/eaux/19Z32PBP",
    "https://www.auchan.ci/mes-courses/produits-laitiers-oeufs-fromages/UNBA9591",
    "https://www.auchan.ci/mes-courses/chiens/UZZ9GLOL",
    "https://www.auchan.ci/mes-courses/javel-et-produits-multi-usages/19XT4PBP",
    "https://www.auchan.ci/mes-courses/corps/CZ02LOLU",
    "https://www.auchan.ci/mes-courses/brioches-et-pains-de-mie/1LQB4N7N",
    "https://www.auchan.ci/mes-courses/soins-du-visage-et-maquillage/CX99MLOL",
    "https://www.auchan.ci/mes-courses/chocolat-en-poudre/1RB9SB7B",
    "https://www.auchan.ci/mes-courses/boissons-gazeuses-et-sirops/175K2PLP",
    "https://www.auchan.ci/mes-courses/bieres-et-cidres/UR7QGB7B",
    "https://www.auchan.ci/mes-courses/bonbons/1RNO6B7B",
    "https://www.auchan.ci/mes-courses/legumes-et-fruits/CRPYAB7B",
    "https://www.auchan.ci/les-nouveautes/CYXMB7NU/cp",
    "https://www.auchan.ci/mes-courses/boissons/UFRY2VYV",
    "https://www.auchan.ci/mes-courses/savons/UENW2QWQ",
    "https://www.auchan.ci/mes-courses/cereales/UPNJ4B7B",
    "https://www.auchan.ci/mes-courses/couches-et-couches-culottes/1ZOYSLOL",
    "https://www.auchan.ci/mes-courses/cafes/U0FDIPBP",
    "https://www.auchan.ci/mes-courses/soins-des-cheveux/UZZNGLOL",
    "https://www.auchan.ci/mes-courses/laits-et-boissons-lactees/C7NZMPLP",
    "https://www.auchan.ci/mes-courses/serviettes-hygieniques/1EOXAQWQ",
    "https://www.auchan.ci/special-de-2000fcfa/C5ESPLRU/cp",
    "https://www.auchan.ci/mes-courses/legumes-et-fruits/CRPYAB7B",
    "https://www.auchan.ci/mes-courses/cafes-moulus-grains-et-chicorees/1HRRAN7N",
    "https://www.auchan.ci/mes-courses/accessoires-de-cuisine-salle-de-bains-et-wc/U0DBAPBP",
    "https://www.auchan.ci/mes-courses/vins/CP7F2B7B",
    "https://www.auchan.ci/mes-courses/eaux/19Z32PBP",
    "https://www.auchan.ci/mes-courses/produits-laitiers-oeufs-fromages/UNBA9591",
    "https://www.auchan.ci/mes-courses/chiens/UZZ9GLOL",
    "https://www.auchan.ci/mes-courses/javel-et-produits-multi-usages/19XT4PBP",
    "https://www.auchan.ci/mes-courses/corps/CZ02LOLU",
    "https://www.auchan.ci/mes-courses/dosettes-et-capsules/C3DR2PLP",
    "https://www.auchan.ci/mes-courses/wc-et-canalisations/CHHW6N7N",
    "https://www.auchan.ci/mes-courses/cereales-et-petit-dejeuner-bebe/1Y9Z6B7B",
    "https://www.auchan.ci/mes-courses/colas-et-boissons-gazeuses/1NOJI959",
    "https://www.auchan.ci/mes-courses/volaille-gibier-lapin-et-autres/UZ9ZSLOL",
    "https://www.auchan.ci/mes-courses/plats-cuisines-en-conserve/1YLVIB7B",
    "https://www.auchan.ci/mes-courses/shampoings/URVNSB7B",
    "https://www.auchan.ci/mes-courses/gel-douche-et-bain/UEKIQWQ1",
    "https://www.auchan.ci/mes-courses/oeufs-beurres-et-creme/1LOLAN7N",
    "https://www.auchan.ci/mes-courses/produits-menagers-et-accessoires-de-la-maison/C3QIPLPU",
    "https://www.auchan.ci/mes-courses/cremes-fraiches/CBOBGQ0Q",
    "https://www.auchan.ci/mes-courses/ustensiles-de-cuisine/1Z7JILOL",
    "https://www.auchan.ci/mes-courses/conserves-et-plats-cuisines/UBO4Q0Q1",
    "https://www.auchan.ci/mes-courses/cremes-et-soins/1H0R6N7N",
    "https://www.auchan.ci/mes-courses/vaisselle-jetable/18LR6ELE",
    "https://www.auchan.ci/mes-courses/lessives-repassage-et-soin-du-linge/1KOW2N5N",
    "https://www.auchan.ci/mes-courses-avec-livraison/1V3AB7FC/cp",
    "https://www.auchan.ci/mes-courses-du-mois/CVYIB7FU/cp",
    "https://www.auchan.ci/mes-courses/ketchup-mayonnaise-moutarde-et-sauces/UO8ZSN7N",
    "https://www.auchan.ci/mes-courses/panaches/CRXB4B7B",
    "https://www.auchan.ci/mes-courses/fruits-et-legumes/CH0J2N7N",
    "https://www.auchan.ci/tout-a-moins-de-1000f/C5ESPLRU/cp",
    "https://www.auchan.ci/mes-courses/epicerie-salee/CENGQWQU",
    "https://www.auchan.ci/mes-courses/mouchoirs/CZVNGLOL",
    "https://www.auchan.ci/mes-courses/laits-en-poudre/C0PE2PBP",
    "https://www.auchan.ci/promos-can/U5DH4PLR/cp",
    "https://www.auchan.ci/mes-courses/surgeles/UYB4B7B1",
    "https://www.auchan.ci/mes-courses/beurres-margarines-et-autres/UJBB6N7N",
    "https://www.auchan.ci/mes-courses/eaux-minerales/C7ORAPLP",
    "https://www.auchan.ci/mes-courses/bebe/U8P06ELE",
    "https://www.auchan.ci/mes-courses/huiles-epices-condiments/CQQDSP9P",
    "https://www.auchan.ci/mes-courses/pates/10EAPBPC",
    "https://www.auchan.ci/mes-courses/riz/1QH6P9PC",
    "https://www.auchan.ci/mes-courses/vinaigres-vinaigrettes-et-sauces-crudites/UZXQSLOL",
    "https://www.auchan.ci/mes-courses/brioches-et-pains-de-mie/1LQB4N7N",
    "https://www.auchan.ci/mes-courses/soins-du-visage-et-maquillage/CX99MLOL",
    "https://www.auchan.ci/mes-courses/chocolat-en-poudre/1RB9SB7B",
    "https://www.auchan.ci/mes-courses/boissons-gazeuses-et-sirops/175K2PLP",
    "https://www.auchan.ci/mes-courses/bieres-et-cidres/UR7QGB7B",
    "https://www.auchan.ci/mes-courses/bonbons/1RNO6B7B",
    "https://www.auchan.ci/mes-courses/jambons-et-saucissons-de-porc/UVZP6B7B",
    "https://www.auchan.ci/mes-courses/farines/1J0Y2N7N",
    "https://www.auchan.ci/mes-courses/papiers-toilette/UVWF2B7B",
    "https://www.auchan.ci/mes-courses/aperitif/1EN9IQWQ",
    "https://www.auchan.ci/mes-courses/cuisine/CTPXSB5B",
    "https://www.auchan.ci/mes-courses/charcuterie/13NXAPLP"


      
        ]

    # Configuration du navigateur Selenium
    

    #options = webdriver.ChromeOptions()
    #options.add_argument("--headless")  # Pour exécuter le navigateur en arrière-plan
    #options.add_argument("--disable-gpu")  # Désactiver l'accélération GPU en mode headless
    #chrome_driver_path = '/usr/bin/chromedriver'
    #options.binary_location = "/usr/bin/google-chrome"  # Remplacez par l'emplacement réel de votre Chrome binary
    #options.add_argument(f"webdriver.chrome.driver={chrome_driver_path}")
    #driver = webdriver.Chrome(options=options)

    chrome_driver_path = '/usr/bin/chromedriver'
    chrome_service = webdriver.chrome.service.Service(chrome_driver_path)
    chrome_options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(service=chrome_service, options=chrome_options)
    

    # Initialiser une liste pour stocker les données
    data_list = []

    # Parcourir la liste des URLs
    for url in urls:
        driver.get(url)
        try:
            wait = WebDriverWait(driver, 10)  # Reduce wait time for better performance
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.js-jt-product-card')))

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            products = soup.select('.js-jt-product-card')

            for product in products:
                product_pid = product.get('cy-product-pid')
                product_title = product.select_one('.js-title-line').text.strip()
                product_brand = product.select_one('.js-brand-line').text.strip()
                product_image = product.select_one('.js-image-line').get('data-src')
                product_price = product.select_one('.js-price-line').text.strip()
                product_old_price = product.select_one('.js-wasPrice-line')
                url_lien = url

                if product_old_price:
                    product_old_price = product_old_price.text.strip()
                else:
                    product_old_price = None

                data = {
                    'N_ordre': product_pid,
                    'Libellé': product_title,
                    'code_site': url_lien,
                    'Code_ID_PE': product_brand,
                    'Product Image': product_image,
                    'Prix du produit': product_price,
                    'Product Old Price': product_old_price,
                    'date de collecte': datetime.now().strftime('%Y-%m-%d'),
                }

                product_link = product.select_one('.js-product-anchor')
                if product_link and product_link.has_attr('href'):
                    product_details_url = product_link['href']
                    driver.execute_script(f"window.open('{product_details_url}', '_blank');")
                    driver.switch_to.window(driver.window_handles[1])

                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.jt-breadcrumb-title-ellipsis span:last-child')))
                    detail_soup = BeautifulSoup(driver.page_source, 'html.parser')
                    description_element = detail_soup.select_one('.jt-description-content-wrapper p')
                    if description_element:
                        product_description = description_element.text.strip()
                    else:
                        product_description = None

                    data['Caractéristiques du produit'] = product_description

                    driver.close()
                    driver.switch_to.window(driver.window_handles[0])

                data_list.append(data)

        except WebDriverException as e:
            print(f"An error occurred while interacting with the URL {url}: {str(e)}")
            continue  # Continue to the next URL

    # Fermer le navigateur Selenium
    driver.quit()
     # Créer un DataFrame à partir de la liste
    df_auchan = pd.DataFrame(data_list)

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
    
    Web_scraping_Auchan_Kevajo_Ivoirshop = pd.concat([df_kevajo, df_auchan], ignore_index=True)

    # Si vous ne souhaitez pas réinitialiser les index, vous pouvez laisser ignore_index=False

    # Afficher le résultat
    return Web_scraping_Auchan_Kevajo_Ivoirshop




from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import requests
from urllib.parse import urljoin
import time
import re
from selenium.common.exceptions import WebDriverException, NoSuchElementException, StaleElementReferenceException

def scrapping_AIK():
    
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
        date_new = datetime.now().strftime('%Y-%m-%d')

        return {
            'date de collecte': date_new,
            'Libellé du produit': title,
            'Prix réel': price,
            'Prix du produit': promo,
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

    # URL de la page Kevajo
    urls_kevajo = ["https://kevajo.com/", "https://kevajo.com/product-category/pour-bebe/",
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
    
    urls_auchan = ["https://www.auchan.ci/mes-courses/cuisine-et-arts-de-la-table/1X78GLOL",
"https://www.auchan.ci/selection-hygiene-beaute/CQQZ2P9E/cp",
      "https://www.auchan.ci/mes-courses/tampons/C3Z06PLP",
     "https://www.auchan.ci/mes-courses/fromages/CV5SB7BU",
      "https://www.auchan.ci/mes-courses/produits-laitiers-oeufs-fromages/UNBA9591",
      "https://www.auchan.ci/mes-courses/bieres-et-cidres/UR7QGB7B",
      "https://www.auchan.ci/mes-courses/viandes-charcuterie-et-poissons/COEBIN7N",
      "https://www.auchan.ci/mes-courses/produits-laitiers-oeufs-fromages/UNBA9591",
      "https://www.auchan.ci/mes-courses/fruits-et-legumes/CH0J2N7N",
      "https://www.auchan.ci/mes-courses/conserves-et-plats-cuisines/UBO4Q0Q1",
      "https://www.auchan.ci/mes-courses/rechercher?q=riz&queryId=a54ef1df64907375ef73a3313b5e2cce",
    "https://www.auchan.ci/mes-courses/hygiene-soins-homme/UED6QWQ1",
    "https://www.auchan.ci/mes-courses/thes-et-infusions/1JTX6N7N",
     "https://www.auchan.ci/mes-courses/dentifrices-et-bains-de-bouche/19D0SPBP",  
     "https://www.auchan.ci/mes-courses/cafes-moulus-grains-et-chicorees/1HRRAN7N",
    "https://www.auchan.ci/mes-courses/accessoires-de-cuisine-salle-de-bains-et-wc/U0DBAPBP",
    "https://www.auchan.ci/mes-courses/vins/CP7F2B7B",
    "https://www.auchan.ci/mes-courses/eaux/19Z32PBP",
    "https://www.auchan.ci/mes-courses/produits-laitiers-oeufs-fromages/UNBA9591",
    "https://www.auchan.ci/mes-courses/chiens/UZZ9GLOL",
    "https://www.auchan.ci/mes-courses/javel-et-produits-multi-usages/19XT4PBP",
    "https://www.auchan.ci/mes-courses/corps/CZ02LOLU",
    "https://www.auchan.ci/mes-courses/brioches-et-pains-de-mie/1LQB4N7N",
    "https://www.auchan.ci/mes-courses/soins-du-visage-et-maquillage/CX99MLOL",
    "https://www.auchan.ci/mes-courses/chocolat-en-poudre/1RB9SB7B",
    "https://www.auchan.ci/mes-courses/boissons-gazeuses-et-sirops/175K2PLP",
    "https://www.auchan.ci/mes-courses/bieres-et-cidres/UR7QGB7B",
    "https://www.auchan.ci/mes-courses/bonbons/1RNO6B7B",
    "https://www.auchan.ci/mes-courses/legumes-et-fruits/CRPYAB7B",
    "https://www.auchan.ci/les-nouveautes/CYXMB7NU/cp",
    "https://www.auchan.ci/mes-courses/boissons/UFRY2VYV",
    "https://www.auchan.ci/mes-courses/savons/UENW2QWQ",
    "https://www.auchan.ci/mes-courses/cereales/UPNJ4B7B",
    "https://www.auchan.ci/mes-courses/couches-et-couches-culottes/1ZOYSLOL",
    "https://www.auchan.ci/mes-courses/cafes/U0FDIPBP",
    "https://www.auchan.ci/mes-courses/soins-des-cheveux/UZZNGLOL",
    "https://www.auchan.ci/mes-courses/laits-et-boissons-lactees/C7NZMPLP",
    "https://www.auchan.ci/mes-courses/serviettes-hygieniques/1EOXAQWQ",
    "https://www.auchan.ci/special-de-2000fcfa/C5ESPLRU/cp",
    "https://www.auchan.ci/mes-courses/legumes-et-fruits/CRPYAB7B",
    "https://www.auchan.ci/mes-courses/cafes-moulus-grains-et-chicorees/1HRRAN7N",
    "https://www.auchan.ci/mes-courses/accessoires-de-cuisine-salle-de-bains-et-wc/U0DBAPBP",
    "https://www.auchan.ci/mes-courses/vins/CP7F2B7B",
    "https://www.auchan.ci/mes-courses/eaux/19Z32PBP",
    "https://www.auchan.ci/mes-courses/produits-laitiers-oeufs-fromages/UNBA9591",
    "https://www.auchan.ci/mes-courses/chiens/UZZ9GLOL",
    "https://www.auchan.ci/mes-courses/javel-et-produits-multi-usages/19XT4PBP",
    "https://www.auchan.ci/mes-courses/corps/CZ02LOLU",
    "https://www.auchan.ci/mes-courses/dosettes-et-capsules/C3DR2PLP",
    "https://www.auchan.ci/mes-courses/wc-et-canalisations/CHHW6N7N",
    "https://www.auchan.ci/mes-courses/cereales-et-petit-dejeuner-bebe/1Y9Z6B7B",
    "https://www.auchan.ci/mes-courses/colas-et-boissons-gazeuses/1NOJI959",
    "https://www.auchan.ci/mes-courses/volaille-gibier-lapin-et-autres/UZ9ZSLOL",
    "https://www.auchan.ci/mes-courses/plats-cuisines-en-conserve/1YLVIB7B",
    "https://www.auchan.ci/mes-courses/shampoings/URVNSB7B",
    "https://www.auchan.ci/mes-courses/gel-douche-et-bain/UEKIQWQ1",
    "https://www.auchan.ci/mes-courses/oeufs-beurres-et-creme/1LOLAN7N",
    "https://www.auchan.ci/mes-courses/produits-menagers-et-accessoires-de-la-maison/C3QIPLPU",
    "https://www.auchan.ci/mes-courses/cremes-fraiches/CBOBGQ0Q",
    "https://www.auchan.ci/mes-courses/ustensiles-de-cuisine/1Z7JILOL",
    "https://www.auchan.ci/mes-courses/conserves-et-plats-cuisines/UBO4Q0Q1",
    "https://www.auchan.ci/mes-courses/cremes-et-soins/1H0R6N7N",
    "https://www.auchan.ci/mes-courses/vaisselle-jetable/18LR6ELE",
    "https://www.auchan.ci/mes-courses/lessives-repassage-et-soin-du-linge/1KOW2N5N",
    "https://www.auchan.ci/mes-courses-avec-livraison/1V3AB7FC/cp",
    "https://www.auchan.ci/mes-courses-du-mois/CVYIB7FU/cp",
    "https://www.auchan.ci/mes-courses/ketchup-mayonnaise-moutarde-et-sauces/UO8ZSN7N",
    "https://www.auchan.ci/mes-courses/panaches/CRXB4B7B",
    "https://www.auchan.ci/mes-courses/fruits-et-legumes/CH0J2N7N",
    "https://www.auchan.ci/tout-a-moins-de-1000f/C5ESPLRU/cp",
    "https://www.auchan.ci/mes-courses/epicerie-salee/CENGQWQU",
    "https://www.auchan.ci/mes-courses/mouchoirs/CZVNGLOL",
    "https://www.auchan.ci/mes-courses/laits-en-poudre/C0PE2PBP",
    "https://www.auchan.ci/promos-can/U5DH4PLR/cp",
    "https://www.auchan.ci/mes-courses/surgeles/UYB4B7B1",
    "https://www.auchan.ci/mes-courses/beurres-margarines-et-autres/UJBB6N7N",
    "https://www.auchan.ci/mes-courses/eaux-minerales/C7ORAPLP",
    "https://www.auchan.ci/mes-courses/bebe/U8P06ELE",
    "https://www.auchan.ci/mes-courses/huiles-epices-condiments/CQQDSP9P",
    "https://www.auchan.ci/mes-courses/pates/10EAPBPC",
    "https://www.auchan.ci/mes-courses/riz/1QH6P9PC",
    "https://www.auchan.ci/mes-courses/vinaigres-vinaigrettes-et-sauces-crudites/UZXQSLOL",
    "https://www.auchan.ci/mes-courses/brioches-et-pains-de-mie/1LQB4N7N",
    "https://www.auchan.ci/mes-courses/soins-du-visage-et-maquillage/CX99MLOL",
    "https://www.auchan.ci/mes-courses/chocolat-en-poudre/1RB9SB7B",
    "https://www.auchan.ci/mes-courses/boissons-gazeuses-et-sirops/175K2PLP",
    "https://www.auchan.ci/mes-courses/bieres-et-cidres/UR7QGB7B",
    "https://www.auchan.ci/mes-courses/bonbons/1RNO6B7B",
    "https://www.auchan.ci/mes-courses/jambons-et-saucissons-de-porc/UVZP6B7B",
    "https://www.auchan.ci/mes-courses/farines/1J0Y2N7N",
    "https://www.auchan.ci/mes-courses/papiers-toilette/UVWF2B7B",
    "https://www.auchan.ci/mes-courses/aperitif/1EN9IQWQ",
    "https://www.auchan.ci/mes-courses/cuisine/CTPXSB5B",
    "https://www.auchan.ci/mes-courses/charcuterie/13NXAPLP"]

    # Scrape product information for Kevajo
    df_kevajo = pd.DataFrame()
    for url in urls_kevajo:
        df_product = scrape_page(url)
        df_kevajo = pd.concat([df_kevajo, df_product], ignore_index=True)

    # If Kevajo scraping failed, move to Auchan
    if df_kevajo.empty:
        print("Kevajo site is not available. Switching to Auchan.")
        
        # List of Auchan URLs
        

        chrome_driver_path = '/usr/bin/chromedriver'
        chrome_service = webdriver.chrome.service.Service(chrome_driver_path)
        chrome_options = webdriver.ChromeOptions()
        driver = webdriver.Chrome(service=chrome_service, options=chrome_options)

        data_list = []

        for url in urls_auchan:
            driver.get(url)
            try:
                wait = WebDriverWait(driver, 10)
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.js-jt-product-card')))

                soup = BeautifulSoup(driver.page_source, 'html.parser')
                products = soup.select('.js-jt-product-card')

                for product in products:
                    product_pid = product.get('cy-product-pid')
                    product_title = product.select_one('.js-title-line').text.strip()
                    product_brand = product.select_one('.js-brand-line').text.strip()
                    product_image = product.select_one('.js-image-line').get('data-src')
                    product_price = product.select_one('.js-price-line').text.strip()
                    product_old_price = product.select_one('.js-wasPrice-line')
                    url_lien = url

                    if product_old_price:
                        product_old_price = product_old_price.text.strip()
                    else:
                        product_old_price = None

                    data = {
                        'N_ordre': product_pid,
                        'Libellé': product_title,
                        'code_site': url_lien,
                        'Code_ID_PE': product_brand,
                        'Product Image': product_image,
                        'Prix du produit': product_price,
                        'Product Old Price': product_old_price,
                        'date de collecte': datetime.now().strftime('%Y-%m-%d'),
                    }

                    product_link = product.select_one('.js-product-anchor')
                    if product_link and product_link.has_attr('href'):
                        product_details_url = product_link['href']
                        driver.execute_script(f"window.open('{product_details_url}', '_blank');")
                        driver.switch_to.window(driver.window_handles[1])

                        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.jt-breadcrumb-title-ellipsis span:last-child')))
                        detail_soup = BeautifulSoup(driver.page_source, 'html.parser')
                        description_element = detail_soup.select_one('.jt-description-content-wrapper p')
                        if description_element:
                            product_description = description_element.text.strip()
                        else:
                            product_description = None

                        data['Caractéristiques du produit'] = product_description

                        driver.close()
                        driver.switch_to.window(driver.window_handles[0])

                    data_list.append(data)

            except WebDriverException as e:
                print(f"An error occurred while interacting with the URL {url}: {str(e)}")
                continue

        driver.quit()
        df_auchan = pd.DataFrame(data_list)

        def extract_info(product_title):
            match = re.match(r'(?P<Libellé_du_produit>.*?)(?P<Quantité>\d+)(?P<Unité_de_mesure>[a-zA-Z]+)', product_title)
            if match:
                return match.group('Libellé_du_produit').strip(), match.group('Quantité'), match.group('Unité_de_mesure')
            else:
                return None, None, None

        df_auchan[['Libellé du produit', 'Quantité', 'unite de mesure']] = df_auchan["Libellé"].apply(extract_info).apply(pd.Series)

        df_auchan = df_auchan[[
            'N_ordre', 'code_site', 'Code_ID_PE', 'date de collecte', 'Libellé du produit', 'Caractéristiques du produit',
            'unite de mesure', 'Quantité', "Prix du produit"]]

        df_auchan[["Prix du produit", 'Unite_monetaire']] = df_auchan["Prix du produit"].str.extract(r"([0-9.]+)\s*([a-zA-Z]+)")

        Web_scraping_Auchan_Kevajo_Ivoirshop = df_auchan
    else:
        print("Kevajo site is available. Processing Kevajo data.")

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
        df_kevajo[["Prix du produit", 'Unite_monetaire']] = df_kevajo["Prix du produit"].str.extract(r"([0-9,]+)\s*([a-zA-Z]+)")

        
        
        #options = webdriver.ChromeOptions()
        #options.add_argument("--headless")
        #driver = webdriver.Chrome(options=options)
        chrome_driver_path = '/usr/bin/chromedriver'
        chrome_service = webdriver.chrome.service.Service(chrome_driver_path)
        chrome_options = webdriver.ChromeOptions()
        driver = webdriver.Chrome(service=chrome_service, options=chrome_options)

        data_list = []

        for url in urls_auchan:
            driver.get(url)
            try:
                wait = WebDriverWait(driver, 10)
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.js-jt-product-card')))

                soup = BeautifulSoup(driver.page_source, 'html.parser')
                products = soup.select('.js-jt-product-card')

                for product in products:
                    product_pid = product.get('cy-product-pid')
                    product_title = product.select_one('.js-title-line').text.strip()
                    product_brand = product.select_one('.js-brand-line').text.strip()
                    product_image = product.select_one('.js-image-line').get('data-src')
                    product_price = product.select_one('.js-price-line').text.strip()
                    product_old_price = product.select_one('.js-wasPrice-line')
                    url_lien = url

                    if product_old_price:
                        product_old_price = product_old_price.text.strip()
                    else:
                        product_old_price = None

                    data = {
                        'N_ordre': product_pid,
                        'Libellé': product_title,
                        'code_site': url_lien,
                        'Code_ID_PE': product_brand,
                        'Product Image': product_image,
                        'Prix du produit': product_price,
                        'Product Old Price': product_old_price,
                        'date de collecte': datetime.now().strftime('%Y-%m-%d'),
                    }

                    product_link = product.select_one('.js-product-anchor')
                    if product_link and product_link.has_attr('href'):
                        product_details_url = product_link['href']
                        driver.execute_script(f"window.open('{product_details_url}', '_blank');")
                        driver.switch_to.window(driver.window_handles[1])

                        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.jt-breadcrumb-title-ellipsis span:last-child')))
                        detail_soup = BeautifulSoup(driver.page_source, 'html.parser')
                        description_element = detail_soup.select_one('.jt-description-content-wrapper p')
                        if description_element:
                            product_description = description_element.text.strip()
                        else:
                            product_description = None

                        data['Caractéristiques du produit'] = product_description

                        driver.close()
                        driver.switch_to.window(driver.window_handles[0])

                    data_list.append(data)

            except WebDriverException as e:
                print(f"An error occurred while interacting with the URL {url}: {str(e)}")
                continue

        driver.quit()
        df_auchan = pd.DataFrame(data_list)

        def extract_info(product_title):
            match = re.match(r'(?P<Libellé_du_produit>.*?)(?P<Quantité>\d+)(?P<Unité_de_mesure>[a-zA-Z]+)', product_title)
            if match:
                return match.group('Libellé_du_produit').strip(), match.group('Quantité'), match.group('Unité_de_mesure')
            else:
                return None, None, None

        df_auchan[['Libellé du produit', 'Quantité', 'unite de mesure']] = df_auchan["Libellé"].apply(extract_info).apply(pd.Series)

        df_auchan = df_auchan[[
            'N_ordre', 'code_site', 'Code_ID_PE', 'date de collecte', 'Libellé du produit', 'Caractéristiques du produit',
            'unite de mesure', 'Quantité', "Prix du produit"]]

        df_auchan[["Prix du produit", 'Unite_monetaire']] = df_auchan["Prix du produit"].str.extract(r"([0-9.]+)\s*([a-zA-Z]+)")

        #Web_scraping_Auchan_Kevajo_Ivoirshop = df_auchan
        #Web_scraping_Auchan_Kevajo_Ivoirshop = df_kevajo
        Web_scraping_Auchan_Kevajo_Ivoirshop = pd.concat([df_kevajo, df_auchan], ignore_index=True)

    def lien_ivoire():
        # Liste des URLs principales à traiter
        main_urls = [
                "https://www.ivoirshop.ci/categorie-produit/supermache",
                "https://www.ivoirshop.ci/",
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

        # Liste pour stocker les URLs de toutes les pages
        all_page_urls = []

        # Parcourir chaque URL principale
        for main_url in main_urls:
            try:
                # Envoyer une requête HTTP à la page web et récupérer le contenu HTML
                response = requests.get(main_url)
                response.raise_for_status()  # Lève une exception si la réponse n'est pas OK

                html_content = response.text

                # Utiliser BeautifulSoup pour parser le contenu HTML
                soup = BeautifulSoup(html_content, "html.parser")

                # Extraire les liens des pages de pagination
                pagination_links = soup.select(".page-numbers a")

                # Liste pour stocker les URLs de toutes les pages pour cette URL principale
                page_urls = []

                # Parcourir les liens de pagination et extraire les URLs de toutes les pages
                for link in pagination_links:
                    href = link.get("href")
                    if href:
                        page_urls.append(href)

                # Supprimer les doublons et conserver uniquement les URLs uniques
                page_urls = list(set(page_urls))

                # Ajouter les URLs de cette URL principale à la liste globale
                all_page_urls.extend(page_urls)
            except requests.exceptions.RequestException as e:
                print(f"Une erreur est survenue lors de la récupération de {main_url}: {e}")


        # Supprimer les doublons et conserver uniquement les URLs uniques
        all_page_urls = list(set(all_page_urls))

        # Ajouter toutes les URLs principales et les URLs de pagination dans une seule liste
        all_urls = main_urls + all_page_urls

        return all_urls
    url= lien_ivoire()
    from selenium.webdriver.chrome.options import Options
    def scrapping_AIK_1(url): 
        # Configurez Selenium pour s'exécuter en mode headless
        #chrome_options = Options()
        #chrome_options.add_argument("--headless")
        #chrome_options.add_argument("--disable-web-security")  # Désactiver le blocage des cookies tiers
        #chrome_options.add_argument("--disable-cookie-encryption")  # Désactiver le chiffrement des cookies
        chrome_driver_path = '/usr/bin/chromedriver'
        chrome_service = webdriver.chrome.service.Service(chrome_driver_path)
        chrome_options = webdriver.ChromeOptions()
        driver = webdriver.Chrome(service=chrome_service, options=chrome_options)

        # Liste des URLs à traiter
        urls = url
        # Liste pour stocker les DataFrames de chaque site
        all_dfs = []  
        # Boucle à travers chaque URL  
        for url in urls:
            try:
                
                driver.get(url)
            # Le reste du code...
            except Exception as e:
                print("Une exception s'est produite :", e)
            try:
                # Attendre que les produits soient chargés
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "product"))) 
                # Récupérer le contenu de la page après le chargement dynamique
                html_content = driver.page_source
                soup = BeautifulSoup(html_content, "html.parser")
                # Liste pour stocker les informations des produits sur cette page
                products = []
                # Extraction des informations des produits
                product_tags = soup.find_all("li", class_="product")
                for product_tag in product_tags:
                    product_info = {}
                    product_info['N_ordre'] = ""
                    product_info["Code_ID_PE"] = ""
                    product_info['date de collecte'] = datetime.now().strftime('%Y-%m-%d')
                    product_info['URL'] = url  # Ajouter l'URL de la page
                    # Extraire le lien du produit
                    product_info["lien_produit"] = product_tag.find("a")["href"]
                    # Extraire le titre du produit
                    product_info["Libellé du produit"] = product_tag.find("h2", class_="woo-loop-product__title").text.strip()
                    # Extraire le prix du produit
                    price_tag = product_tag.find("span", class_="price")
                    product_info["Prix du produit"] = price_tag.find("ins").text.strip() if price_tag and price_tag.find("ins") else None
                    products.append(product_info)
                # Créer un DataFrame à partir des informations extraites
                df_ivoirshop = pd.DataFrame(products)
                # Ajouter le DataFrame à la liste globale
                all_dfs.append(df_ivoirshop)   
            except Exception as e:
                print(f"Une erreur est survenue lors de la récupération des données depuis {url}: {e}")
                continue  # Passer à l'URL suivante en cas d'erreur
        # Concaténer tous les DataFrames de chaque site en un seul DataFrame
        if all_dfs:
            df_ivoirshop = pd.concat(all_dfs, ignore_index=True)
        else:
            df_ivoirshop=pd.DataFrame()
    #--------------------------------------------------------------------------------------------------------------------
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
        if "lien_produit" in df_ivoirshop.columns:
            urls = list(df_ivoirshop["lien_produit"])
        else:
            urls=[]
            # Initialize an empty DataFrame to store the results
        combined_df_ivoirshop = pd.DataFrame()
        # Scrape product information for each URL and concatenate the results
        for url in urls:
            if url is not None:
                df_product = scrape_product_info(url)
                combined_df_ivoirshop = pd.concat([combined_df_ivoirshop, df_product], ignore_index=True)
            else:
                combined_df_ivoirshop = pd.concat([df_product], ignore_index=True)
    #--------------------------------------------------------------------------------------------------------------------------------------
        if 'Prix Réel' in combined_df_ivoirshop.columns:
            df_ivoirshop['Prix Réel'] = combined_df_ivoirshop['Prix Réel']
        else: 
            df_ivoirshop['Prix Réel'] =""
        if 'code_siteDescription' in combined_df_ivoirshop.columns:
            df_ivoirshop['code_siteDescription'] = combined_df_ivoirshop['code_siteDescription'] 
        else: 
            df_ivoirshop['code_siteDescription'] =""

        if 'Quantité' in combined_df_ivoirshop.columns:
            df_ivoirshop['Quantité'] = combined_df_ivoirshop['Quantité']
        else:
            df_ivoirshop['Quantité'] =""
        if 'Caractéristiques du produit' in combined_df_ivoirshop.columns:
            df_ivoirshop['Caractéristiques du produit'] = combined_df_ivoirshop['Caractéristiques du produit']
        else:    
            df_ivoirshop['Caractéristiques du produit'] =""    
    #-------------------------------------------------------------------------------------------------------------------------------------
        # Réorganiser les colonnes selon vos besoins
        if 'code_site'in df_ivoirshop.columns and "Prix du produit" in df_ivoirshop.columns:
            df_ivoirshop = df_ivoirshop[['N_ordre', 'code_site', 'Code_ID_PE', 'date de collecte', 'Libellé du produit', 'Caractéristiques du produit',
         'Quantité', "Prix du produit"]]
        else:
            df_ivoirshop==None
    #-------------------------------------------------------------------------------------------------------------------------------------
        if  "Prix du produit" in df_ivoirshop.columns:
            extracted_data = df_ivoirshop["Prix du produit"].str.extract(r"([0-9.]+)\s*([a-zA-Z]+)")
            df_ivoirshop.loc[:, "Prix du produit"] = extracted_data[0]
            df_ivoirshop.loc[:, 'Unite_monetaire'] = extracted_data[1]    
        return df_ivoirshop
    
    df_ivoirshop_1 = scrapping_AIK_1(url)    
    #Web_scraping_Auchan_Kevajo_Ivoirshop = pd.concat([Web_scraping_Auchan_Kevajo_Ivoirshop, df_ivoirshop_1], ignore_index=True)

    if df_ivoirshop_1 is None:
        Web_scraping_Auchan_Kevajo_Ivoirshop = Web_scraping_Auchan_Kevajo_Ivoirshop
    else:
        equivalences = {
        
        "lien_produit":'code_site', 
        }

        # Fonction pour renommer les colonnes du DataFrame en conservant les colonnes sans équivalence
        def renommer_colonnes(df, equivalences):
            colonnes_renommees = {ancien_nom: nouvel_nom for ancien_nom, nouvel_nom in equivalences.items() if
                                  nouvel_nom is not None}
            df_renomme = df.rename(columns=colonnes_renommees)
            return df_renomme
        df_ivoirshop_1=renommer_colonnes(df_ivoirshop_1, equivalences)
        df_ivoirshop_1 = df_ivoirshop_1[['N_ordre', 'code_site', 'Code_ID_PE', 'date de collecte', 'Libellé du produit', 'Caractéristiques du produit','Quantité', "Prix du produit"]]
        df_ivoirshop_1['Quantité'] = df_ivoirshop_1['Quantité'].str.extract('(\d+)')

        
        Web_scraping_Auchan_Kevajo_Ivoirshop = pd.concat([Web_scraping_Auchan_Kevajo_Ivoirshop, df_ivoirshop_1], ignore_index=True)
    
    driver.quit()
    
    
    return Web_scraping_Auchan_Kevajo_Ivoirshop
    






import time
import random

from datetime import datetime
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By

def fetch_jumia_data():
    chrome_driver_path = '/usr/bin/chromedriver'
    chrome_service = webdriver.chrome.service.Service(chrome_driver_path)
    chrome_options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(service=chrome_service, options=chrome_options)

    
    
    urls= [
    "https://www.jumia.ci/epicerie-produits-lies-au-tabac/?page={}#catalog-listing".format(page) for page in range(0, 3)]
    urls+= [
    "https://www.jumia.ci/mlp-supermarche-h-alimentaire/?page={}#catalog-listing".format(page) for page in range(0, 14)]
    
    
    data = []

    for url in urls:
        try:
            driver.get(url)
            time.sleep(random.uniform(1, 3))
            products = driver.find_elements(By.CLASS_NAME, "prd")

            for product in products:
                try:
                    product_name = product.find_element(By.CLASS_NAME, "name").text.strip()

                    try:
                        real_price = product.find_element(By.CLASS_NAME, "old").text.strip()
                    except:
                        real_price = "N/A"

                    try:
                        promo_price = product.find_element(By.CLASS_NAME, "prc").text.strip()
                    except:
                        promo_price = real_price

                    product_url = product.find_element(By.CLASS_NAME, "core").get_attribute("href")

                    try:
                        product_image = product.find_element(By.TAG_NAME, "img").get_attribute("data-src")
                    except:
                        product_image = "N/A"

                    date_de_collecte = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    data.append({
                        "Date_de_collecte": date_de_collecte,
                        "Code_site": "jumia",
                        "Libelle_du_produit": product_name,
                        "Real Price": real_price,
                        "Prix_du_produit": promo_price,
                        "Image URL": product_image,
                        "Product URL": product_url
                    })
                except Exception as e:
                    print(f"Error processing product: {e}")

            # Pause to avoid being blocked
            time.sleep(random.uniform(1, 3))
        except Exception as e:
            print(f"Error fetching URL {url}: {e}")

    jumia_main_df = pd.DataFrame(data)
    
    

    if "Product URL" not in jumia_main_df.columns:
        print("La colonne 'Product URL' n'existe pas dans le DataFrame. Assurez-vous que les données sont correctement extraites.")
        return

    product_urls = list(jumia_main_df["Product URL"])

    products_details_data = []

    for product_url in product_urls:
        try:
            driver.get(product_url)
            time.sleep(random.uniform(1, 3))
            product_title = driver.find_element(By.CLASS_NAME, "-fs20").text.strip()

            try:
                product_image = driver.find_element(By.CLASS_NAME, 'img').get_attribute("data-src")
            except:
                product_image = "N/A"

            try:
                paragraphs = driver.find_element(By.CLASS_NAME, 'markup').find_elements(By.TAG_NAME, 'p')
                description_lines = [p.text.strip() for p in paragraphs]
                product_description = "\n".join(description_lines)
            except:
                product_description = "N/A"

            products_details_data.append({
                "Product URL": product_url,
                "Titre du produit": product_title,
                "Image du produit": product_image,
                "Caracteristique": product_description
            })

            # Pause to avoid being blocked
            time.sleep(random.uniform(1, 3))
        except Exception as e:
            print(f"Error processing product URL {product_url}: {e}")

    driver.quit()

    products_details_df = pd.DataFrame(products_details_data)

    merged_df = pd.merge(jumia_main_df, products_details_df, on="Product URL", how="left")
    merged_df[["Prix_du_produit", 'Unite_monetaire']] = merged_df["Prix_du_produit"].str.extract(r"([0-9,]+)\s*([a-zA-Z]+)")
    merged_df[['Unite']] = 'Unite'
    merged_df[['Quantite']] = 'Quantite'
    merged_df = merged_df[['Date_de_collecte', 'Code_site', 'Libelle_du_produit', 'Quantite', "Prix_du_produit", 'Caracteristique', 'Unite', 'Unite_monetaire']] 
    return merged_df

#from Script_Scrapping_jumia import fetch_jumia_data
#from send_mail import send_mail_success, send_mail_error



from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import numpy as np
#Importation du module d'envoi de mail
from selenium import webdriver
#from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
#import dateparser

import os



def fetch_jumia_data1():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }

    urls = [
    "https://www.jumia.ci/beaute-hygiene-sante/?page={}&#catalog-listing".format(page) for page in range(0, 51)]
    urls += [
    "https://www.jumia.ci/telephone-tablette/?page={}&#catalog-listing".format(page) for page in range(0, 50)]
    urls += [
    "https://www.jumia.ci/electronique/?page={}&#catalog-listing".format(page) for page in range(0, 50)]
    urls += [
    "https://www.jumia.ci/maison-cuisine-jardin/?page={}&#catalog-listing".format(page) for page in range(0, 50)]
    urls += [
    "https://www.jumia.ci/ordinateurs-accessoires-informatique/?page={}&#catalog-listing".format(page) for page in range(0, 50)]
    urls += [
    "https://www.jumia.ci/fashion-mode/?page={}&#catalog-listing".format(page) for page in range(0, 50)]
    urls += [
    "https://www.jumia.ci/epicerie/?page={}&#catalog-listing".format(page) for page in range(0, 50)]
    urls += [
    "https://www.jumia.ci/beaute-hygiene-sante/?page={}&#catalog-listing".format(page) for page in range(0, 50)]
    urls += [
    "https://www.jumia.ci/bebe-puericulture/?page={}&#catalog-listing".format(page) for page in range(0, 50)]
    urls += [
    "https://www.jumia.ci/jardin-plein-air-ferme-ranch/?page={}&#catalog-listing".format(page) for page in range(0, 50)]
    urls += [
    "https://www.jumia.ci/jardin-plein-air-ferme-ranch/?page={}&#catalog-listing".format(page) for page in range(0, 50)]
    urls += [
    "https://www.jumia.ci/mlp-boutiques-officielles/?page={}&#catalog-listing".format(page) for page in range(0, 50)]
    
    data = []

    for url in urls:
        try:
            response = requests.get(url, headers=headers, timeout=100)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            if url == "https://www.jumia.ci/":
                products = soup.find_all("article", class_="prd _box _hvr")
                name_class = "div"
                real_price_class = "prc"
                promo_price_class = "prc _dsc"
            else:
                products = soup.find_all("article", class_="prd _fb col c-prd")
                name_class = "h3"
                real_price_class = "old"
                promo_price_class = "prc"

            for product in products:
                try:
                    product_name = product.find(name_class, class_="name").text.strip()
                    real_price_tag = product.find("div", class_=real_price_class)
                    real_price = real_price_tag.text.strip() if real_price_tag else "N/A"
                    promo_price_tag = product.find("div", class_=promo_price_class)
                    promo_price = promo_price_tag.text.strip() if promo_price_tag else real_price
                    product_url_tag = product.find("a", class_="core")
                    product_url = "https://www.jumia.ci" + product_url_tag["href"] if product_url_tag and 'href' in product_url_tag.attrs else "N/A"
                    product_image_tag = product.find("img")
                    product_image = product_image_tag["data-src"] if product_image_tag and 'data-src' in product_image_tag.attrs else "N/A"
                    date_de_collecte = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    data.append({
                        "Date_de_collecte": date_de_collecte,
                        "Code_site": "jumia",
                        "Libelle_du_produit": product_name,
                        "Real Price": real_price,
                        "Prix_du_produit": promo_price,
                        "Image URL": product_image,
                        "Product URL": product_url
                    })
                except Exception as e:
                    print(f"Error processing product: {e}")
        except Exception as e:
            print(f"Error fetching URL {url}: {e}")

    jumia_main_df = pd.DataFrame(data)

    if 'Product URL' in jumia_main_df.columns:
        product_urls = list(jumia_main_df["Product URL"])
    else:
        print("La colonne 'Product URL' est manquante dans le DataFrame.")
        product_urls = []

    products_details_data = []

    for product_url in product_urls:
        try:
            response = requests.get(product_url, headers=headers, timeout=100)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            product_title = soup.find('h1', class_="-fs20 -pts -pbxs").text.strip()
            product_image = soup.find('img', class_='img')["data-src"]
            paragraphs = soup.find('div', class_='markup -mhm -pvl -oxa -sc').find_all('p')
            description_lines = [p.text.strip() for p in paragraphs]
            product_description = "\n".join(description_lines)
            products_details_data.append({
                "Product URL": product_url,
                "Titre du produit": product_title,
                "Image du produit": product_image,
                "Caracteristique": product_description
            })
        except Exception as e:
            print(f"Error processing product URL {product_url}: {e}")

    products_details_df = pd.DataFrame(products_details_data)

    merged_df = pd.merge(jumia_main_df, products_details_df, on="Product URL", how="left")

    # Extraire les quantités et les unités
    merged_df[['Prix_du_produit', 'Unite_monetaire']] = merged_df["Prix_du_produit"].str.extract(r"([0-9,]+)\s*([a-zA-Z]+)")
    merged_df['Prix_du_produit'] = merged_df['Prix_du_produit'].fillna('N/A') 
    merged_df['Unite'] = 'Unite'  # Ajustez si nécessaire
    merged_df[['Unite']] ='Unite'
    merged_df[['Quantite']] ='Quantite'

    

    merged_df = merged_df[['Date_de_collecte', 'Code_site', 'Libelle_du_produit', 'Quantite', 'Prix_du_produit', 'Caracteristique', 'Unite', 'Unite_monetaire']]

    return merged_df


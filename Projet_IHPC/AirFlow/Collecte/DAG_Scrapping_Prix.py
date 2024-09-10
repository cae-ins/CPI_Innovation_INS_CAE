from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from selenium import webdriver
import pandas as pd
from bs4 import BeautifulSoup
import re
import numpy as np
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException, WebDriverException
import sys
import os
# Add the parent directory of 'modules' to the Python path


#from Script_Scrapping_jumia import fetch_jumia_data
from send_mail import send_mail_success, send_mail_error
from Script_Scrapping_cpi import scrapping_AIK
from Script_Scrapping_cpi import fetch_jumia_data


 
date_debut = datetime.strptime(datetime.now().strftime('%m-%Y'), "%m-%Y")

mois_en_cours=date_debut.strftime("%m%Y")                                                                                                                 
# Fonction de scraping JUMIA

'''
def scrap_jumia(**kwargs) :

    df3 = fetch_jumia_data()

    kwargs['ti'].xcom_push(key='jumia_data', value=df3.to_json())

'''



# Fonction de scraping AIK
def scrap_aik(**kwargs):
    try:
        df1 = scrapping_AIK()
        df2 = fetch_jumia_data()
        
        df1 = df1.rename(columns={'code_site': 'Code_site', 'date de collecte': 'Date_de_collecte', 'Libellé du produit': 'Libelle_du_produit','Caractéristiques du produit':'Caracteristique',"Prix du produit":"Prix_du_produit",'Quantité':'Quantite','unite de mesure':'Unite'})
        # Réaffectation des colonnes dans le nouvel ordre
        df1 = df1[['Date_de_collecte', 'Code_site', 'Libelle_du_produit','Quantite',"Prix_du_produit",'Caracteristique','Unite','Unite_monetaire']]
        
        df1= pd.concat([df1, df2], ignore_index=True)
        # Pusher les résultats dans XCom
        kwargs['ti'].xcom_push(key='aik_data', value=df1.to_json())
    
    except Exception as e:
        send_mail_error(["abdoulayebakayoko265@gmail.com", "doumbiaabdoulaye0525@gmail.com"], ["j.migone@stat.plan.gouv.ci","moussakr@gmail.com"])
        raise

# Fonction de scraping Adjovan
def get_description_text(driver, product_url):
    try:
        driver.get(product_url)
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        description_heading = soup.find('h2', text='Description')
        if description_heading:
            description_paragraph = description_heading.find_next('p')
            return description_paragraph.get_text(strip=True) if description_paragraph else None
        else:
            return "Pas de Caracteristique"
    except Exception as e:
        return f"Erreur dans la récupération de la description: {str(e)}"

def extraire_chiffres(value):
    chiffres = re.findall(r'\d+', value)
    return int(chiffres[0]) if chiffres else np.nan

def supprimer_zeros(value):
    if isinstance(value, float) and not pd.isnull(value):
        return re.sub(r'\.0$', '', str(value))
    return value

def scrap_adjovan(**kwargs):
    try:
        chrome_driver_path = '/usr/bin/chromedriver'
        chrome_service = webdriver.chrome.service.Service(chrome_driver_path)
        chrome_options = webdriver.ChromeOptions()
        driver = webdriver.Chrome(service=chrome_service, options=chrome_options)

        Category_Links = [
            'https://www.adjovan.com/product-category/fruits-legumes/',
            'https://www.adjovan.com/product-category/viande-volaille/',
            'https://www.adjovan.com/product-category/articles-pour-bebe/',
            'https://www.adjovan.com/product-category/boulangerie-patisserie/',
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

        for category_url in Category_Links:
            driver.get(category_url)
            driver.implicitly_wait(5)
            while True:
                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')
                current_url = driver.current_url
                products = soup.find_all('li', class_='product')
                for product in products:
                    title = product.find('h3', class_='product-name').text.strip()
                    price_element = product.find('span', class_='price')
                    price = price_element.text.strip() if price_element else "Prix non disponible"
                    quantity = title.split('[')[-1].split(']')[0]
                    date_de_collecte = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    product_link = product.find('div', class_='image-block').find('a')['href']
                    caracteristique = get_description_text(driver, product_link)
                    data.append({
                        "Date_de_collecte": date_de_collecte,
                        "Code_site": "adjovan",
                        "Libelle_du_produit": title,
                        "Quantite": quantity,
                        "Prix_du_produit": price,
                        "Caracteristique": caracteristique
                    })
                try:
                    driver.get(current_url)
                    driver.implicitly_wait(5)
                    next_page_link = driver.find_element(By.CSS_SELECTOR, 'a.next.page-numbers')
                    if 'disabled' in next_page_link.get_attribute('class'):
                        break
                    driver.execute_script("arguments[0].click();", next_page_link)
                    driver.implicitly_wait(5)
                except NoSuchElementException:
                    print("Dernière page atteinte.")
                    break

        driver.quit()
        df = pd.DataFrame(data)
        df['Unite'] = df['Quantite'].str.extract('([A-Za-z]+)')
        df['Quantite'] = df['Quantite'].apply(extraire_chiffres)
        df['Quantite'] = df['Quantite'].apply(supprimer_zeros)
        df['Unite_monetaire'] = "CFA"
        df['Prix_du_produit'] = df['Prix_du_produit'].str.replace('CFA', '').str.strip()

        # Pusher les résultats dans XCom
        kwargs['ti'].xcom_push(key='adjovan_data', value=df.to_json())

    except Exception as e:
        send_mail_error(["abdoulayebakayoko265@gmail.com", "doumbiaabdoulaye0525@gmail.com"], ["j.migone@stat.plan.gouv.ci","moussakr@gmail.com"])
        raise

def merge_and_save_data(**kwargs):
    try:
        #df_aik = scrap_aik()
        #df_adjovan = scrap_adjovan()
        ti = kwargs['ti']
        #df_jumia_json = ti.xcom_pull(key='jumia_data', task_ids='scrap_jumia')
        df_aik_json = ti.xcom_pull(key='aik_data', task_ids='scrap_aik')
        df_adjovan_json = ti.xcom_pull(key='adjovan_data', task_ids='scrap_adjovan')
        
        #df_jumia = pd.read_json(df_jumia_json)
        df_aik = pd.read_json(df_aik_json)
        df_adjovan = pd.read_json(df_adjovan_json)

        df_final = pd.concat([df_adjovan, df_aik], axis=0, ignore_index=True)
        fichier_sortie = os.path.join("/mnt","c","airflow","dags","WorkFlow_IHPC","Data_Collecte",mois_en_cours,f"Data_Scrapping_{datetime.now().strftime('%d%m%Y')}.xlsx")
        os.makedirs(os.path.dirname(fichier_sortie), exist_ok=True)
        df_final.to_excel(fichier_sortie, index=False)
        send_mail_success(["abdoulayebakayoko265@gmail.com", "doumbiaabdoulaye0525@gmail.com"], ["j.migone@stat.plan.gouv.ci","moussakr@gmail.com"])

    except Exception as e:
        send_mail_error(["abdoulayebakayoko265@gmail.com", "doumbiaabdoulaye0525@gmail.com"], ["j.migone@stat.plan.gouv.ci","moussakr@gmail.com"])
        raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date' : datetime(2024, 8, 5),
    'email': ['bakayokoabdoulaye2809@gmail.com'],
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Workflow_Scrapping_Prix',
    default_args=default_args,
    description='Workflow pour le scrapping des prix',
    catchup=False,
    schedule_interval ='30 01 * * *',
)

'''
task1 = PythonOperator(
    task_id='scrap_jumia',
    python_callable=scrap_jumia,
    provide_context=True,
    dag=dag,
)
'''

task2 = PythonOperator(
    task_id='scrap_aik',
    python_callable=scrap_aik,
    provide_context=True,
    dag=dag,
)


task3 = PythonOperator(
    task_id='scrap_adjovan',
    python_callable=scrap_adjovan,
    provide_context=True,
    dag=dag,
)

task4 = PythonOperator(
    task_id='merge_and_save_data',
    python_callable=merge_and_save_data,
    provide_context=True,
    dag=dag,
)

#task1 >> task2 >> task3 >> task4
task2 >> task3 >> task4

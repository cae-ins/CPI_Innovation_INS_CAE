from bs4 import BeautifulSoup
import pandas as pd
import requests

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
        # Envoyer une requête HTTP à la page web et récupérer le contenu HTML
        response = requests.get(main_url)
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

    # Supprimer les doublons et conserver uniquement les URLs uniques
    all_page_urls = list(set(all_page_urls))

    # Ajouter toutes les URLs principales et les URLs de pagination dans une seule liste
    all_urls = main_urls + all_page_urls

    return all_urls



#IMPORTATION DES MODULES :

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime

#CODE D'ENVOIE DU MAIL :

def send_mail_success(name, destinataire="bakayokoabdoulaye2809@gmail.com", cc="abdoulayebakayoko265@gmail.com", fichier="Donnee_Scrapping_Adjovan.xlsx"):
    # Création de l'e-mail
    msg = MIMEMultipart()
    msg['From'] = 'bakayoko.aboulaye562@gmail.com'
    msg['To'] = destinataire
    msg['Cc'] = cc
    msg['Subject'] = 'Testing Mail Suivi Carto Automatique'

    # Corps du message
    current_day = datetime.now().strftime("%Y-%m-%d")
    current_hour = datetime.now().strftime("%H")
    message = f'''Bonjour,
    Je vous prie de trouver en pièce jointe le point de la vérification cartographique du {current_day} à {current_hour}.
    Cordialement,
    Votre Nom'''
    msg.attach(MIMEText(message, 'plain'))

    # Pièce jointe
    attachment = open(fichier, "rb")
    part = MIMEBase('application', 'octet-stream')
    part.set_payload((attachment).read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', "attachment; filename= " + name + ".xlsx")
    msg.attach(part)

    # Connexion au serveur SMTP de Gmail
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()

    # Connexion à votre compte Gmail
    server.login("bakayoko.aboulaye562@gmail.com", "vmicqliakkajdaus")

    # Envoi de l'e-mail
    text = msg.as_string()
    server.sendmail("bakayokoabdoulaye2809@gmail.com", destinataire.split(",") + cc.split(","), text)

    # Fermeture de la connexion
    server.quit()

def send_mail_error(name, destinataire="bakayokoabdoulaye2809@gmail.com", cc="abdoulayebakayoko265@gmail.com", fichier="Donnee_Scrapping_Adjovan.xlsx"):
    # Création de l'e-mail
    msg = MIMEMultipart()
    msg['From'] = 'bakayoko.aboulaye562@gmail.com'
    msg['To'] = destinataire
    msg['Cc'] = cc
    msg['Subject'] = 'Testing Mail Suivi Carto Automatique'

    # Corps du message
    current_day = datetime.now().strftime("%Y-%m-%d")
    current_hour = datetime.now().strftime("%H")
    message = f'''Le lancement du JOB de scrapping du {current_day} à {current_hour} a échoué.
    Cordialement,
    BAKAYOKO Abdoulaye'''
    msg.attach(MIMEText(message, 'plain'))

    # Pièce jointe
    attachment = open(fichier, "rb")
    part = MIMEBase('application', 'octet-stream')
    part.set_payload((attachment).read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', "attachment; filename= " + name + ".xlsx")
    msg.attach(part)

    # Connexion au serveur SMTP de Gmail
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()

    # Connexion à votre compte Gmail
    server.login("bakayoko.aboulaye562@gmail.com", "vmicqliakkajdaus")

    # Envoi de l'e-mail
    text = msg.as_string()
    server.sendmail("bakayokoabdoulaye2809@gmail.com", destinataire.split(",") + cc.split(","), text)

    # Fermeture de la connexion
    server.quit()



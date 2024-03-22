import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime

def send_mail_success(name, destinataires=["bakayokoabdoulaye2809@gmail.com"], cc=["abdoulayebakayoko265@gmail.com"]):
    
    # Création de l'e-mail
    msg = MIMEMultipart()
    msg['From'] = 'bakayoko.aboulaye562@gmail.com'
    msg['To'] = ", ".join(destinataires)
    msg['Cc'] = ", ".join(cc)
    msg['Subject'] = 'COLLECTE JOURNALIERE DES DONNEES DE SCRAPPING'

    # Corps du message
    current_day = datetime.now().strftime("%Y-%m-%d")
    current_hour = datetime.now().strftime("%H")
    message = f'''Le Lancement du JOB de scrapping du {current_day} à {current_hour} a reussi !.
    Cordialement,
    BAKAYOKO Abdoulaye'''
    msg.attach(MIMEText(message, 'plain'))

    # Pièce jointe
    #attachment = open(fichier, "rb")
    #part = MIMEBase('application', 'octet-stream')
    #part.set_payload((attachment).read())
    #encoders.encode_base64(part)
    #part.add_header('Content-Disposition', "attachment; filename= " + name + ".xlsx")
    #msg.attach(part)

    # Connexion au serveur SMTP de Gmail
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()

    # Connexion à votre compte Gmail
    server.login("bakayoko.aboulaye562@gmail.com", "vmicqliakkajdaus")

    # Envoi de l'e-mail
    text = msg.as_string()
    server.sendmail("bakayoko.aboulaye562@gmail.com", destinataires + cc, text)

    # Fermeture de la connexion
    server.quit()

# Exemple d'utilisation
send_mail_success("Adjovan", ["bakayoko.aboulaye562@gmail.com", "abdoulayebakayoko265@gmail.com"], ["bakayokoabdoulaye2809@gmail.com"])
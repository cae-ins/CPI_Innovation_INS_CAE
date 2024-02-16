from Script_Scrapping_cpi import scrapping_AIK

try:

    df = scrapping_AIK()

    df.to_excel("Data_Scrapping_AIK"+datetime.now().strftime('%d%m%Y')+".xlsx", index=False)

    #ENVOIE DU MAIL:

    send_mail_success("Donnee_Scrapping_Adjovan", "bakayokoabdoulaye2809@gmail.com", "abdoulayebakayoko265@gmail.com", "Donnee_Scrapping_Adjovan.xlsx")
    print("Process sucess !!!")

except Exception as e :

    print("Il y a une erreur dans le code:", e) 
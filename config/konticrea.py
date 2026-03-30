import mysql.connector
from mysql.connector import Error
from config.config import Config as config

def connect_kit():
    try:
        connection = mysql.connector.connect(
            host=config.KONTICREA["host"],
            user=config.KONTICREA["username"],
            password=config.KONTICREA["password"],
            database=config.KONTICREA["database"]
            #port=config.KONTICREA["port"]
        )

        if connection.is_connected():
            return connection

    except Error as e:
        print("Erreur connexion serveur KIT :", e)
        return None
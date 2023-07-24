# ####################################################################### #
# APP: Program to fetch meteo predictions, sunrise and sunset             #
#                                                                         #
# Year:2019                                                               #
#                                                                         #
# ####################################################################### #
# # Datos Estación Meteorológica Colmenar Viejo. Prediccion y hora de amanecer y ocaso.

# import pandas to manage data, request to fetch url data, json to 
# manage the jsons and psycopg2 to 
# load data to postgress database

import pandas as pd #para gestionar eficientemente los datos directamente de json a pandas.
import requests
import json
import time
from datetime import datetime
# importamos la libreria de conexion a postgres. Antes hay que instalarla con pip3 install psycopg2-binary
import psycopg2
import bbdd_funciones
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from colorama import init as colorama_init # to have colered prints f to install "pip3 install colorama"
from colorama import Fore
from colorama import Style
from flask import Flask
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import os



# ####################################################################### #
# ####################################################################### #
# ####################################################################### #
# MAIN                                                                    #
# ####################################################################### #
# ####################################################################### #
# ####################################################################### #
database_ip="xx.xxx.xxx.xxx"
database_port=5432
database_db="YOUR_DB"
database_user="YOUR USER"
database_password="YOUR_PASS"
tiempo_centrales=25
tiempo_esquinas=5
ON=0
OFF=1
EMAIL_SERVER="YOUR EMAIL SERVER"
EMAIL_ALERT="YOUR_EMAIL_ADDRESS"

app = Flask(__name__)
@app.route("/")
def ETLdatossondaclima():
    http_body = '<p style = "font-family:perpetua;"><font color="ORANGE">FUNCTION TO ETL AEMET PREDICTION AND OTHER WHEATHER DATA</font><br> '
    http_body += '<p style = "font-family:perpetua;"><font color="red">EXTRACCION:</font><br> '
    mail_body = "Hola Santiago:\nLa salida de la ETL de prediccion y otros datos es:\n "

    # #Lo primero que hacemos es llamar a la primera URL
    # #que nos va a devolver si todo es correcto, la URL
    # #definitiva donde esta la informacion
    #Key es el key de la AEMET
    print("")
    print(f"{Fore.CYAN}*** *********************************************** ***{Style.RESET_ALL}")
    print(f"{Fore.CYAN}*** ACCESSO A DATOS DE LA API OPEN DATA DE AEMET    ***{Style.RESET_ALL}")
    print(f"{Fore.CYAN}*** *********************************************** ***{Style.RESET_ALL}")
    print("")

    key = "YOUR_KEY"

    #URI tiene la informacion de la peticion que vamos a hacer (cambia para cada llamada)
    uri = "opendata/api/prediccion/especifica/municipio/horaria/28082/?api_key=" #prediccion de Manzanares el Real.

    #montamos la URL y mostramos para ver que esta ok
    uri_total= "https://opendata.aemet.es/"+uri+key
    print(f"{Fore.GREEN}*** La URL a llamar:{Style.RESET_ALL}{Fore.YELLOW}https://opendata.aemet.es/{uri}YOUR_API_KEY_HERE{Style.RESET_ALL}")

    http_body += '<br><font color="green">La URL que nos dira la URL de los datos y de los metadatos:</font>'
    http_body += '<font color="blue">' +"https://opendata.aemet.es/"+ uri + '</font>'
    mail_body += "SOBRE EXTRACCION: \n   La URL que nos dira la URL de los datos y de los metadatos:"
    mail_body += "https://opendata.aemet.es/"+uri

    #hacemos la llamada y capturamos los datos en un objeto response, si lo imprimimos nos dice el resultado de HTTP
    response=requests.get(uri_total, verify=False)
    print(f"{Fore.GREEN}*** La respuesta es:{Style.RESET_ALL}{Fore.YELLOW}{response}{Style.RESET_ALL}")

    http_body += '<br><font color="green">La respuesta del servidor web es:</font><font color="blue"> ' + str(response.status_code) + '</font>'
    http_body += "</p>"
    mail_body += "\n   La respuesta del servidor web es:" + str(response.status_code)

    #Convertimos esa salida que es un JSON a un objeto JSON
    json_response= json.loads(response.text)
    #print(json_response)

    #Imprimimos el campo que tiene la URL buena
    print(f"{Fore.GREEN}*** La URL a llamar con datos prevision hoy, mañana y pasado, esta en json[datos]{Style.RESET_ALL}{Fore.YELLOW}{json_response['datos']}{Style.RESET_ALL}")

    http_body += '<br><font color="green">La URL de la que cogemos los datos:</font>'
    http_body += '<font color="blue">' + json_response['datos'] + '</font>'
    mail_body += "\n\nSOBRE EXTRACCION: \n   La URL de la que cogemos los datos: " + json_response['datos']

    #capturamos los datos de la URL buena, comprobando que ha cogido bien la información y lo pasamos a un json
    response2=requests.get(json_response["datos"], verify=False)
    print(f"{Fore.GREEN}*** La respuesta es:{Style.RESET_ALL}{Fore.YELLOW}{response2}{Style.RESET_ALL}")
    json_response2=json.loads(response2.text)

    #print(json.dumps(json_response2, indent=3))
    #print(json.dumps(json_response2[0]['prediccion']['dia'], indent=3))

    http_body += '<br><font color="green">La respuesta del servidor web es:</font><font color="blue"> ' + str(response2.status_code) + '</font>'
    http_body += "</p>"
    mail_body += "\n   La respuesta del servidor web es:" + str(response2.status_code)

    #lo pasamos a pandas con un dataFrame
    print(f"{Fore.GREEN}*** Pasamos a un DataFrame la prediccion de hoy mañana y pasado.{Style.RESET_ALL}")
    df=pd.DataFrame(json_response2[0]['prediccion']['dia'])

    print(f"{Fore.YELLOW}{df.to_markdown}{Style.RESET_ALL}")
    http_body += '<p style = "font-family:perpetua;"><font color="red">EXTRACCION: JSON de Predicciones para hoy, mañana y pasado</p>'
    mail_body += "\n\nTRANSFORMACION: JSON de Amaneceres y predicciones para hoy, mañana y pasado\n"

    #mostramos las columnas para luego poder ordenarlas como queremos
    #print (df.columns.tolist())
    http_body += '<p style = "font-family:perpetua;"><font color="red">TRANSFORMACIONES:</font></p>'
    mail_body += "\nTRANSFORMACIONES: "

    print("")
    print(f"{Fore.CYAN}*** *********************************************** ***{Style.RESET_ALL}")
    print(f"{Fore.CYAN}*** AMANECERES Y OCASOS                             ***{Style.RESET_ALL}")
    print(f"{Fore.CYAN}*** *********************************************** ***{Style.RESET_ALL}")
    print("")

    columnas_ordenadas=['fecha','orto','ocaso']
    print (f"{Fore.GREEN}*** Nos quedamos con las columnas de Amanecer y Ocaso:{Style.RESET_ALL}")
    print (f"{Fore.YELLOW}{df[columnas_ordenadas].to_markdown(tablefmt='grid')}{Style.RESET_ALL}")

    http_body += '<p style = "font-family:perpetua;"><font color="green">Transformación de Amaneceres y Ocasos</font</p>'
    http_body += '<p style = "font-family:monospace;"><font color="blue">' + df[columnas_ordenadas].to_html() +'</font></p>'
    mail_body += "Amaneceres y Ocasos\n" + df[columnas_ordenadas].to_markdown()


    #definimos la operacion SQL
    SQLupsert="insert into public.amaneceres  (fecha, orto, ocaso) values "
    SQLupsert_end=" on conflict (fecha) do "
    #conectamos con la tabla
    cur,con = bbdd_funciones.conectar_bd (database_ip,database_port,database_user,database_password, database_db,"select 1")

    print(f"{Fore.GREEN}*** Escribimos los datos en la BBDD{Style.RESET_ALL}")
    for index, fila in df.iterrows():
        cadena= "('"
        cadena+=fila['fecha'] + "','" + fila['orto'] + "','" + fila['ocaso']+ "'"
        cadena+=')'
        cadena2= "update set orto="
        cadena2+= "'" + fila['orto'] + "',"
        cadena2+= "ocaso=" + "'"+ fila['ocaso']+"'"
        #cadena2+="' where fecha='" +fila['fecha']+"'"
        SQL=SQLupsert + cadena + SQLupsert_end + cadena2
        #imprimimos el SQL a ver como queda
        #print (SQL)
        #print ("")
        #ejecutamos el upsert
        cur.execute(SQL)

    #al final del for actualizamos logs
    print(f"{Fore.GREEN}*** Escribimos en el log que hemos actualizado los amaneceres{Style.RESET_ALL}")
    bbdd_funciones.escribir_log(cur, con, "GoogleCloud","ACTUALIZAMOS AMANECERES","operacion normal")
    #con.rollback() #por si da error de current transaccion is aborted, commands ignored until end of trans
    con.commit() #hay que hacer esto para que se escriban
    ##cerramos la conexion
    bbdd_funciones.cerrar_conexion_bbdd (cur,con)

    http_body += '<p style = "font-family:perpetua"><font color="red">CARGA: de Amaneceres y Oscasos en la BBDD:</font><br>'
    mail_body += "\n\nCARGA: De amanceres y Oscasos en la BBDD:\n"



    # ############################################################################
    # cogemos la probabilidad de lluvias de los proximos 3 dias                  #
    # ############################################################################
    print("")
    print(f"{Fore.CYAN}*** *********************************************** ***{Style.RESET_ALL}")
    print(f"{Fore.CYAN}*** ESTIMACION DE LLUVIAS POR FRANJA HORARIA DE 72h ***{Style.RESET_ALL}")
    print(f"{Fore.CYAN}*** *********************************************** ***{Style.RESET_ALL}")
    print("")
    columnas_ordenadas=['fecha','probPrecipitacion']
    print(f"{Fore.GREEN}*** Tabla con la prob Precipirtacion{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}{df[columnas_ordenadas].to_markdown(tablefmt='grid')}{Style.RESET_ALL}")

    http_body += '<p style = "font-family:perpetua;"><font color="green">TRANSFORMACION de Probabilidad de Lluvia proximos 3 dias: Hoy, mañana, pasado.</font> </p>'
    http_body += '<p style = "font-family:monospace;"><font color="green" </p>' + df[columnas_ordenadas].to_html()
    mail_body += "\n\nTRANSFORMACION de Probabilidad de lluvia hoy mañana y pasado\n" + df[columnas_ordenadas].to_markdown()

    #print(f"{Fore.GREEN}*** Tabla con la probabilidad de lluvia de 8 a 14h, de 14 a 20h, de 20 a 2h, y de 2 a 8h de HOY{Style.RESET_ALL}")
    #print(f"{Fore.YELLOW}{df['probPrecipitacion'][0]}{Style.RESET_ALL}")
    #print(f"{Fore.GREEN}*** Tabla con la probabilidad de lluvia MAÑANA{Style.RESET_ALL}")
    #print(f"{Fore.YELLOW}{df['probPrecipitacion'][1]}{Style.RESET_ALL}")
    #print(f"{Fore.GREEN}*** Tabla con la probabilidad de lluvia PASADO MAÑANA{Style.RESET_ALL}")
    #print(f"{Fore.YELLOW}{df['probPrecipitacion'][2]}{Style.RESET_ALL}")

    cadena_insert=["","",""]
    cadena_upsert=["","",""]
    print(f"{Fore.GREEN}*** Metemos los datos en una lista para trabajar mejor y poder poner los valores que faltan a -1.{Style.RESET_ALL}")

    for index, fila in df.iterrows():

       i=index #para tener todas las cadenas de SQL en una lista y asi escribirlas mas facilmente
       ### Los datos tienden a estar ordenados luego deberian tener todos los dias 0208, 0814, 1420, 2002
       df2=pd.DataFrame(df['probPrecipitacion'][index])
       print (f"- {Fore.YELLOW}Dia:{fila['fecha']}{Style.RESET_ALL}")

       #### Tengo que hacer un bucle que recorre df2 pero a nivel de index no con iterrows, para ello sacar el tamaño del df
       filas_df2=int (df2.size/2)
       #print ("la longitud del df es: ",filas_df2) #para ssaber el numero de filas
       if filas_df2 == 4:
          #print ("ESTE DIA ESTA COMPLETO")
          cadena_insert[i]="('"+fila['fecha']+"',"+df2['value'][0]+","+df2['value'][1]+","+df2['value'][2]+","+df2['value'][3]+")"
          cadena_upsert[i]="do update SET periodo_0208="+df2['value'][0]+",periodo_0814="+df2['value'][1]+",periodo_1420="+df2['value'][2]+",periodo_2002="+df2['value'][3]
          #print (f"--- {Fore.LIGHTRED_EX}{cadena_insert[i]}{Style.RESET_ALL}")
          #print (f"--- {Fore.LIGHTRED_EX}{cadena_upsert[i]}{Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}{df2['periodo'][0]}{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}{df2['value'][0]}{Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}{df2['periodo'][1]}{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}{df2['value'][1]}{Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}{df2['periodo'][2]}{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}{df2['value'][2]}{Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}{df2['periodo'][3]}{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}{df2['value'][3]}{Style.RESET_ALL}")
       elif filas_df2 == 3 and df2['periodo'][0] == '0208':
          #print ("ESTE DIA LE FALTA 2002")
          cadena_insert[i]="('"+fila['fecha']+"',"+df2['value'][0]+","+df2['value'][1]+","+df2['value'][2]+",-1)"
          cadena_upsert[i]="do update SET periodo_0208="+df2['value'][0]+",periodo_0814="+df2['value'][1]+",periodo_1420="+df2['value'][2]
          #print (f"--- {Fore.LIGHTRED_EX}{cadena_insert[i]}{Style.RESET_ALL}")
          #print (f"--- {Fore.LIGHTRED_EX}{cadena_upsert[i]}{Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}{df2['periodo'][0]}{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}{df2['value'][0]}{Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}{df2['periodo'][1]}{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}{df2['value'][1]}{Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}{df2['periodo'][2]}{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}{df2['value'][2]}{Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}2002{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}Nulo (-1){Style.RESET_ALL}")
       elif filas_df2 == 3 and df2['periodo'][0] == '0814':
          #print ("ESTE DIA LE FALTA 0208")
          cadena_insert[i]="('"+fila['fecha']+"',-1,"+df2['value'][0]+","+df2['value'][1]+","+df2['value'][2]+")"
          cadena_upsert[i]="do update SET periodo_0814="+df2['value'][0]+",periodo_1420="+df2['value'][1]+",periodo_2002="+df2['value'][2]
          #print (f"--- {Fore.LIGHTRED_EX}{cadena_insert[i]}{Style.RESET_ALL}")
          #print (f"--- {Fore.LIGHTRED_EX}{cadena_upsert[i]}{Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}0208{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}Nulo (-1){Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}{df2['periodo'][0]}{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}{df2['value'][0]}{Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}{df2['periodo'][1]}{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}{df2['value'][1]}{Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}{df2['periodo'][2]}{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}{df2['value'][2]}{Style.RESET_ALL}")
       elif filas_df2 == 2 and df2['periodo'][0] == '0208':
          #print ("ESTE DIA LE FALTAN 1420 y 2002")
          cadena_insert[i]="('"+fila['fecha']+"',"+df2['value'][0]+","+df2['value'][1]+",-1,-1)"
          cadena_upsert[i]="do update SET periodo_0208="+df2['value'][0]+",periodo_0814="+df2['value'][1]
          #print (f"--- {Fore.LIGHTRED_EX}{cadena_insert[i]}{Style.RESET_ALL}")
          #print (f"--- {Fore.LIGHTRED_EX}{cadena_upsert[i]}{Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}{df2['periodo'][0]}{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}{df2['value'][0]}{Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}{df2['periodo'][1]}{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}{df2['value'][1]}{Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}1420{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}Nulo (-1){Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}2002{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}Nulo (-1){Style.RESET_ALL}")

       elif filas_df2 == 2 and df2['periodo'][0] == '0814':
          #print ("ESTE DIA LE FALTAN 0208 y 2002")
          cadena_insert[i]="('"+fila['fecha']+"',-1,"+df2['value'][0]+","+df2['value'][1]+",-1)"
          cadena_upsert[i]="do update SET periodo_0814="+df2['value'][0]+",periodo_1420="+df2['value'][1]
          #print (f"--- {Fore.LIGHTRED_EX}{cadena_insert[i]}{Style.RESET_ALL}")
          #print (f"--- {Fore.LIGHTRED_EX}{cadena_upsert[i]}{Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}0208{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}Nulo (-1){Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}{df2['periodo'][0]}{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}{df2['value'][0]}{Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}{df2['periodo'][1]}{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}{df2['value'][1]}{Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}2002{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}Nulo (-1){Style.RESET_ALL}")

       elif filas_df2 == 2 and df2['periodo'][0] == '1420':
          #print ("ESTE DIA LE FALTAN 0208 y 0814")
          cadena_insert[i]="('"+fila['fecha']+"',-1,-1,"+df2['value'][0]+","+df2['value'][1]+")"
          cadena_upsert[i]="do update SET periodo_1420="+df2['value'][0]+",periodo_2002="+df2['value'][1]
          #print (f"--- {Fore.LIGHTRED_EX}{cadena_insert[i]}{Style.RESET_ALL}")
          #print (f"--- {Fore.LIGHTRED_EX}{cadena_upsert[i]}{Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}0208{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}Nulo (-1){Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}0814{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}Nulo (-1){Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}{df2['periodo'][0]}{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}{df2['value'][0]}{Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}{df2['periodo'][1]}{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}{df2['value'][1]}{Style.RESET_ALL}")

       elif filas_df2 == 1 and df2['periodo'][0] == '0208':
          #print ("ESTE DIA LE FALTAN 0814, 1420 y 2002")
          cadena_insert[i]="('"+fila['fecha']+"',"+df2['value'][0]+",-1,-1,-1)"
          cadena_upsert[i]="do update SET periodo_0208="+df2['value'][0]
          #print (f"--- {Fore.LIGHTRED_EX}{cadena_insert[i]}{Style.RESET_ALL}")
          #print (f"--- {Fore.LIGHTRED_EX}{cadena_upsert[i]}{Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}{df2['periodo'][0]}{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}{df2['value'][0]}{Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}0814{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}Nulo (-1){Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}1420{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}Nulo (-1){Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}2002{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}Nulo (-1){Style.RESET_ALL}")

       elif filas_df2 == 1 and df2['periodo'][0] == '0814':
          #print ("ESTE DIA LE FALTAN 0208, 1420 y 2002")
          cadena_insert[i]="('"+fila['fecha']+"',-1,"+df2['value'][0]+",-1,-1)"
          cadena_upsert[i]="do update SET periodo_0814="+df2['value'][0]
          #print (f"--- {Fore.LIGHTRED_EX}{cadena_insert[i]}{Style.RESET_ALL}")
          #print (f"--- {Fore.LIGHTRED_EX}{cadena_upsert[i]}{Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}0208{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}Nulo (-1){Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}{df2['periodo'][0]}{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}{df2['value'][0]}{Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}1420{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}Nulo (-1){Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}2002{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}Nulo (-1){Style.RESET_ALL}")

       elif filas_df2 == 1 and df2['periodo'][0] == '1420':
          #print ("ESTE DIA LE FALTAN 0208, 0814 y 2002")
          cadena_insert[i]="('"+fila['fecha']+"',-1,-1,"+df2['value'][0]+",-1)"
          cadena_upsert[i]="do update SET periodo_1420="+df2['value'][0]
          #print (f"--- {Fore.LIGHTRED_EX}{cadena_insert[i]}{Style.RESET_ALL}")
          #print (f"--- {Fore.LIGHTRED_EX}{cadena_upsert[i]}{Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}0208{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}Nulo (-1){Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}0814{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}Nulo (-1){Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}{df2['periodo'][0]}{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}{df2['value'][0]}{Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}2002{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}Nulo (-1){Style.RESET_ALL}")

       elif filas_df2 == 1 and df2['periodo'][0] == '2002':
          #print ("ESTE DIA LE FALTAN 0208, 0814 y 1420")
          cadena_insert[i]="('"+fila['fecha']+"',-1,-1,-1,"+df2['value'][0]+")"
          cadena_upsert[i]="do update SET periodo_2002="+df2['value'][0]
          #print (f"--- {Fore.LIGHTRED_EX}{cadena_insert[i]}{Style.RESET_ALL}")
          #print (f"--- {Fore.LIGHTRED_EX}{cadena_upsert[i]}{Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}0208{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}Nulo (-1){Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}0814{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}Nulo (-1){Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}1420{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}Nulo (-1){Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}{df2['periodo'][0]}{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}{df2['value'][0]}{Style.RESET_ALL}")

       elif filas_df2 == 0:
          #print ("ESTE DIA LE FALTAN 0208, 0814, 1420 y 2002")
          cadena_insert[i]="('"+fila['fecha']+"',-1,-1,-1,-1)"
          cadena_upsert[i]="DO NOTHING"
          print (f"--- {Fore.LIGHTRED_EX}{cadena_insert[i]}{Style.RESET_ALL}")
          print (f"--- {Fore.LIGHTRED_EX}{cadena_upsert[i]}{Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}0208{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}Nulo (-1){Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}0814{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}Nulo (-1){Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}1420{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}Nulo (-1){Style.RESET_ALL}")
          print (f"--- {Fore.CYAN}Periodo:{Fore.YELLOW}2002{Fore.CYAN} probabilidad lluvia:{Fore.YELLOW}Nulo (-1){Style.RESET_ALL}")
    #definimos la operacion SQL
    SQLupsert="insert into public.problluvia (fecha, periodo_0208, periodo_0814, periodo_1420, periodo_2002) values "
    SQLupsert_end=" on conflict (fecha) "
    #conectamos con la tabla
    print (f"{Fore.GREEN}*** Conectando a la BBDD {Style.RESET_ALL}")
    cur,con = bbdd_funciones.conectar_bd (database_ip,database_port,database_user,database_password, database_db,"select 1")
    for i in range(3):
        SQL=SQLupsert + cadena_insert[i] + SQLupsert_end + cadena_upsert[i]
        #imprimimos el SQL a ver como queda
        print (f"{Fore.GREEN}*** Escribiendo sentencia:{Fore.YELLOW}{SQL}{Style.RESET_ALL}")
        #ejecutamos el upsert
        cur.execute(SQL)
    http_body += '<p style = "font-family:perpetua;"><font color="red">CARGA:de la Probabilidad de Lluvia proximos 3 dias: Hoy, mañana, pasado.</font> </p>'
    mail_body += "\n\nCARGA: de la Probabilidad de lluvia hoy mañana y pasado\n"

    #al final del for actualizamos logs
    print (f"{Fore.GREEN}*** Actualizamos el log con el dato probabilidad de lluvia {Style.RESET_ALL}")
    bbdd_funciones.escribir_log(cur, con, "Google Cloud","ACTUALIZAMOS PROBABILIDAD LLUVIA","operacion normal")
    con.commit() #hay que hacer esto para que se escriban

    ##cerramos la conexion
    bbdd_funciones.cerrar_conexion_bbdd (cur,con)

    # ###############################################################################
    # Vamos a coger ahora la estimacion de la temperatura para hoy
    # ###############################################################################
    http_body += '<p style = "font-family:perpetua;"><font color="green"> TRANSFORMACION prediccion Temperaturas proximos 3 dias: Hoy, mañana, pasado.</font> </p>'
    mail_body += "\n\nTransformacion de prediccion tempretarura 3 proximos dias \n"

    #lo pasamos a pandas con un dataFrame
    df=pd.DataFrame(json_response2[0]['prediccion']['dia'])
    columnas_ordenadas=['fecha','temperatura']
    print("")
    print(f"{Fore.CYAN}*** *********************************************** ***{Style.RESET_ALL}")
    print(f"{Fore.CYAN}*** PREVISION DE TEMPERATURAS 72h                   ***{Style.RESET_ALL}")
    print(f"{Fore.CYAN}*** *********************************************** ***{Style.RESET_ALL}")
    print("")

    #print(f"{Fore.YELLOW}{df[columnas_ordenadas]}{Style.RESET_ALL}")
    #print(df['temperatura'])
    #print(df['fecha'])
    fecha_hoy=datetime.strptime(df['fecha'][0], '%Y-%m-%dT%H:%M:%S')
    #print("LA FECHA DE ES",fecha_hoy.strftime("%Y-%m-%d") )
    df2=pd.DataFrame(df['temperatura'][0])
    #print(f"{Fore.GREEN}*** Temperaturas estimadas hoy{Style.RESET_ALL}")
    #print(f"{Fore.YELLOW}{df2.to_markdown(tablefmt='grid')}{Style.RESET_ALL}")

    fecha_manana=datetime.strptime(df['fecha'][1], '%Y-%m-%dT%H:%M:%S')
    #print("LA FECHA DE ES", fecha_manana.strftime("%Y-%m-%d"))
    df3=pd.DataFrame(df['temperatura'][1])
    #print(f"{Fore.GREEN}*** Temperaturas estimadas mañana{Style.RESET_ALL}")
    #print(f"{Fore.YELLOW}{df3.to_markdown(tablefmt='grid')}{Style.RESET_ALL}")

    fecha_pasadomanana=datetime.strptime(df['fecha'][2], '%Y-%m-%dT%H:%M:%S')
    #print("LA FECHA DE ES", fecha_pasadomanana.strftime("%Y-%m-%d"))
    df4=pd.DataFrame(df['temperatura'][2])
    #print(f"{Fore.GREEN}*** Temperaturas estimadas pasado mañana{Style.RESET_ALL}")
    #print(f"{Fore.YELLOW}{df4.to_markdown(tablefmt='grid')}{Style.RESET_ALL}")
    print(f"{Fore.GREEN}*** Temp previstas Hoy")
    print(f"{Fore.YELLOW}{df2.transpose().to_markdown(tablefmt='grid')}")

    http_body += '<p style = "font-family:perpetua;"><font color="green">Prevision Temperaturas hoy.</p>'
    http_body += '<p style = "font-family:monospace;"><font color="green" </p>' + df2.transpose().to_html()
    mail_body += "\n\nPrevision Temperatura Hoy:\n" + df2.transpose().to_markdown()

    print(f"{Fore.GREEN}*** Temp previstas Mañana")
    print(f"{Fore.YELLOW}{df3.transpose().to_markdown(tablefmt='grid')}")

    http_body += '<p style = "font-family:perpetua;"><font color="green">Prevision Temperaturas Mañana.</font> </p>'
    http_body += '<p style = "font-family:monospace;"><font color="green" </p>' + df3.transpose().to_html()
    mail_body += "\n\nPrevision Temperatura Mañana:\n" + df3.transpose().to_markdown()

    print(f"{Fore.GREEN}*** Temp previstas Pasado Mañana")
    print(f"{Fore.YELLOW}{df4.transpose().to_markdown(tablefmt='grid')}")

    http_body += '<p style = "font-family:perpetua;"><font color="green">Temperaturas pasado mañana.</font></p>'
    http_body += '<p style = "font-family:monospace;"><font color="green" </p>' + df4.transpose().to_html()
    mail_body += "\n\nPrevision Temperatura Pasado Mañana:\n" + df4.transpose().to_markdown()


    #definimos la operacion SQL
    SQLupsert="insert into public.probtemp (fint,ta) values "
    SQLupsert_end=" on conflict (fint) do "
    #conectamos con la tabla
    print(f"{Fore.GREEN}*** Insertamos estimaciones de temperaturas en la BBDD{Style.RESET_ALL}")
    cur,con = bbdd_funciones.conectar_bd (database_ip,database_port,database_user,database_password, database_db,"select 1")

    for index, fila in df2.iterrows():
        cadena= "('"
        cadena+=fecha_hoy.strftime("%Y-%m-%d")
        cadena+=" " + fila['periodo'] + ":00:00'," + fila['value']
        cadena+=')'
        cadena2= "update set ta="
        cadena2+= fila['value']

        SQL=SQLupsert + cadena + SQLupsert_end + cadena2
        #imprimimos el SQL a ver como queda
        #print (f"{Fore.GREEN}insertamos SQL:{Fore.YELLOW}{SQL}{Style.RESET_ALL}")
        #ejecutamos el upsert
        cur.execute(SQL)
    for index, fila in df3.iterrows():
        cadena= "('"
        cadena+=fecha_manana.strftime("%Y-%m-%d")
        cadena+=" " + fila['periodo'] + ":00:00'," + fila['value']
        cadena+=')'
        cadena2= "update set ta="
        cadena2+= fila['value']

        SQL=SQLupsert + cadena + SQLupsert_end + cadena2
        #imprimimos el SQL a ver como queda
        #print (f"{Fore.GREEN}insertamos SQL:{Fore.YELLOW}{SQL}{Style.RESET_ALL}")
        #ejecutamos el upsert
        cur.execute(SQL)

    for index, fila in df4.iterrows():
        cadena= "('"
        cadena+=fecha_pasadomanana.strftime("%Y-%m-%d")
        cadena+=" " + fila['periodo'] + ":00:00'," + fila['value']
        cadena+=')'
        cadena2= "update set ta="
        cadena2+= fila['value']

        SQL=SQLupsert + cadena + SQLupsert_end + cadena2
        #imprimimos el SQL a ver como queda
        #print (f"{Fore.GREEN}insertamos SQL:{Fore.YELLOW}{SQL}{Style.RESET_ALL}")
        #ejecutamos el upsert
        cur.execute(SQL)

    http_body += '<p style = "font-family:perpetua;"><font color="red">CARGA:en BBDD de la Probabilidad de Temperatura para 3 dias: Hoy, mañana, pasado.</font></p>'
    mail_body += "\n\nCARGA en la BBDD Probabilidad de temperatura hoy mañana y pasado\n"

    #al final del for actualizamos logs
    print (f"{Fore.GREEN}*** Actualizamos el log con el dato temperaturas estimadas {Style.RESET_ALL}")
    bbdd_funciones.escribir_log(cur, con, "Google Cloud","ACTUALIZAMOS PROB TEMPERATURAS","operacion normal")
    con.commit() #hay que hacer esto para que se escriban

    ##cerramos la conexion
    bbdd_funciones.cerrar_conexion_bbdd (cur,con)

    # ################################################################################
    # ## cogemos la estimacion de lluvia en cantidad para hoy
    ##################################################################################
    print("")
    print(f"{Fore.CYAN}*** *********************************************** ***{Style.RESET_ALL}")
    print(f"{Fore.CYAN}*** PREVISION DE CANTIDAD DE AGUA 72h               ***{Style.RESET_ALL}")
    print(f"{Fore.CYAN}*** *********************************************** ***{Style.RESET_ALL}")
    print("")
    #lo pasamos a pandas con un dataFrame
    df=pd.DataFrame(json_response2[0]['prediccion']['dia'])

    df2=pd.DataFrame(df['precipitacion'][0])
    df3=pd.DataFrame(df['precipitacion'][1])
    df4=pd.DataFrame(df['precipitacion'][2])

    print(f"{Fore.GREEN}*** Agua previstas Hoy (l/m2)")
    print(f"{Fore.YELLOW}{df2.transpose().to_markdown(tablefmt='grid')}{Style.RESET_ALL}")

    http_body += '<p style = "font-family:perpetua;"><font color="green">TRANSFORMACION Agua Prevista para hoy.</font> </p>'
    http_body += '<p style = "font-family:monospace;">' + df2.transpose().to_html() + '</p>'
    mail_body += "\n\nTRANSFORMACION Agua Prevista para hoy:\n" + df2.transpose().to_markdown(tablefmt='grid')

    print(f"{Fore.GREEN}*** Agua previstas Mañana (l/m2)")
    print(f"{Fore.YELLOW}{df3.transpose().to_markdown(tablefmt='grid')}{Style.RESET_ALL}")

    http_body += '<p style = "font-family:perpetua;"><font color="green">TRANSFORMACION Agua Prevista para mañana.</font> </p>'
    http_body += '<p style = "font-family:monospace;">' + df3.transpose().to_html() + '</p>'
    mail_body += "\n\nTRANSFORMACION Agua Prevista para mañana:\n" + df3.transpose().to_markdown()

    print(f"{Fore.GREEN}*** Agua previstas Pasado Mañana (l/m2)")
    print(f"{Fore.YELLOW}{df4.transpose().to_markdown(tablefmt='grid')}{Style.RESET_ALL}")

    http_body += '<p style = "font-family:perpetua;"><font color="green">TRANSFORMACION Agua Prevista para pasado mañana.</font> </p>'
    http_body += '<p style = "font-family:monospace;">' + df4.transpose().to_html() + '</p>'
    mail_body += "\n\nTRANSFORMACION Agua Prevista para pasado mañana:\n" + df4.transpose().to_markdown()

    #definimos la operacion SQL
    SQLupsert="insert into public.problluviahora (fint,cc) values "
    SQLupsert_end=" on conflict (fint) do "
    #conectamos con la tabla
    cur,con = bbdd_funciones.conectar_bd (database_ip,database_port,database_user,database_password, database_db,"select count(*) from public.problluviahora")

    print(f"{Fore.GREEN}*** Insertamos estimaciones de temperaturas en la BBDD{Style.RESET_ALL}")

    for index, fila in df2.iterrows():
        cadena= "('"
        cadena+=fecha_hoy.strftime("%Y-%m-%d")
        cadena+=" " + fila['periodo'] + ":00:00',"
        if fila['value'] == "Ip":
           #print ("es Ip")
           fila['value'] = "0"
        cadena+=fila['value']
        cadena+=')'
        cadena2= "update set cc="
        cadena2+= fila['value']

        SQL=SQLupsert + cadena + SQLupsert_end + cadena2
        #imprimimos el SQL a ver como queda
        #print (SQL)
        #ejecutamos el upsert
        cur.execute(SQL)

    for index, fila in df3.iterrows():
        cadena= "('"
        cadena+=fecha_manana.strftime("%Y-%m-%d")
        cadena+=" " + fila['periodo'] + ":00:00',"
        if fila['value'] == "Ip":
           #print ("es Ip")
           fila['value'] = "0"
        cadena+=fila['value']
        cadena+=')'
        cadena2= "update set cc="
        cadena2+= fila['value']

        SQL=SQLupsert + cadena + SQLupsert_end + cadena2
        #imprimimos el SQL a ver como queda
        #print (SQL)
        #ejecutamos el upsert
        cur.execute(SQL)

    for index, fila in df4.iterrows():
        cadena= "('"
        cadena+=fecha_pasadomanana.strftime("%Y-%m-%d")
        cadena+=" " + fila['periodo'] + ":00:00',"
        if fila['value'] == "Ip":
           #print ("es Ip")
           fila['value'] = "0"
        cadena+=fila['value']
        cadena+=')'
        cadena2= "update set cc="
        cadena2+= fila['value']

        SQL=SQLupsert + cadena + SQLupsert_end + cadena2
        #imprimimos el SQL a ver como queda
        #print (SQL)
        #ejecutamos el upsert
        cur.execute(SQL)

    http_body += '<p style = "font-family:perpetua;"><font color="red">CARGA: en BBDD de la Probabilidad de lluvia 3 dias: Hoy, mañana, pasado.</font></p>'
    mail_body += "\n\n\n CARGA: en la BBDD Probabilidad de lluvia hoy mañana y pasado\n\n\n\n Fin del Correo, pase un buen dia.\n\nJarbis"
    #al final del for actualizamos logs
    print (f"{Fore.GREEN}*** Actualizamos el log con el dato agua estimadas {Style.RESET_ALL}")
    bbdd_funciones.escribir_log(cur, con, "Google Cloud","ACTUALIZAMOS PROB LLUVIA HORAS","operacion normal")

    con.commit() #hay que hacer esto para que se escriban


    ##cerramos la conexion
    bbdd_funciones.cerrar_conexion_bbdd (cur,con)

    print(f"{Fore.BLUE}*** FIN DEL PROGRAMA{Style.RESET_ALL}")

    # Le mandamos un correo a quien esté configurado si nos pasamos en algun limite

    http_body += '<p style = "font-family:perpetua;"><font color="blue">ENVIO EMAIL</font></p>'
    # create message object instance
    print("-SENDING EMAIL:")
    msg = MIMEMultipart()

    message = mail_body

    # setup the parameters of the message
    password = "your_password"
    msg['From'] = "alertas@mole4.com"
    msg['To'] = EMAIL_ALERT
    msg['Subject'] = "RESUMEN ETL AEMET PREDICCION Y OCASOS"

    # add in the message body
    msg.attach(MIMEText(message, 'plain'))

    # create server
    server = smtplib.SMTP(EMAIL_SERVER + ': 25')

    # server.starttls()

    # Login Credentials for sending the mail
    # server.login(msg['From'], password)

    # send the message via the server.
    server.sendmail(msg['From'], msg['To'], msg.as_string())
    server.quit()

    print("   *** successfully sent email to %s:" % (msg['To']))

    print (f"{Fore.BLUE}*** FIN DEL PROGRAMA ***{Style.RESET_ALL}")

    return f"{http_body}"

if __name__ == "__main__":
   app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))



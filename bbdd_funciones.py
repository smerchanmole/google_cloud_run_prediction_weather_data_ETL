# importamos la libreria de conexion a postgres. Antes hay que instalarla con pip3 install psycopg2-binary
import psycopg2
import datetime

# ####################################################################### #
# Data Base functions to avoid complex main program reading.              #
# ####################################################################### #

# ####################################################################### #
# FUNTION: conectar_db                                                    #
# DESCRIPTION: Generate a connection to the database (postgreSQL)         #
# INPUT: Data needed to connect and the inital connection query           #
# OUTPUT: Cursor and Connection,  print error if happens                  #
# ####################################################################### #
def conectar_bd (PS_HOST, PS_PORT, PS_USER, PS_PASS, PS_DB, PS_QUERY):
    """Funcion para conectar con la base de datos, mandamos los datos de conexion y la consulta,
    devolvemos un array con cursor y connector"""
    #realizamos la conexión
    try:
        # Conectarse a la base de datos
        connstr = "host=%s port=%s user=%s password=%s dbname=%s" % (PS_HOST, PS_PORT, PS_USER, PS_PASS, PS_DB)
        conn = psycopg2.connect(connstr)

        # Abrir un cursor para realizar operaciones sobre la base de datos
        cur = conn.cursor()
        
        #Ejecutamos la peticion
        cur.execute(PS_QUERY)

             
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        if conn is not None:
            conn.close()
            print('*** **** Database connection closed.')   
    return cur, conn

# ####################################################################### #
# FUNTION: cerrar_conexion_bbdd                                           #
# DESCRIPTION: Close the connection                                       #
# INPUT: Data needed to close                                             #
# OUTPUT: Nothing                                                         #
# ####################################################################### #
def cerrar_conexion_bbdd (PS_CURSOR, PS_CONN):
    # Cerrar la conexión con la base de datos
    PS_CURSOR.close()
    PS_CONN.close()

# ####################################################################### #
# FUNTION: escribir_log                                                   #
# DESCRIPTION: Write the operation in a log table (just for info)         #
# INPUT: Data needed to write the log                                     #
# OUTPUT: 1                                                               #
# ####################################################################### #
def escribir_log (PS_CURSOR, PS_CONN, ip, comando, extra):
    # Escribimos el mensaje en la tabla logs. 
    x=datetime.datetime.now()
    # x.isoformat() para tener el timestamp formato ISO
    InsertLOG="INSERT INTO public.logs (hora, ip, comando, extra) values ('"+str(x.isoformat())+"','"+ip+"','"+comando+"','"+extra+"')"
    # print (InsertLOG)

    PS_CURSOR.execute(InsertLOG)
    return 1


# # Funciones para el riego. Simplificar la escritura
# ## escribe_riego: Escribe la zona y el tiempo en la bbdd
# ## activa_riego: activa una zona por un tiempo
# ## guarda_en_ddbb: conecta con la bbdd, escribe el riego, y cierra la conexion.
# ## def protocolo_si_llueve_no_riegues (cantidad_lluvia, horas_a_revisar):
#     

def escribe_riego (PS_CURSOR, PS_CONN, zona, tiempo):
    # Escribimos el tiempo y zona de riego en la tabla riego. 
    x=datetime.now()
    # x.isoformat() para tener el timestamp formato ISO
    InsertRIEGO="INSERT INTO public.riego (fecha, zona, tiempo) values ('"+str(x.isoformat())+"',"+str(zona)+","+str(tiempo)+")"
    print (InsertRIEGO)
    PS_CURSOR.execute(InsertRIEGO)
    return 1

def activa_riego (zona,tiempo):
   #activamos ese rele
   if tiempo !=0: 
      GPIO.output(zona, True)        #Encendemos rele
   
      time.sleep(tiempo)          #Dormimos X segunos
      GPIO.output(zona, False)       #Apagamos rele

   return 1

def guarda_en_bbdd (numero, tiempo):
    #conectamos a bbdd
    cur,con = conectar_bd (database_ip,database_port,database_user,database_password, database_db,"select count(*) from public.riego") 
    escribe_riego(cur,con,numero,tiempo) 
    # escribimos
    con.commit() #hay que hacer esto para que se escriban en las tabla
    #y cerramos, asi evitamos time out.
    cerrar_conexion_bbdd (cur,con)
    return 1



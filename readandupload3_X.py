#!/usr/bin/python3
import sqlite3 
import requests
import time
API_ENDPOINT ="https://polyproductos-gt.goandsee.co/api/v3/oee"
#API_ENDPOINT ="https://oeetest.goandsee.co/api/v3/oee"

###################################
#### Funciones auxiliares
#### Convertir un arreglo a un objeto
def to_remote_object(databaseRecord):
  temp_object = {}
  temp_object['date'] = databaseRecord[0]
  temp_object['sensor'] = databaseRecord[1]
  temp_object['eth_mac'] = databaseRecord[2]
  temp_object['date_time'] = databaseRecord[3]
  return temp_object
###################################

maximo_renglones=6000

### Conectar  ala base de datos
limitadorTemporal_A=time.time()

#tiempoA=time.time()
connection = sqlite3.connect('/home/pi/Desktop/hibrido/colector_X.db')
cursor = connection.cursor()

cursor.execute('SELECT COUNT(*) FROM oeerecords');
cur_result = cursor.fetchone()
n_renglones=cur_result[0]
print("CONTEO A: renglones ANTES",n_renglones)

if n_renglones>maximo_renglones:    
    n_renglones=maximo_renglones

n_renglones=str(n_renglones)

cursor.execute("SELECT * FROM oeerecords ORDER BY date LIMIT (?)",[n_renglones])
data = cursor.fetchall()

# Convertir a objetos para enviar
dataToSend = list(map(to_remote_object, data))

connection.close()

#tiempoB=time.time()
#duracion=tiempoB-tiempoA
#print("duracion SECTOR A---------------------- base de datos abierta", duracion)

post_request = requests.post(url = API_ENDPOINT, json = dataToSend)
    ##print("post request",post_request.status_code)

n_renglones=int(n_renglones)

    
if post_request.status_code == 200 and n_renglones>0:
    
    
    tiempoA=time.time()
    connection = sqlite3.connect('/home/pi/Desktop/hibrido/colector_X.db')
    cursor = connection.cursor()
    
    n_renglones=str(n_renglones)    
    cursor.execute("DELETE FROM oeerecords ORDER BY date LIMIT (?)",[n_renglones]);    
    connection.commit()
    connection.close()
    
    tiempoB=time.time()
    duracion=tiempoB-tiempoA
#    print("duracion SECTOR B---------------------- base de datos abierta", duracion)

    tiempoA=time.time()
    connection = sqlite3.connect('/home/pi/Desktop/hibrido/colector_X.db')
    cursor = connection.cursor()
    cursor.execute('SELECT COUNT(*) FROM oeerecords');
    cur_result = cursor.fetchone()
    n_renglones=cur_result[0]
    
    connection.commit()
    connection.close()    

    print("CONTEO A: renglones DESPUES",n_renglones) 
    tiempoB=time.time()
    duracion=tiempoB-tiempoA
#    print("duracion SECTOR B---------------------- base de datos abierta", duracion)

    limitadorTemporal_B=time.time()

    limitadorTemporal_Total=limitadorTemporal_B-limitadorTemporal_A

    while (n_renglones>120) and (limitadorTemporal_Total<45):
        
        tiempoA=time.time()
        
        connection = sqlite3.connect('/home/pi/Desktop/hibrido/colector_X.db')
        cursor = connection.cursor()
        cursor.execute('SELECT COUNT(*) from oeerecords');
        cur_result = cursor.fetchone()
        n_renglones=cur_result[0]
        
        print("CONTEO B:renglones ANTES",n_renglones)

        if n_renglones>maximo_renglones:
            print("fue mayor que :",maximo_renglones, ", de hecho fueron: ", n_renglones)
            n_renglones=maximo_renglones
            
        n_renglones=str(n_renglones)
        cursor.execute("SELECT * FROM oeerecords ORDER BY date LIMIT (?)",[n_renglones])     
        
        data = cursor.fetchall()
            #print("Data from database")
            #print(data)

            #Convertir a objetos para enviar
        dataToSend = map(to_remote_object, data)
            #print("Data to send")
            #print(dataToSend)

            #Enviar al endpoint
 #           try:

        connection.close()
        tiempoB=time.time()
        duracion=tiempoB-tiempoA
#        print("duracion SECTOR C---------------------- base de datos abierta", duracion)
        
        status_code = requests.post(url = API_ENDPOINT, json = dataToSend) #json = 
        #print(post_request.status_code)
        
        
        if (post_request.status_code==200):
            #n_renglones=str(n_renglones)
            
            tiempoA=time.time()
        
            connection = sqlite3.connect('/home/pi/Desktop/hibrido/colector_X.db')
            cursor = connection.cursor()
            
            n_renglones=str(n_renglones)
            cursor.execute("DELETE FROM oeerecords ORDER BY date LIMIT (?)",[n_renglones]);    
            connection.commit()
            connection.close()

            tiempoB=time.time()
    
            duracion=tiempoB-tiempoA
#            print("duracion SECTOR D---------------------- base de datos abierta", duracion)
            
            #time.sleep(5) 
            tiempoA=time.time()
        
            connection = sqlite3.connect('/home/pi/Desktop/hibrido/colector_X.db')
            cursor = connection.cursor()
            cursor.execute('SELECT COUNT(*) FROM oeerecords');
            cur_result = cursor.fetchone()
            n_renglones=cur_result[0]
            connection.close()

            duracion=tiempoB-tiempoA
#            print("duracion SECTOR D---------------------- base de datos abierta", duracion)

            
            print("CONTEO B: renglones DESPUES",n_renglones)
            
            limitadorTemporal_B=time.time()
            limitadorTemporal_Total=limitadorTemporal_B-limitadorTemporal_A


        else:
            print("se perdio conexion #1")
            ##Cerrar la base de datos
            connection.close()
  #          except:
            #print("No se subio, no borrar los datos. Tal vez no hay WIFI")

    #else:
     #   print("No se subio, no borrar los datos")
    connection.close()

#except:
else:
    if not (post_request.status_code == 200):
        print("se perdio conexion #2")
        print("n renglones",n_renglones)

    if n_renglones==0:
        print("no hay datos que subir #2 en upload")


    ##Cerrar la base de datos
    connection.close()



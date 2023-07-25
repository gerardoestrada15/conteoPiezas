#! /usr/bin/python3
import automationhat
from datetime import date
from datetime import datetime
import time
import ipaddress
import requests
import json
import os
import threading
import subprocess

import sqlite3

import random
import threading
import logging
from concurrent.futures import ThreadPoolExecutor
logging.basicConfig(level=logging.DEBUG, format='%(threadName)s: %(message)s')



proc = subprocess.Popen(["cat", "/sys/class/net/eth0/address"], stdout=subprocess.PIPE, shell=False)
(eth_mac, err) =proc.communicate()

#eth_mac="dc:a6:32:12:61:"
print("direccion MAC ",eth_mac)



three = False
two = False
one = False

deltaT=0
currT=0

prevT=time.time()
bandera_A=1
bandera_B=1
bandera_C=1

parA=1
parB=1
parC=1

diferenciaDeTiempoA=0
tiempo1A=0
tiempo2A=0
diferenciaDeTiempoB=0
tiempo1B=0
tiempo2B=0
diferenciaDeTiempoC=0
tiempo1C=0
tiempo2C=0

carrouselBuffer="X"

tresholdA=0.0
tresholdB=0.0
tresholdC=0.0

sensorA=1
sensorB=2
sensorC=3

contadorSensorA=0
contadorSensorB=0
contadorSensorC=0

contadorSensorCortoA=0
contadorSensorCortoB=0
contadorSensorCortoC=0



def escribe(eth_mac,boton,dateTIME,carrouselBuffer):
    sensor= boton
    #t1=time.time()
    #tiempoA=time.time()
    nowe =datetime.now()
    date_time=nowe.strftime("%m/%d/%Y, %H:%M:%S")
    eth_mac="dc:a6:32:12:61:e6"

    fecha= datetime.today().strftime('%Y-%m-%d')
    hora=date_time = nowe.strftime("%m/%d/%Y, %H:%M:%S")		
    date=time.time()*1000000
    #epoch=Date.now()
    date=str(date)
    date=date+date
    
    #    print(date[0:13])
    pulsos=[
    (date[0:13],sensor,eth_mac,date_time)
    ]
    
    if carrouselBuffer=="Z":
        conn = sqlite3.connect('/home/pi/Desktop/hibrido/colector_Z.db')
        
    elif carrouselBuffer=="X":
        conn = sqlite3.connect('/home/pi/Desktop/hibrido/colector_X.db')
        
    elif carrouselBuffer=="Y":
        conn = sqlite3.connect('/home/pi/Desktop/hibrido/colector_Y.db')
        
    elif carrouselBuffer=="A":
        conn = sqlite3.connect('/home/pi/Desktop/hibrido/colector_A.db')

    c = conn.cursor()
    c.executemany("INSERT INTO  oeerecords VALUES(?,?,?,?)",pulsos)
        
    conn.commit()
    conn.close() 
    #tiempoB=time.time()
    #duracion=tiempoB-tiempoA
    #print("duracion escritura",duracion)



 
#-----------------------------------------------------------------------------------------

def subir_datos_a_la_nubeX():
    #pass    
    subprocess.run([ "python","/home/pi/Desktop/hibrido/readandupload3_X.py"])
    #print("Subio a la nube en X")
def subir_datos_a_la_nubeY():
    #pass    
    subprocess.run([ "python","/home/pi/Desktop/hibrido/readandupload3_Y.py"])
    #print("Subio a la nube en Y")
def subir_datos_a_la_nubeZ():
    #pass    
    subprocess.run([ "python","/home/pi/Desktop/hibrido/readandupload3_Z.py"])
    #print("Sunio a la nube en Z")
def subir_datos_a_la_nubeA():
    subprocess.run([ "python","/home/pi/Desktop/hibrido/readandupload3_A.py"])    
    #subprocess.run([ "python","/home/pi/Desktop/UPLOAD_pruebasA.py"])
 


def treshold(diferenciaDeTiempo,treshold,sensor,contadorSensorCorto,contadorSensor,carrouselBuffer):
        nowe = datetime.now()
        date_time = nowe.strftime("%m/%d/%Y, %H:%M:%S")
        if diferenciaDeTiempo<treshold:
            contadorSensorCorto=contadorSensorCorto+1
            
            print(f"{date_time} Tiempo SENSOR {sensor} pequenno {diferenciaDeTiempo}, anomalia {contadorSensorCorto}")
        else:
            contadorSensor=contadorSensor+1
            escribe(eth_mac,sensor,date_time,carrouselBuffer)
 #           if contadorSensor%1==0:
            if sensor==2:
                pass
                #escribe(eth_mac,sensor,date_time,carrouselBuffer)
#                print(f"{sensor}- {date_time} normal {diferenciaDeTiempo}, numero {contadorSensor}")       
            print(f"{date_time} Tiempo SENSOR {sensor} grande {diferenciaDeTiempo}, evento {contadorSensor}")
         
        return contadorSensorCorto, contadorSensor
    
nowe = datetime.now()
date_time = nowe.strftime("%m/%d/%Y, %H:%M:%S.%f")
print("********************Iniciando programa...",date_time )

executor=ThreadPoolExecutor(max_workers=1)

#piezas=1
#tiempoPiezas1=time.time()
#totalPiezas=50+1

time.sleep(1)

while True:#piezas<totalPiezas:#True:
    currT=time.time()
    deltaT=currT-prevT

    if (deltaT > 60):# or (piezas==totalPiezas-1):
        
        prevT=currT
        nowe = datetime.now()
        date_time = nowe.strftime("%m/%d/%Y, %H:%M:%S")
        
        #tiempoA=time.time()
        
        if carrouselBuffer=="X":
            executor.submit(subir_datos_a_la_nubeY)
            tiempoB=time.time()
            #deltaT=tiempoB-tiempoA
            print(date_time,'Subiendo a la nube Y', deltaT)
            carrouselBuffer="Z"
            
        elif carrouselBuffer=="Y":
            executor.submit(subir_datos_a_la_nubeZ)
            tiempoB=time.time()
            #deltaT=tiempoB-tiempoA
            print(date_time,'Subiendo a la nube Z', deltaT)
            carrouselBuffer="X"
            
        elif carrouselBuffer=="Z":
            executor.submit(subir_datos_a_la_nubeX)
            tiempoB=time.time()
            #deltaT=tiempoB-tiempoA
            print(date_time,'Subiendo a la nube X', deltaT)
            carrouselBuffer="Y"

        elif carrouselBuffer=="A":
            #subir_datos_a_la_nubeA()
            executor.submit(subir_datos_a_la_nubeA)
            tiempoB=time.time()
            #deltaT=tiempoB-tiempoA
  #          print(date_time,'Subiendo a la nube A', deltaT)
            carrouselBuffer="A"
        #treshold=0.00001
        #if deltaT>treshold:
        #    pass
            #print("treshold",treshold,'Subiendo a la nube',deltaT)


    three= automationhat.input.three.is_on()
    if (three and bandera_C==1):
        bandera_C=0
                
        if parC==1:
            tiempo1C=time.time()
            diferenciaDeTiempoC=tiempo1C-tiempo2C
   #         print(date_time, "contador", contador_defectos, ", tiempo entre defectos",diferenciaDeTiempoX)
            parC=2
        elif parC==2:
            tiempo2C=time.time()
            parC=1
            diferenciaDeTiempoC=tiempo2C-tiempo1C
            
        CRONO_C1=time.time()
  #          print(date_time, "CONTADOR", contador_defectos, ", TIEMPO ENTRE DEFECTOS",diferenciaDeTiempoX)
        
#        contadorSensorCortoC,contadorSensorC=treshold(diferenciaDeTiempoC,tresholdC,3,contadorSensorCortoC,contadorSensorC)   
    elif (not three and bandera_C==0):
        bandera_C=1
        CRONO_C2=time.time()
        contadorSensorCortoC,contadorSensorC=treshold(CRONO_C2-CRONO_C1,tresholdC,3,contadorSensorCortoC,contadorSensorC,carrouselBuffer)   

 
 
    two= automationhat.input.two.is_off()
    if (two and bandera_B==1):
        bandera_B=0
        
        if parB==1:
            tiempo1B=time.time()
            diferenciaDeTiempoB=tiempo1B-tiempo2B
   #         print(date_time, "contador", contador_defectos, ", tiempo entre defectos",diferenciaDeTiempoX)
            parB=2
        elif parB==2:
            tiempo2B=time.time()
            parB=1
            diferenciaDeTiempoB=tiempo2B-tiempo1B
  #          print(date_time, "CONTADOR", contador_defectos, ", TIEMPO ENTRE DEFECTOS",diferenciaDeTiempoX)
            
 #       contadorSensorCortoB,contadorSensorB=treshold(diferenciaDeTiempoB,tresholdB,2,contadorSensorCortoB,contadorSensorB)
        CRONO_B1=time.time()
    elif (not two and bandera_B==0):
        bandera_B=1
        CRONO_B2=time.time()
        contadorSensorCortoB,contadorSensorB=treshold(CRONO_B2-CRONO_B1,tresholdB,2,contadorSensorCortoB,contadorSensorB,carrouselBuffer)  

    one= automationhat.input.one.is_off()
    if (one and bandera_A==1): #( not oneprev)
        nowe = datetime.now()
        date_time = nowe.strftime("%m/%d/%Y, %H:%M:%S")
        bandera_A=0

        if parA==1:
            parA=2
            tiempo1A=time.time()
            diferenciaDeTiempoA=tiempo1A-tiempo2A                      
        elif parA==2:
            tiempo2A=time.time()
            parA=1
            diferenciaDeTiempoA=tiempo2A-tiempo1A
        CRONO_A1=time.time()
#        contadorSensorCortoA,contadorSensorA=treshold(diferenciaDeTiempoA,tresholdA,1,contadorSensorCortoA,contadorSensorA)
    elif (not one and bandera_A==0):
        bandera_A=1
        CRONO_A2=time.time()
        contadorSensorCortoA,contadorSensorA=treshold(CRONO_A2-CRONO_A1,tresholdA,1,contadorSensorCortoA,contadorSensorA,carrouselBuffer)


#tiempoPiezas2=time.time()
#duracionPiezas=tiempoPiezas2-tiempoPiezas1
#frecuencia=(totalPiezas-1)/duracionPiezas
#print("Frecuencia de sampleo",frecuencia)


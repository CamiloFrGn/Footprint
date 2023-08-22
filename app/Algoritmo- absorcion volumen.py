# -*- coding: utf-8 -*-
"""
Created on Wed Apr 29 11:07:29 2020

@author: jsdelgadoc
"""


import modulo_conn_sql as mcq
import numpy as np
import pandas as pd 


def conectarSQL():
    conn = mcq.ConexionSQL()
    cursor = conn.getCursor()
    return cursor


def querySQL(query, parametros):
    #Conectar con base sql y ejecutar consulta
    cursor = conectarSQL()
    try:
        cursor.execute(query, parametros)
        #obtener nombre de columnas
        names = [ x[0] for x in cursor.description]
        
        #Reunir todos los resultado en rows
        rows = cursor.fetchall()
        resultadoSQL = []
            
        #Hacer un array con los resultados
        while rows:
            resultadoSQL.append(rows)
            if cursor.nextset():
                rows = cursor.fetchall()
            else:
                rows = None
                
        #Redimensionar el array para que quede en dos dimensiones
        resultadoSQL = np.array(resultadoSQL)
        resultadoSQL = np.reshape(resultadoSQL, (resultadoSQL.shape[1], resultadoSQL.shape[2]) )
    finally:
            if cursor is not None:
                cursor.close()
    return pd.DataFrame(resultadoSQL, columns = names)

def inicializarRedPlantas():
    plantasBase = [
    {"centro": "F001", "plantaUnica": "CO-PLANTA 240", "capMensual": 9520, "VolAsignado" : 0},
    {"centro": "F003", "plantaUnica": "CO-PLANTA BOSA", "capMensual": 10000, "VolAsignado" : 0},
    {"centro": "F049", "plantaUnica": "CO-PLANTA CALLE 170", "capMensual": 5000, "VolAsignado" : 0},
    {"centro": "F080", "plantaUnica": "CO-PLANTA CALLE 170", "capMensual": 5000, "VolAsignado" : 0},
    {"centro": "F004", "plantaUnica": "CO-PLANTA FONTIBÒN", "capMensual": 1300, "VolAsignado" : 0},
    {"centro": "F048", "plantaUnica": "CO-PLANTA PUENTE ARANDA", "capMensual": 0, "VolAsignado" : 0},
    {"centro": "F069", "plantaUnica": "CO-PLANTA PUENTE ARANDA", "capMensual": 0, "VolAsignado" : 0},
    {"centro": "F058", "plantaUnica": "CO-PLANTA SIBERIA", "capMensual": 6160, "VolAsignado" : 0},
    {"centro": "F030", "plantaUnica": "CO-PLANTA SOACHA", "capMensual": 10000, "VolAsignado" : 0},
    {"centro": "F006", "plantaUnica": "CO-PLANTA SUR (DTE)", "capMensual": 7200, "VolAsignado" : 0},
    {"centro": "F007", "plantaUnica": "CO-PLANTA TOCANCIPA", "capMensual": 6800, "VolAsignado" : 0},
    {"centro": "F067", "plantaUnica": "CO-PLANTA VISTA HERMOSA", "capMensual": 4080, "VolAsignado" : 0},
    {"centro": "F086", "plantaUnica": "CO-PLANTA VISTA HERMOSA", "capMensual": 0, "VolAsignado" : 0},
    {"centro": "FA06", "plantaUnica": "CO-PLANTA VILLAVICENCIO", "capMensual": 0, "VolAsignado" : 0}
    
    ]
    
    return plantasBase


def simularRedPlantas(clusterObjetivo, volumenObjetivo ):
    
    #Defino absorciones de plantas Fijas y CXO y por codigo de obra
    AbsorcionTipoPlanta_Cluster, AbsorcionObraCentral_Cluster = [], []
    
    AbsorcionTipoPlanta_Cluster = querySQL( "{CALL SCAC_AP1_VolumenClusterTipoPlanta_AbsorcionMovil (?,?)}", ("Colombia", 13) )
    AbsorcionObraCentral_Cluster = querySQL( "{CALL SCAC_AP2_VolumenObras_AbsorcionMovil_3opcionesDespacho (?,?)}", ("Colombia", 30) )
    
    redPlantas = inicializarRedPlantas()
    
    #-------------------------------------------
    
    cluster_objetivo = clusterObjetivo
    volumen_cluster_objetivo = volumenObjetivo
    volumen_central_objetivo = 0
    
    for i in AbsorcionTipoPlanta_Cluster:
        if i[1] == cluster_objetivo:
            volumen_central_objetivo = volumen_cluster_objetivo * float(i[2])
            #volumen_central_objetivo = volumen_cluster_objetivo * 0.875
            break
    print (volumen_central_objetivo)
    vol_control = 0
    planta_temp = ""
    obraAux = ""
    vol_asignado = False
    #Recorrer la  lista de obras con su correspondiente absorcion
    for obra in AbsorcionObraCentral_Cluster:
        vol_asignado = False
        obraAux = obra[1]
        #si la obra pertenece al cluster que estamos analizando ejecutar analisis, en otros caso continuar
        if obraAux != cluster_objetivo:
           continue
        #asigno el volumen a la planta opcion correspondiente
        else: 
            #Recorro las plantas opciones
            for k in range (4 , len(obra)):
                planta_temp = obra[k]
                
                for j in redPlantas:
                    #¿es la planta que estoy buscando? ¿Tiene capacidad?
                    if j["plantaUnica"] == planta_temp and  j["VolAsignado"] + (volumen_central_objetivo * float(obra[3])) <= j["capMensual"]:
                       j["VolAsignado"] += (volumen_central_objetivo * float(obra[3]))
                       vol_control +=      (volumen_central_objetivo * float(obra[3]))
                       vol_asignado = True
                       #el volumen fue ubicado asi que no tengo que buscar mas opciones
                       break
                if(vol_asignado):
                    break
                   
    return redPlantas


         





clusterObjetivo = "CLUSTER CENTRO"
volumenObjetivo = 64000
#Defino absorciones de plantas Fijas y CXO y por codigo de obra
#AbsorcionTipoPlanta_Cluster, AbsorcionObraCentral_Cluster = [], []

#Defino absorciones por cada codigo de obra
AbsorcionTipoPlanta_Cluster = querySQL( "{CALL SCAC_AP1_VolumenClusterTipoPlanta_AbsorcionMovil (?,?)}", ("Colombia", 13) )
AbsorcionObraCentral_Cluster = querySQL( "{CALL SCAC_AP2_VolumenObras_AbsorcionMovil_3opcionesDespacho (?,?)}", ("Colombia", 30) )
#AbsorcionObraCentral_Cluster = AbsorcionObraCentral_Cluster[AbsorcionObraCentral_Cluster['Cluster'] == clusterObjetivo]

#Consulto los componenentes 
componentes_ciclo = querySQL( "SELECT * FROM AV37_Componentes_Ciclo_Malla_Turnos_Clientes_Tabla", () )

redPlantas = inicializarRedPlantas()

 #-------------------------------------------

cluster_objetivo = clusterObjetivo
volumen_cluster_objetivo = volumenObjetivo

volumen_central_objetivo =AbsorcionTipoPlanta_Cluster[AbsorcionTipoPlanta_Cluster["Cluster"] == cluster_objetivo]

        
vol_control = 0
planta_temp = ""
obraAux = ""
vol_asignado = False
#Recorrer la  lista de obras con su correspondiente absorcion
for obra in AbsorcionObraCentral_Cluster:
    
    opciones = componentes_ciclo[componentes_ciclo['Cliente']] == obra[obra["Obra"]]

        
    
#resultado = simularRedPlantas("CLUSTER CENTRO", 8318)
#resultado = pd.DataFrame(resultado)
#resultado .to_excel('C:/Users/JSDELGADOC/Documents/Asignaciones Concreto/Proyectos/MachineLearning/resultados_Simulacion_14May_2.xlsx')


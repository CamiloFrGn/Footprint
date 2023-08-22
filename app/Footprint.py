# -*- coding: utf-8 -*-
"""
Created on Tue Apr 20 11:40:28 2021

@author: jsdelgadoc
"""

import pandas as pd
import plotly.express as px  
import plotly.graph_objects as go
from plotly.subplots import make_subplots


import dash 
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import dash_table as dtable
from dash_table.Format import Format, Scheme, Trim


import modulo_conn_sql as mcq
import numpy as np


app = dash.Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])
df_tabla_recursos =pd.DataFrame()
df_matriz_volumen_recursos = pd.DataFrame()

def conectarSQL():
    conn = mcq.ConexionSQL()
    cursor = conn.getCursor()
    return cursor

#Query BD SQL-Server Cemex
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

def percentile(n):
    def percentile_(x):
        return np.percentile(x, n)
    percentile_.__name__ = 'percentile_%s' % n
    return percentile_

@app.callback(
    [Output('tabla_recursos', 'columns'),
     Output('tabla_recursos', 'data'),
     Output('camiones', 'figure'),
     Output('volumen', 'figure')],
    [Input('paises', 'value'),
     Input('versiones', 'value'),
     Input('gen_table', 'n_clicks')]
    )
def generar_tabla_recursos(pais, version, n):
    #carga de datos para calculo de camiones (ventana horaria, ciclo, dropsize, desagregacion)
    if n is not None:
        df_desagregacion = querySQL("SELECT * FROM SCAC_AV7_DesagregacionPronosticoCiudadPlantaDiaTabla  WHERE Version = ? AND Pais = ? " , (version, pais))
        df_nombre_cluster = querySQL("SELECT * FROM SCAC_AT1_NombreCluster WHERE Pais = ? AND Activo = 1 " , (pais))
        df_ventana_horaria = querySQL("SELECT * FROM  SCAC_AV9_VentanaHoraria WHERE Pais = ?" , (pais))
        df_ciclo = querySQL("SELECT * FROM  AV37_Componentes_Ciclo_Malla_Turnos_Clientes_Tabla" , ())
        df_dropsize = querySQL("SELECT * FROM  SCAC_AV10_Dropsize" , ())
        
        #listado de obras activas en los ultimos 30 dias
        
        #df_lista_obras = querySQL("select distinct obra from scac_at2_condensadoservicio where fechaentrega >= getdate() - 30" , ())
        #filtro los tiempos de ciclo que corresponden unicamente a las obras activas
        #df_ciclo = pd.merge(df_ciclo, df_lista_obras, how='inner', left_on='Cliente', right_on='obra' )
        
        #copio los df para no estar halando los datos del sql cada vez que se realizan pruebas
        desagregacion = df_desagregacion.copy()
        nombre_cluster = df_nombre_cluster.copy()
        ventana_horaria = df_ventana_horaria.copy()
        ciclo = df_ciclo.copy()
        dropsize = df_dropsize.copy()
        
        dropsize['Dropsize'] = dropsize['Dropsize'].astype(float)
        dropsize = dropsize.groupby('Planta Unica')['Dropsize'].mean().reset_index()
        
        nombre_cluster = pd.DataFrame( nombre_cluster[['Desc Cluster', 'Planta Unica']]).drop_duplicates()
        
        ventana_horaria = ventana_horaria.fillna(value=np.nan)
        ventana_horaria ['VentanaHoraria'] = ventana_horaria ['VentanaHoraria'].astype(float) 
        ciclo = ciclo.fillna(value=np.nan)
        
        desagregacion = pd.merge(desagregacion, nombre_cluster[['Planta Unica', 'Desc Cluster']], how='left', left_on='PlantaUnica', right_on='Planta Unica').drop('Planta Unica',1)
        desagregacion = desagregacion.groupby(['Pais', 'Desc Cluster', 'PlantaUnica', 'FechaEntrega', 'Version'])['M3Forecast'].sum().reset_index()
        
        
        desagregacion['M3Forecast'] = desagregacion['M3Forecast'].astype(float)
        ventana_horaria = ventana_horaria.groupby(['Nombre Centro'])['VentanaHoraria'].mean().reset_index()
        ciclo_pivot = pd.pivot_table(
            ciclo,
            index='Planta',
            values=['T.Cargue','T.Planta','T.Ida', 'T.Obra', 'T.Regreso'],
            aggfunc = np.mean
            )
        
        ciclo_pivot['ciclo_total'] = ciclo_pivot['T.Cargue'] + ciclo_pivot['T.Planta'] + ciclo_pivot['T.Ida'] + ciclo_pivot['T.Obra'] + ciclo_pivot['T.Regreso']
        ciclo_pivot = pd.DataFrame(ciclo_pivot[['ciclo_total']])
        matriz_recursos = pd.merge(desagregacion, ventana_horaria, how='left', left_on='PlantaUnica', right_on='Nombre Centro' ).drop("Nombre Centro", 1)
        matriz_recursos = pd.merge(matriz_recursos, ciclo_pivot, how = 'left', left_on='PlantaUnica', right_on='Planta' )
        matriz_recursos = pd.merge(matriz_recursos, dropsize, how = 'left', left_on='PlantaUnica', right_on='Planta Unica' ).drop('Planta Unica', 1)
        matriz_recursos  = matriz_recursos.fillna(matriz_recursos.mean())
        matriz_recursos['CamionesRodando'] = (matriz_recursos['M3Forecast'] * (matriz_recursos['ciclo_total']/60 )) / (matriz_recursos['VentanaHoraria'] * matriz_recursos['Dropsize'])
        
        resumen_camiones_rodando = matriz_recursos.groupby(['Desc Cluster','PlantaUnica']).agg({'VentanaHoraria':'mean', 'ciclo_total':'mean', 'Dropsize':'mean', 'M3Forecast': 'sum', 'CamionesRodando':[percentile(50), percentile(65), percentile(75)] })
        resumen_camiones_rodando.columns = [' '.join(col).strip() for col in resumen_camiones_rodando.columns.values]
        resumen_camiones_rodando = resumen_camiones_rodando.reset_index()
    
        matriz_recursos = pd.merge(matriz_recursos, resumen_camiones_rodando[['PlantaUnica', 'CamionesRodando percentile_50', 'CamionesRodando percentile_65', 'CamionesRodando percentile_75']], how = 'left', on = 'PlantaUnica' )
        
        cols = [
            {'name': 'Cluster','id': 'Desc Cluster','type': 'text'},
            {'name': 'Planta','id': 'PlantaUnica','type': 'text'},
            {'name': 'Ventana Horaria','id': 'VentanaHoraria mean','type': 'numeric','format': Format(scheme=Scheme.fixed, precision=2) },
            {'name': 'Tiempo Ciclo','id': 'ciclo_total mean','type': 'numeric','format': Format(scheme=Scheme.fixed, precision=0) },
            {'name': 'Dropsize','id': 'Dropsize mean','type': 'numeric','format': Format(scheme=Scheme.fixed, precision=2) },
            {'name': 'Volumen','id': 'M3Forecast sum','type': 'numeric','format': Format(scheme=Scheme.fixed, precision=0) },
            {'name': 'Camiones Percentil50','id': 'CamionesRodando percentile_50','type': 'numeric' ,'format': Format(scheme=Scheme.decimal_integer)},
            {'name': 'Camiones Percentil65','id': 'CamionesRodando percentile_65','type': 'numeric' ,'format': Format(scheme=Scheme.decimal_integer)},
            {'name': 'Camiones Percentil75','id': 'CamionesRodando percentile_75','type': 'numeric' ,'format': Format(scheme=Scheme.decimal_integer)},
                ]

        data = resumen_camiones_rodando.to_dict('records')    
        
        df_tabla_recursos = resumen_camiones_rodando
        df_matriz_volumen_recursos = matriz_recursos
        """
        vol_recursos = px.bar(
            df_matriz_volumen_recursos,
            x='FechaEntrega',
            y='M3Forecast',
            #color='countriesAndTerritories',
            #labels={'countriesAndTerritories':'Countries', 'dateRep':'date'},
        )
        
        vol_recursos.add_trace(
            px.line(df_matriz_volumen_recursos, x='FechaEntrega', y='CamionesRodando percentile_50')
            )
        
        """
        # Create figure with secondary y-axis
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        # Add traces        
        fig.add_trace(
            go.Bar(x=df_matriz_volumen_recursos['FechaEntrega'], y=df_matriz_volumen_recursos['CamionesRodando'], name="Camiones"),
            secondary_y=True,
        )
        
        fig.add_trace(
            go.Line(x=df_matriz_volumen_recursos['FechaEntrega'], y=df_matriz_volumen_recursos['CamionesRodando percentile_50'], name="50%"),
            secondary_y=True,
        )
        
        fig.add_trace(
            go.Line(x=df_matriz_volumen_recursos['FechaEntrega'], y=df_matriz_volumen_recursos['CamionesRodando percentile_65'], name="65%"),
            secondary_y=True,
        )
        
        fig.add_trace(
            go.Line(x=df_matriz_volumen_recursos['FechaEntrega'], y=df_matriz_volumen_recursos['CamionesRodando percentile_75'], name="75%"),
            secondary_y=True,
        )
        
        # Add figure title
        fig.update_layout(
            title_text="Recursos"
        )
        
        
        
        # Create figure with secondary y-axis
        fig2 = make_subplots(specs=[[{"secondary_y": True}]])
        
        # Add traces
        fig2.add_trace(
            go.Bar (x=df_matriz_volumen_recursos['FechaEntrega'], y=df_matriz_volumen_recursos['M3Forecast'], name="Volumen"),
            secondary_y=False,
        )
        
        # Add figure title
        fig2.update_layout(
            title_text="Forecast"
        )

    
    else :
        cols = []
        data =[]
        fig  = go.Figure()
        fig2  = go.Figure()
        
    return cols, data, fig , fig2
    

@app.callback(
    Output('versiones', 'options'),
    Input('paises', 'value')
    )
def filtrar_parametros_pais(pais_seleccionado):
    #df_cluster_visibles = [{'label': i, 'value': i } for i in clusters[clusters['pais'] == pais_seleccionado]['Desc cluster'] ]
    df_versiones_visibles = [{'label': i, 'value': i } for i in versiones[versiones['pais'] == pais_seleccionado]['version'] ]
    return  df_versiones_visibles

"""
def update_graphs():
    
    line_chart = px.line(
            data_frame=df_matriz_volumen_recursos,
            x='FechaEntrega',
            y='M3Forecast',
            #color='countriesAndTerritories',
            #labels={'countriesAndTerritories':'Countries', 'dateRep':'date'},
            )

"""
#Informacion basica a consultar al inicio de la app
paises = querySQL("SELECT DISTINCT pais FROM SCAC_AT1_NOMBRECLUSTER WHERE ACTIVO = 1" , ())
#clusters = querySQL("SELECT DISTINCT pais, [Desc cluster] FROM SCAC_AT1_NOMBRECLUSTER WHERE ACTIVO = 1" , ())
versiones = querySQL("select distinct pais, version from SCAC_AV7_DesagregacionPronosticoCiudadPlantaDiaTabla where FechaEntrega >= getdate() - 30" , ())


#Seccion de filtros
controls = dbc.Row([
    dbc.Col(
        dbc.Card(        
            dbc.FormGroup([
                dbc.Label("País"),
                dcc.Dropdown(
                    id="paises",
                    options=[{"label": col, "value": col} for col in paises.pais],
                    value="Colombia",
                    style={
                    'display': 'block',
                    'margin-left': '7px'
                    }
                ),
            ]),
            body=True,
        ),
        md = 6
    ),

    dbc.Col(
        dbc.Card(        
            dbc.FormGroup([
                dbc.Label("Version Forecast"),
                dcc.Dropdown(
                    id="versiones",
                    value="",
                ),
            ]),
            body=True,            
        ),
        md = 6
    )     
])

# App layout
app.layout = dbc.Container([

    html.H1("Footprint", style={'text-align': 'center'}),
    html.Hr(),
    dbc.Row(
        [dbc.Col(controls, md=12),],
        align="center",
    ),
     dbc.Row([
        dbc.Col(
            dbc.Button("Calcular", id='gen_table', color="primary", block=True),
            md = 12  
        )],
         align="center",
     ),
    html.Br(),
    dbc.Row(
         [
             dbc.Col(
                 dtable.DataTable(
                     id="tabla_recursos",
                    style_as_list_view=True,
                    style_cell={'padding': '5px'},
                    style_header={
                        'backgroundColor': 'white',
                        'fontWeight': 'bold'
                    },
                    style_cell_conditional=[
                        {
                            'if': {'column_id': c},
                            'textAlign': 'left'
                        } for c in ['Desc Cluster', 'PlantaUnica']
                    ],
                     ), 
                 md=6),
         ],
         align="center",
     ),
    
    html.Br(),
    
    dbc.Row(
     [
         dbc.Col(
              dcc.Graph(id='camiones')
             , 
             md=12),
     ],
     align="center",
     ),
    
    html.Br(),
    
    dbc.Row(
     [
         dbc.Col(
              dcc.Graph(id='volumen')
             , 
             md=12),
     ],
     align="center",
     )

], fluid=True)

# ------------------------------------------------------------------------------
if __name__ == '__main__':
    app.run_server(debug=True)
    
    
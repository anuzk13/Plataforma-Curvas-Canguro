"""
Este módulo procesa las tablas intermedias creadas por procesar_tablas_intermedias junto
con los archivos de curvas de Fenton y WHO para crear tablas de los pacientes anotadas según
las antropometrías.
"""

from functools import reduce
import argparse
import pandas as pd

# Dictionary to store removed IDs
nan_info = {}

def remove_nan(data_frame, df_name):
    """
    Verifica que cada df no tenga columnas vacias y si las tiene elimina los datos 
    Registra los ids eliminados en un objeto global
    """
    global nan_info
    if df_name not in nan_info:
        nan_info[df_name] = {}
    for col in data_frame.columns:
        nan_indices = data_frame[data_frame[col].isna()].index
        if not nan_indices.empty:
            nan_info[df_name][col] = nan_indices.tolist()
            data_frame = data_frame.dropna(subset=[col])
    return data_frame

def create_report():
    """
    Crea un reporte de las variables que se eliminaron
    """
    global nan_info
    with open("reporte.txt", "w", encoding='utf-8') as file:
        for table_name, columns in nan_info.items():
            for column_name, indices in columns.items():
                file.write(f"Para la tabla '{table_name}', en la columna '{column_name}', "
                  f"se eliminaron las siguientes filas debido a valores NaN: {len(indices)}\n")

def leer_datos_curvas(dir_datos_crecimiento):
    """
    Lee los datos de creicimiento de fenton y who guardados en dir_datos_creimiento
    Los archivos deben tener la siguiente esctructura
    curvas_(desviaciones/percentiles)_(who/fenton)/
        (z_scores/percentiles)_(pc/talla/peso)_(ninos/ninas)_(who/fenton).csv
    """

    folder = dir_datos_crecimiento + "/curvas"

    growth_vars = [ "pc","talla","peso"]

    z_scores = {
        "fenton" : {
            "ninos": {},
            "ninas":{}
        },
        "who" : {
            "ninos": {},
            "ninas":{}
        }
    }

    percentiles = {
        "fenton" : {
            "ninos": {},
            "ninas":{}
        }
    }

    for var in growth_vars:
        for sex in ["ninos", "ninas"]:
            for curve in ["fenton", "who"]:
                name = f"{folder}_desviaciones_{curve}/z_scores_{var}_{sex}_{curve}.csv"
                z_scores[curve][sex][var] = pd.read_pickle(name)
                name = f"{folder}_percentiles_fenton/percentiles_{var}_{sex}_fenton.csv"
                percentiles["fenton"][sex][var] = pd.read_pickle(name)
    return z_scores, percentiles

def leer_tablas_intermedias(dir_tablas_intermedias):
    """
    Lee los datos de Karen y otras fuentes pre-procesados por 
    procesar_tablas_intermedias. Este script genera dos tablas:
    - pacientes.pkl (Con los datos de Nathalie de destete se llama pacientes_alim_ox.pkl)
        - Iden_Sexo, HD_TotalDiasHospital, Iden_Sede, Iden_Codigo, edaddestete,
        oxigenoalaentrada, pesodesteteoxigeno, algoLM3meses, algoLM6meses,
        algoLM40sem, LME40, LME3m, LME6m
    - antropometrias_nacimiento_evoluciones.pkl : Las antropometrias desde el nacimiento
        - AC_Talla, AC_Peso, AC_PC, AC_Num, AC_EG_Dias
    """
    antropometrias = pd.read_pickle(dir_tablas_intermedias
                                    + "antropometrias_nacimiento_evoluciones.pkl")
    pacientes = pd.read_pickle(dir_tablas_intermedias + "pacientes_alim_ox.pkl")

    return antropometrias, pacientes

def validar_antropometrias_pacientes(antropometrias, pacientes):
    """
    Filtra las antropometrias y los pacientes segun los filtros:
    - sexo de paciente no es 3
    - edad de antropometria (corregida) es mayor a 171 
    - no tiene valores nulos en la antropometria
    """

    sexo_valido = pacientes['Iden_Sexo'] != 3

    filtros_pac = [sexo_valido] 
    nombres_filtros_pac = ["Sexo no es 3"]

    pacientes_filtados = pacientes[reduce(lambda x, y: x & y, filtros_pac)]

    tiene_paciente = antropometrias['Paciente_ID'].isin(pacientes_filtados['Paciente_ID'])
    edad_minima_valida = antropometrias['AC_EG_Dias'] >= 171
    nacimiento_no_nulo = pd.notnull(antropometrias['AC_EG_Dias'])
    peso_no_nulo = pd.notnull(antropometrias['AC_Peso'])
    pc_no_nulo = pd.notnull(antropometrias['AC_PC'])
    talla_no_nulo = pd.notnull(antropometrias['AC_Talla'])

    filtros_ant = [ tiene_paciente, edad_minima_valida, nacimiento_no_nulo,
                    peso_no_nulo , pc_no_nulo , talla_no_nulo]
    nombres_filtros_ant = ['no tiene datos paciente', 'edad < 171' ,
                            'nacimiento nulo', 'peso nulo' , 'pc nulo' , 'talla nula']
    antropometrias_filtradas = antropometrias[reduce(lambda x, y: x & y, filtros_ant)]

    with open("reporte_antropometrias.txt", "w", encoding='utf-8') as file:
        for filtro, nombre in zip(filtros_pac, nombres_filtros_pac):
            eliminados = pacientes[~filtro]
            file.write(f"Filas pacientes eliminadas por {nombre}: {eliminados.size}\n")
            for paciente in eliminados['Paciente_ID'].tolist():
                file.write(f"paciente con id {paciente} eliminado \n")
        for filtro, nombre in zip(filtros_ant, nombres_filtros_ant):
            eliminados = antropometrias[~filtro]
            file.write(f"Filas antropometrias eliminadas por {nombre}: {eliminados.size}\n")
            for ant in eliminados[['Paciente_ID', 'AC_Num']].to_dict('records'):
                file.write(f"ant {ant['AC_Num']} de paciente {ant['Paciente_ID']} eliminada\n")

    return antropometrias_filtradas, pacientes_filtados

def procesar_tablas_visualizacion(dir_tablas_intermedias, dir_datos_crecimiento):
    """
    Crea las tablas para la visualizacion de los datos
    """
    z_scores, percentiles = leer_datos_curvas(dir_datos_crecimiento)
    antropometrias, pacientes = leer_tablas_intermedias(dir_tablas_intermedias)
    antropometrias, pacientes = validar_antropometrias_pacientes(antropometrias, pacientes)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("directorio_tablas_intermedias",
                        help="Directorio de las tablas creadas por procesar_tablas_intermedias")
    parser.add_argument("directorio_datos_crecimiento",
                        help="Directorio con archivos pkl de crecimiento de pacientes")
    args = parser.parse_args()
    procesar_tablas_visualizacion(args.directorio_tablas_intermedias,
                                  args.directorio_datos_crecimiento)

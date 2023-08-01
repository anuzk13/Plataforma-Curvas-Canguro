"""
Este módulo procesa los datos de Karen de un archivo JSON en dos tablas intermedias:
Una tabla de pacientes, con datos de sexo y hospitalizacion y una tabla de antropometrias

La función principal, `procesar_tablas_intermedias`, vincula todos los pasos de procesamiento.
"""

import argparse
import pandas as pd

def obtener_fecha(data_frame, col_name):
    """
    Convierte Unix (Epoch) UTC timestamps en ms a pandas datetime.

    data_frame (pandas.DataFrame): Dataframe con columna con el formato: 
      [{$date: {'$numberLong': '`<Unix (Epoch) UTC ms>`'}}] 
    col_name (str): Nombre de la columna de df con fechas

    datecol (pandas.Series): Serie de pandas con las fechas en formato pandas datetime.

    >>> df = pd.DataFrame({'fecha': [{'$date': {'$numberLong': '1613148710000'}}]})
    >>> obtener_fecha(df, 'fecha')
    0   2021-02-12 16:51:50
    Name: $numberLong, dtype: datetime64[ns]
    """
    date_col = data_frame[col_name].apply(object_to_series)['$date'].apply(object_to_series)
    date_col = pd.to_datetime(date_col['$numberLong'], unit='ms', errors = 'coerce')
    return date_col

def object_to_series(obj):
    """
    Metodo para convertir un objeto en una serie para que cada atributo sea una columna
    en un df
    """
    return pd.Series(obj, dtype='object')

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


def procesar_pacientes(archivo_pacientes):
    """
    Procesa el objeto principal que contiene la información por cada paciente y retorna un df
    con los objetos hijos requeridos para el análisis de las curvas
    - 'Pediatria'
    - 'Antropometria'
    - 'ExamenRecienNacido'
    - 'Identificacion'
    - 'HospitalizacionDiagnostico'
    """
    pacientes = pd.read_json(archivo_pacientes)
    pacientes = pd.concat([pacientes.drop(['_id'], axis=1), pacientes['_id']
                                    .apply(object_to_series)
                          ], axis=1).rename(columns={"$oid": "id"}).set_index('id')
    return pacientes[['Pediatria',
                      'Antropometria',
                      'ExamenRecienNacido',
                      'Identificacion',
                      'HospitalizacionDiagnostico']]

def procesar_examen_recien_nacido(pacientes):
    """
    Procesa el objeto 'ExamenRecienNacido' que contiene 
      - ERN_Talla: talla del paciente en cm, medido al nacer,
      - ERN_PC: peso del paciente en gramos, medido al nacer,
    """
    examen_rn = pacientes['ExamenRecienNacido'].apply(object_to_series)
    examen_rn = examen_rn[['ERN_Talla','ERN_PC']]
    examen_rn = remove_nan(examen_rn, 'ExamenRecienNacido')
    return examen_rn

def procesar_hosp_diagnostico(pacientes):
    """
    Procesa el objeto 'HospitalizacionDiagnostico' que contiene 
      - HD_TotalDiasHospital: dias que estuvo el niño hospitalizado
    """
    hospitalizacion_diag = pacientes['HospitalizacionDiagnostico'].apply(object_to_series)
    hospitalizacion_diag = hospitalizacion_diag[['HD_TotalDiasHospital']]
    hospitalizacion_diag = remove_nan(hospitalizacion_diag, 'HospitalizacionDiagnostico')
    return hospitalizacion_diag


def procesar_antropometrias(pacientes):
    """
    Procesa el objeto 'Antropometria' que contiene 
      - V_id: número de la antropometría por paciente 
      - AN_timestamp: fecha en la que se hizo la antropometría
      - AN_Talla: talla del paciente en cm, medido en la antropometría
      - AN_Peso: peso del paciente en gramos, medido en la antropometría,
      -	AN_PC: perímetro cefálico del paciente en cm, medido en la antropometría,
    """
    antropometrias = pacientes['Antropometria'].explode().apply(object_to_series)
    antropometrias['AN_timestamp'] = obtener_fecha(antropometrias, 'AN_timestamp')
    antropometrias = antropometrias[['V_id', 'AN_timestamp', 'AN_Talla', 'AN_Peso', 'AN_PC']]
    antropometrias = remove_nan(antropometrias, 'Antropometria')
    return antropometrias

def procesar_e_gest_al_nacer(pacientes):
    """
    Procesa el objeto 'EIP_EdadGestacionalAlNacer' que contiene 
    - EIP_EG_DiasTotales: Edad gestacional en dias
    - EIP_EG_Selecciono: Motivo de seleccion de la edad gestacional
    """
    pediatria = pacientes['Pediatria'].apply(object_to_series)
    e_inicial_pediatria = pediatria['ExamenInicialPediatria'].apply(object_to_series)
    e_gest_nacer = e_inicial_pediatria['EIP_EdadGestacionalAlNacer'].apply(object_to_series)
    e_gest_nacer = e_gest_nacer.drop([0], axis=1)
    e_gest_nacer = e_gest_nacer[['EIP_EG_DiasTotales', 'EIP_EG_Selecciono']]
    e_gest_nacer = remove_nan(e_gest_nacer, 'EIP_EG')
    e_gest_nacer['EIP_EG_DiasTotales'] = e_gest_nacer['EIP_EG_DiasTotales'].astype('int')
    return e_gest_nacer

def procesar_identidad(pacientes):
    """
    Procesa el objeto 'Identidad' que contiene 
      - Iden_Sexo: sexo del paciente 1 (niño) u 2 (niña)
      - Iden_FechaParto: fecha en la que nació el paciente
      - Iden_PesoParto: eso del paciente en gramos, medido al nacer,
    """
    iden = pacientes['Identificacion'].apply(pd.Series)
    iden['Iden_FechaParto'] = obtener_fecha(iden, 'Iden_FechaParto')
    iden = iden[['Iden_FechaParto','Iden_PesoParto','Iden_Sexo', 'Iden_Sede']]
    iden = remove_nan(iden, 'Identificacion')
    return iden

def procesar_iden_codigo(archivo_codigo):
    """
    Procesa el objeto iden_codigo que se encuentra en un archivo aparte
    - Iden_Codigo: Codigo de paciente, se puede repetir por sede
    - Iden_Sede: Sede en la que se registro el paciente
    """
    pacientes_id = pd.read_json(archivo_codigo)
    pacientes_id = pd.concat([pacientes_id.drop(['_id'], axis=1), pacientes_id['_id']
                                    .apply(object_to_series)
                          ], axis=1).rename(columns={"$oid": "id"}).set_index('id')
    pacientes_id = pacientes_id['Identificacion'].apply(object_to_series)
    pacientes_id = remove_nan(pacientes_id, 'Identificacion')
    return pacientes_id

def procesar_tabla_antropometrias_curvas(antropometrias, identidad, e_gest_nacer, examen_rn):
    """
    Crea una tabla con las antropometrias de los pacientes para talla, peso y pc con las columnas
      - AN_Talla: Talla del paciente en cm
      - AC_Peso: Peso del paciente en gramos
      - AN_PC: Perimetro Cefalico del paciente en cm
      - AC_Num: Consecutivo de las antropometrias tomadas desde el nacimiento
                La antropometria al nacer es la numero 0
      - Edad_Corregida_AT_Dias: Edad del paciente en dias corregida cuando se le tomo la 
        antropometria se calcula sumando los dias entre la fecha de nacimiento y la fecha
        de la antropometria mas la edad gestacional al nacer
    """
    datos_antropometria = (antropometrias
                          .join(identidad[['Iden_FechaParto']], how='left')
                          .join(e_gest_nacer[['EIP_EG_DiasTotales']], how='left'))
    edad_dias_ant = ((datos_antropometria['AN_timestamp']- datos_antropometria['Iden_FechaParto'])
                               .dt.days
                              + datos_antropometria['EIP_EG_DiasTotales'])
    datos_antropometria['Edad_Corregida_AT_Dias'] = edad_dias_ant
    datos_antropometria_nacimiento = (identidad[['Iden_PesoParto']]
                                    .join(e_gest_nacer[['EIP_EG_DiasTotales']], how='left')
                                    .join(examen_rn[['ERN_Talla', 'ERN_PC']], how='left'))
    datos_antropometria = datos_antropometria[['V_id',
                                                'AN_Talla',
                                                'AN_Peso',
                                                'AN_PC',
                                                'Edad_Corregida_AT_Dias']]
    # En vez de usar antropometria AN que viene de la base de datos se usa AC antropometria Curvas
    datos_antropometria.rename(columns = {'AN_Talla' : 'AC_Talla',
                                                  'AN_Peso': 'AC_Peso',
                                                  'AN_PC': 'AC_PC',
                                                  'V_id':'AC_Num',
                                                  'Edad_Corregida_AT_Dias': 'AC_EG_Dias'}, 
                                      inplace = True)
    datos_antropometria_nacimiento.rename(columns = {'EIP_EG_DiasTotales':'AC_EG_Dias',
                                                      'Iden_PesoParto': 'AC_Peso',
                                                      'ERN_Talla': 'AC_Talla',
                                                      'ERN_PC': 'AC_PC'},
                                          inplace = True)
    datos_antropometria_nacimiento['AC_Num'] = 0
    datos_curvas = pd.concat([datos_antropometria, datos_antropometria_nacimiento])
    return datos_curvas

def procesar_tabla_pacientes(identidad, iden_codigo, hosp_diagnostico):
    """
    Crea una tabla con los pacientes sus datos de sexo y hospitalización
      - Iden_Sexo: 1 niño, 2 niña, 3 no definido
      - HD_TotalDiasHospital: Numero de dias que el bebe estuvo hospitalizado antes
      de ingresar al ambulatorio
    """
    datos_pacientes = identidad.loc[:,['Iden_Sexo']]
    datos_pacientes = datos_pacientes.join(hosp_diagnostico[['HD_TotalDiasHospital']], how='left')
    datos_pacientes = datos_pacientes.join(iden_codigo, how='left')
    return datos_pacientes

def procesar_destete_alimentacion(archivo_destete_alimentacion, tabla_pacientes):
    """
    Crea una tabla que une los datos de la tabla de pacientes con tabla de información 
    de destete de oxigeno y de alimentación 
      - Iden_Sexo: 1 niño, 2 niña, 3 no definido
      - HD_TotalDiasHospital: Numero de dias que el bebe estuvo hospitalizado antes
      - Iden_Codigo: Codigo de paciente, se puede repetir por sede
      - Iden_Sede: Sede en la que se registro el paciente
      - Variables calculadas en script externo: 'edaddestete', 'oxigenoalaentrada', 
        'pesodesteteoxigeno','algoLM3meses','algoLM6meses','algoLM40sem','LME40',
        'LME3m','LME6m'
    """
    destete_alimentacion = pd.read_excel(archivo_destete_alimentacion)
    destete_alimentacion = destete_alimentacion.replace('#NULL!', None)
    destete_alimentacion['Iden_Codigo'] = destete_alimentacion['Iden_Codigo'].astype(int)
    destete_alimentacion['Iden_Sede'] = destete_alimentacion['Iden_Sede'].astype(int)

    tabla_pacientes['Iden_Codigo'] = tabla_pacientes['Iden_Codigo'].astype(int)
    tabla_pacientes['Iden_Sede'] = tabla_pacientes['Iden_Sede'].astype(int)

    tabla_pacientes_alim_ox = tabla_pacientes.reset_index().merge(destete_alimentacion,
                                                  left_on=['Iden_Codigo', 'Iden_Sede'],
                                                  right_on=['Iden_Codigo', 'Iden_Sede'],
                                                  how='left')
    tabla_pacientes_alim_ox = tabla_pacientes_alim_ox[['Iden_Sexo', 'HD_TotalDiasHospital',
                                                       'Iden_Sede', 'Iden_Codigo',
                                                       'edaddestete', 'oxigenoalaentrada',
                                                       'pesodesteteoxigeno', 'algoLM3meses',
                                                       'algoLM6meses','algoLM40sem','LME40',
                                                       'LME3m','LME6m']]
    return tabla_pacientes_alim_ox

def procesar_tablas_intermedias(archivo_pacientes, archivo_codigo, archivo_destete_alim):
    """
    Crea las tablas intermedias para el analisis de los datos
    """
    pacientes = procesar_pacientes(archivo_pacientes)
    examen_rn = procesar_examen_recien_nacido(pacientes)
    hosp_diagnostico = procesar_hosp_diagnostico(pacientes)
    antropometrias = procesar_antropometrias(pacientes)
    identidad = procesar_identidad(pacientes)
    iden_codigo = procesar_iden_codigo(archivo_codigo)
    e_gest_nacer = procesar_e_gest_al_nacer(pacientes)
    tabla_ant_curvas = procesar_tabla_antropometrias_curvas(antropometrias,
                                                                    identidad,
                                                                    e_gest_nacer,
                                                                    examen_rn)
    tabla_ant_curvas.to_pickle("antropometrias_nacimiento_evoluciones.pkl")
    tabla_pacientes = procesar_tabla_pacientes(identidad,
                                                iden_codigo,
                                                hosp_diagnostico)
    tabla_pacientes.to_pickle("pacientes.pkl")
    tabla_pacientes_alim_ox = procesar_destete_alimentacion(archivo_destete_alim, tabla_pacientes)
    tabla_pacientes_alim_ox.to_pickle("pacientes_alim_ox.pkl")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("datos_pacientes", help="Archivo JSON de pacientes")
    parser.add_argument("datos_codigo_iden",
                        help="Archivo JSON con el codigo de identidad del paciente")
    parser.add_argument("datos_destete_alim",
    help="Archivo Excel con Iden_Codigo, Iden_Sede y variables de destete oxigeno y alimentación")
    args = parser.parse_args()
    procesar_tablas_intermedias(args.datos_pacientes,
                                args.datos_codigo_iden,
                                args.datos_destete_alim)

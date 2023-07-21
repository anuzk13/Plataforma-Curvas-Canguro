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
    datecol = data_frame[col_name].apply(pd.Series)['$date'].apply(pd.Series)
    datecol = pd.to_datetime(datecol['$numberLong'], unit='ms', errors = 'coerce')
    return datecol


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
                  .apply(pd.Series)], axis=1).rename(columns={"$oid": "id"}).set_index('id')
    return pacientes[['Pediatria',
                      'Antropometria',
                      'ExamenRecienNacido',
                      'Parto',
                      'HospitalizacionDiagnostico']]

def procesar_examen_recien_nacido(pacientes):
    """
    Procesa el objeto 'ExamenRecienNacido' que contiene 
      - ERN_Talla: talla del paciente en cm, medido al nacer,
      - ERN_PC: peso del paciente en gramos, medido al nacer,
    """
    examen_rn = pacientes['ExamenRecienNacido'].apply(pd.Series)
    return examen_rn[['ERN_Talla','ERN_PC']]

def procesar_hosp_diagnostico(pacientes):
    """
    Procesa el objeto 'HospitalizacionDiagnostico' que contiene 
      - HD_TotalDiasHospital: dias que estuvo el niño hospitalizado
    """
    hospitalizacion_diag = pacientes['HospitalizacionDiagnostico'].apply(pd.Series)
    return hospitalizacion_diag[['HD_TotalDiasHospital']]


def procesar_antropometrias(pacientes):
    """
    Procesa el objeto 'Antropometria' que contiene 
      - V_id: número de la antropometría por paciente 
      - AN_timestamp: fecha en la que se hizo la antropometría
      - AN_Talla: talla del paciente en cm, medido en la antropometría
      - AN_Peso: peso del paciente en gramos, medido en la antropometría,
      -	AN_PC: perímetro cefálico del paciente en cm, medido en la antropometría,
    """
    antropometrias = pacientes['Antropometria'].explode().apply(pd.Series)
    antropometrias['AN_timestamp'] = obtener_fecha(antropometrias, 'AN_timestamp')
    return antropometrias[['V_id', 'AN_timestamp', 'AN_Talla', 'AN_Peso', 'AN_PC']]

def procesar_e_gest_al_nacer(pacientes):
    """
    Procesa el objeto 'EIP_EdadGestacionalAlNacer' que contiene 
    - EIP_EG_DiasTotales: Edad gestacional en dias
    - EIP_EG_Selecciono: Motivo de seleccion de la edad gestacional
    """
    pediatria = pacientes['Pediatria'].apply(pd.Series)
    e_inicial_pediatria = pediatria['ExamenInicialPediatria'].apply(pd.Series)
    e_gest_nacer = e_inicial_pediatria['EIP_EdadGestacionalAlNacer'].apply(pd.Series)
    e_gest_nacer = e_gest_nacer.drop([0], axis=1)
    return e_gest_nacer[['EIP_EG_DiasTotales', 'EIP_EG_Selecciono']]

def procesar_identidad(pacientes):
    """
    Procesa el objeto 'Identidad' que contiene 
      - Iden_Sexo: sexo del paciente 1 (niño) u 2 (niña)
      - Iden_FechaParto: fecha en la que nació el paciente
      - Iden_PesoParto: eso del paciente en gramos, medido al nacer,
    """
    iden = pacientes['Identificacion'].apply(pd.Series)
    return iden[['Iden_FechaParto',
                'Iden_PesoParto',
                'Iden_Sexo']]

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
                          .join(antropometrias[['Iden_FechaParto']], how='left')
                          .join(e_gest_nacer[['EIP_EG_DiasTotales']], how='left'))
    edad_dias_ant = ((datos_antropometria['AN_timestamp']- datos_antropometria['Iden_FechaParto'])
                               .dt.days
                              + datos_antropometria['EIP_EG_DiasTotales'])
    datos_antropometria['Edad_Corregida_AT_Dias'] = edad_dias_ant
    datos_antropometria_nacimiento = (identidad[['Iden_PesoParto']]
                                    .join(e_gest_nacer[['EIP_EG_DiasTotales']], how='left')
                                    .join(examen_rn[['ERN_Talla', 'ERN_PC']], how='left'))
    datos_curvas_antropometrias = datos_antropometria[['V_id',
                                                       'AN_Talla',
                                                       'AN_Peso',
                                                       'AN_PC',
                                                       'Edad_Corregida_AT_Dias']]
    # En vez de usar antropometria AN que viene de la base de datos se usa AC antropometria Curvas
    datos_curvas_antropometrias.rename(columns = {'AN_Talla' : 'AC_Talla',
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
    datos_curvas = pd.concat([datos_curvas_antropometrias, datos_antropometria_nacimiento])
    return datos_curvas

def procesar_tabla_pacientes(identidad, hosp_diagnostico):
    """
    Crea una tabla con los pacientes sus datos de sexo y hospitalización
      - Iden_Sexo: 1 niño, 2 niña, 3 no definido
      - HD_TotalDiasHospital: Numero de dias que el bebe estuvo hospitalizado antes
      de ingresar al ambulatorio
    """
    patient_df = identidad.loc[:,['Iden_Sexo']]
    patient_df = patient_df.join(hosp_diagnostico[['HD_TotalDiasHospital']], how='left')
    return patient_df

def procesar_tablas_intermedias(archivo_pacientes):
    """
    Crea las tablas intermedias para el analisis de los datos
    """
    pacientes = procesar_pacientes(archivo_pacientes)
    examen_rn = procesar_examen_recien_nacido(pacientes)
    hosp_diagnostico = procesar_hosp_diagnostico(pacientes)
    antropometrias = procesar_antropometrias(pacientes)
    identidad = procesar_identidad(pacientes)
    e_gest_nacer = procesar_e_gest_al_nacer(pacientes)
    tabla_ant_curvas = procesar_tabla_antropometrias_curvas(antropometrias,
                                                                 identidad,
                                                                 e_gest_nacer,
                                                                 examen_rn)
    tabla_ant_curvas.to_pickle("antropometrias_nacimiento_evoluciones.pkl")
    tabla_pacientes = procesar_tabla_pacientes(identidad,
                                               hosp_diagnostico)
    tabla_pacientes.to_pickle("antropometrias_pacientes.pkl")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("datos_pacientes", help="Archivo JSON de pacientes")
    args = parser.parse_args()
    procesar_tablas_intermedias(args.file_name)

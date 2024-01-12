"""
Este módulo filtra las antrpometrias según criterios de pacientes o de las
antropometrías.
"""

import pandas as pd
from utils import Z_SCORE_COLS


class FiltrosPacientes:
    """
    Esta clase representa los filtros aplicados a los datos de pacientes.

    Parámetros:
    - sexo_masculino: Booleano, filtro para sexo masculino (True) o femenino (False).
    - rango_edad_nacimiento: Diccionario que especifica el rango de edad gestacional al nacer.
      'min': Valor mínimo en semanas.
      'max': Valor máximo en semanas.
      'mas_de_40': Booleano, si es True, incluye edades gestacionales mayores a 40 semanas.
    - rangos_ant_nacimiento: Diccionario con rangos mínimos y máximos para variables de crecimiento.
      'min_w', 'max_w': Rango de peso (en gramos).
      'min_s', 'max_s': Rango de talla (en centímetros).
      'min_pc', 'max_pc': Rango de perímetro cefálico (en centímetros).
    - variables_rciu: Indica filtros por presencia de Retraso del Crecimiento Intrauterino (RCIU).
      Estructura:
        - 'vars': Lista de variables de crecimiento para las cuales se aplicará el filtro.
        - Cada variable en 'vars': Booleano que indica si se incluirán (True) o excluirán (False)
                                   los pacientes con RCIU en esa variable específica.
      Ejemplo:
        {
          'vars': ['Peso', 'Talla'],
          'Peso': True,
          'Talla': False
        }
      En este ejemplo, se incluirán pacientes con RCIU en 'Peso' y excluirá con RCIU en 'Talla'.
      Se ignorarán los valores de RCIU para PC
    - variables_rceu: Indica filtros por presencia de Retraso del Crecimiento Extrauterino (RCEU)
        Comparte la misma estructura de variables_rciu
    - dias_hospitalizacion: Diccionario con el rango de días de hospitalización.
      'min': Días mínimos.
      'max': Días máximos.
    """
    def __init__(self, sexo_masculino, rango_edad_nacimiento, rangos_ant_nacimiento, variables_rciu,
                 variables_rceu, dias_hospitalizacion):
        self.sexo_masculino = sexo_masculino
        self.rango_edad_nacimiento = rango_edad_nacimiento
        self.rangos_ant_nacimiento = rangos_ant_nacimiento
        self.variables_rciu = variables_rciu
        self.variables_rceu = variables_rceu
        self.dias_hospitalizacion = dias_hospitalizacion

class FiltrosAntropometricos:
    """
    Esta clase representa los filtros aplicados a los datos antropométricos.
    - variables_outliers: Diccionario con valores de outliers a excluir
        'vars' (lista de variables a considerar)
        'rangos': ('outlier_neg', 'outlier_pos')
    - rango_edad_ant: Diccionario del rango de edad gestacional para variables de crecimiento.
      'min': Edad mínima en semanas.
      'max': Edad máxima en semanas.
    - cortes_ant_nacimiento: Diccionario con cortes (fenton) para variables de crecimieinto
        - cada variable (peso, talla, pc) puede estar presente y tener dos valores
            - min: nombre de rango en Z_SCORE_COLS o 'nomin'
            - max: nombre de rango en Z_SCORE_COLS o 'nomax'
    """
    def __init__(self, variables_outliers, rango_edad_ant, cortes_ant_nacimiento):
        self.variables_outliers = variables_outliers
        self.rango_edad_ant = rango_edad_ant
        self.cortes_ant_nacimiento = cortes_ant_nacimiento


def filtrar_por_dias_hospitalizacion(df, dias_hospitalizacion):
    """
    Filtra los datos basándose en el número de días de hospitalización.
    - df: DataFrame que contiene los datos de los pacientes.
    - dias_hospitalizacion: Diccionario con el rango de días de hospitalización.
      'min': Días mínimos.
      'max': Días máximos.
    """
    texto_filtro = (
        f"Días de hospitalización entre {dias_hospitalizacion['min']} y "
        f"{dias_hospitalizacion['max']} días"
    )
    filtro_min = df['HD_TotalDiasHospital'] >= dias_hospitalizacion['min']
    filtro_max = df['HD_TotalDiasHospital'] <= dias_hospitalizacion['max']
    filtro = df[filtro_min & filtro_max].index.to_list()
    return df.index.isin(filtro), texto_filtro


def filtrar_por_valores_nacimiento(df, rangos_ant_nacimiento):
    """
    Filtra los datos basándose en los valores de crecimiento al nacer
    - df: DataFrame que contiene los datos de los pacientes.
    - rangos_ant_nacimiento: Diccionario con rangos mínimos y máximos para variables de crecimiento.
      'min_w', 'max_w': Rango de peso (en gramos).
      'min_s', 'max_s': Rango de talla (en centímetros).
      'min_pc', 'max_pc': Rango de perímetro cefálico (en centímetros).
    """
    texto_filtro = (
        f"Valores de antropometrías al nacer:\n"
        f" Peso min {rangos_ant_nacimiento['min_w']} gr y max {rangos_ant_nacimiento['max_w']} gr\n"
        f" Talla min {rangos_ant_nacimiento['min_s']} cm y max {rangos_ant_nacimiento['max_s']} cm\n"
        f" PC min {rangos_ant_nacimiento['min_pc']} cm y max {rangos_ant_nacimiento['max_pc']} cm"
    )
    filtro_peso = ((df['N_Peso'] < rangos_ant_nacimiento['max_w']) & 
                   (df['N_Peso'] > rangos_ant_nacimiento['min_w']))
    filtro_talla = ((df['N_Talla'] < rangos_ant_nacimiento['max_s']) & 
                    (df['N_Talla'] > rangos_ant_nacimiento['min_s']))
    filtro_pc = ((df['N_PC'] < rangos_ant_nacimiento['max_pc']) &
                  (df['N_PC'] > rangos_ant_nacimiento['min_pc']))
    filtro = df[filtro_peso & filtro_talla & filtro_pc].index.to_list()
    return df.index.isin(filtro), texto_filtro


def filtrar_por_edad_gestacional_nacimiento(df, rango_edad_nacimiento):
    """
    Filtra los datos basándose en la edad gestacional al nacer.
    - df: DataFrame que contiene los datos de los pacientes.
    - rango_edad_nacimiento: Diccionario que especifica el rango de edad gestacional al nacer.
      'min': Valor mínimo en semanas.
      'max': Valor máximo en semanas.
      'mas_de_40': Booleano, si es True, incluye edades gestacionales mayores a 40 semanas.
    """
    min_edad = rango_edad_nacimiento['min']
    max_edad = rango_edad_nacimiento['max']
    texto_filtro = (
        f"Edad gestacional al nacer entre: {min_edad} semanas y "
        f"{'más de 40 semanas' if rango_edad_nacimiento['mas_de_40'] else f'{max_edad} semanas'}"
    )
    filtro_min = df['N_EG_Dias'] >= (min_edad * 7)
    if rango_edad_nacimiento['mas_de_40']:
        filtro_max = pd.Series([True] * len(df), index=df.index)
    else:
        filtro_max = df['N_EG_Dias'] <= (max_edad * 7)
    filtro = df[filtro_min & filtro_max].index.to_list()
    return df.index.isin(filtro), texto_filtro

def filtrar_por_RCIU(df, variables_rciu):
    """
    Filtra los datos exclutendo o incluyendo pacientes según tengan Retraso del 
    Crecimiento Intrauterino (RCIU) en variables de crecimiento
    - df: DataFrame que contiene los datos de los pacientes.
    - variables_rciu: Indica filtros por presencia de Retraso del Crecimiento Intrauterino (RCIU).
      Estructura:
        - 'vars': Lista de variables de crecimiento para las cuales se aplicará el filtro.
        - Cada variable en 'vars': Booleano que indica si se incluirán (True) o excluirán (False)
            los pacientes con RCIU en esa variable
    """
    texto_filtro = 'RCIU:'
    filtro_base = pd.Series([True] * len(df), index=df.index)
    for var in variables_rciu['vars']:
        incluir = 'con' if variables_rciu[var] else 'sin'
        texto_filtro += f' - Incluir pacientes {incluir} RCIU en {var}'
        filtro = df['RCIU_' + var] == variables_rciu[var]
        filtro_base = filtro_base & filtro
    return filtro_base, texto_filtro

def filtrar_por_RCEU(df, variables_rceu):
    """
    Filtra los datos exclutendo o incluyendo pacientes según tengan Retraso del 
    Crecimiento Extrauterino (RCEU) en variables de crecimiento
    - df: DataFrame que contiene los datos de los pacientes.
    - variables_rceu: Indica filtros por presencia de Retraso del Crecimiento Extrauterino (RCEU).
      Estructura:
        - 'vars': Lista de variables de crecimiento para las cuales se aplicará el filtro.
        - Cada variable en 'vars': Booleano que indica si se incluirán (True) o excluirán (False)
            los pacientes con RCEU en esa variable
    """
    texto_filtro =  'RCEU:'
    filtro_base = pd.Series([True] * len(df), index=df.index)
    for var in variables_rceu['vars']:
        incluir = 'con' if variables_rceu[var] else 'sin'
        texto_filtro += f' - Incluir pacientes {incluir} RCEU en {var}'
        filtro = df['RCEU_' + var] == variables_rceu[var]
        filtro_base = filtro_base & filtro
    return filtro_base, texto_filtro

def filtrar_outliers(df, variables_outliers):
    """
    Excluye outliers de un DataFrame basándose en variables específicas.
    - df: DataFrame que contiene los datos a filtrar.
    - variables_outliers: Diccionario con valores de outliers a excluir
        'vars' (lista de variables a considerar)
        'rangos' (rango de valores atípicos a excluir).
    """
    filtro_base = pd.Series([True] * len(df), index=df.index)
    texto_filtro = 'Excluir outliers'

    for var in variables_outliers['vars']:
        col_min = f"{var}_min"
        col_max = f"{var}_max"
        for rango in variables_outliers['rangos']:
            filtro = df[col_max] != 'outlier_neg' if rango == 'outlier_neg' else df[col_min] != 'outlier_pos'
        filtro_base = filtro_base & filtro

    return filtro_base, texto_filtro

def filtrar_ant_edad(df, rango_edad_ant):
    """
    Filtra las antropometrías basándose en un rango de edad especificado.
    - df: DataFrame que contiene los datos de antropometrías.
    - rango_edad_ant: Diccionario del rango de edad gestacional para variables de crecimiento.
      'min': Edad mínima en semanas.
      'max': Edad máxima en semanas.
    """
    texto_filtro = (
        f"Antropometrías entre {rango_edad_ant['min']} semanas "
        f"y {rango_edad_ant['max']} semanas"
    )
    filtro_min = df['AC_EG_Dias'] > (rango_edad_ant['min'] * 7)
    filtro_max = df['AC_EG_Dias'] < (rango_edad_ant['max'] * 7)
    return (filtro_min & filtro_max), texto_filtro

def filtrar_por_desviacion_estandar(df, rango_ant, var_ant):
    """
    Filtra los datos basándose en desviaciones estándar específicas para variables antropométricas.
    - df: DataFrame que contiene los datos.
    - rango_ant: Diccionario con
        - min: nombre de rango en Z_SCORE_COLS o 'nomin'
        - max: nombre de rango en Z_SCORE_COLS o 'nomax'
    - var_ant: Variable de crecimiento
    """
    nombre_var = var_ant.replace('labels_AC_', '')
    texto_filtro = (
        f"Rangos de {nombre_var} al nacer entre {rango_ant['min']}"
        f"y {rango_ant['max']} estándar de fenton"
    )

    etiquetas = Z_SCORE_COLS['fenton']
    # tampoco estoy incluyendo a las que tienen outlier positivo
    indice_min =  etiquetas.index(rango_ant['min']) if rango_ant['min'] in etiquetas else 0
    indice_max = etiquetas.index(rango_ant['max']) if rango_ant['max'] in etiquetas else len(etiquetas) - 1
    # TODO: remove the last one so etiquetas_rango = etiquetas[indice_min:indice_max ]
    etiquetas_rango = etiquetas[indice_min:indice_max + 1]
    # Si no tengo minimo entonces deberia incluir tambien 'None' para aquellas que tienen menos del outlier y tambien outlier_neg en el minimo
    if not rango_ant['min']:
        etiquetas_rango.append([None, 'outlier_neg'])
    # Si no tengo maximo entonces deberia incluir 'outlier_pos' para aquellas que tienen outlier pos
    # TODO: cambiar outlier_neg y outlier_pos a constantes 
    if not rango_ant['max']:
        etiquetas_rango.append(['outlier_pos'])

    # Check if Var_min is in etiquetas rango
    indices_filtro = df[(df['AC_Num'] == 0) & df[var_ant].isin(etiquetas_rango)].index
    return df.index.isin(indices_filtro), texto_filtro

# # Updated filter_patients function
# def filter_patients(df, patient_filters):
#     sex_text = 'Sexo: Niños' if patient_filters.sex_male else 'Sexo: Niñas'
#     filter_text = [sex_text]
#     df = df[df['Iden_Sexo'] == 1] if patient_filters.sex_male else df[df['Iden_Sexo'] == 2]

#     filter_mask = pd.Series([True] * len(df), index=df.index)

#     if patient_filters.brith_age_range:
#         filter, f_text = filter_by_gestational_age_birth(df, patient_filters.brith_age_range)
#         filter_text.append(f_text)
#         filter_mask = filter_mask & filter

#     if patient_filters.brith_ant_ranges:
#         filter, f_text = filter_by_birth_values(df, patient_filters.brith_ant_ranges)
#         filter_text.append(f_text)
#         filter_mask = filter_mask & filter

#     if patient_filters.rciu_vars:
#         filter, f_text = filter_by_RCIU(df, patient_filters.rciu_vars)
#         filter_text.append(f_text)
#         filter_mask = filter_mask & filter

#     if patient_filters.rceu_vars:
#         filter, f_text = filter_by_RCEU(df, patient_filters.rceu_vars)
#         filter_text.append(f_text)
#         filter_mask = filter_mask & filter

#     if patient_filters.hosp_days:
#         filter, f_text = filter_by_hospitalization_days(df, patient_filters.hosp_days)
#         filter_text.append(f_text)
#         filter_mask = filter_mask & filter

#     return df[filter_mask], filter_text

# # Updated filter_ant_measures function
# def filter_ant_measures(df, ant_filters):
#     filter_text = []
#     filter_mask = pd.Series([True] * len(df), index=df.index)

#     if ant_filters.exclude_outliers:
#         filter, f_text = filter_exclude_outliers(df, ant_filters.exclude_outliers)
#         filter_text.append(f_text)
#         filter_mask = filter_mask & filter

#     if ant_filters.ant_age_range:
#         filter, f_text = filter_by_ant_age(df, ant_filters.ant_age_range)
#         filter_text.append(f_text)
#         filter_mask = filter_mask & filter

#     if ant_filters.birth_ant_cuts:
#         for measure in ['peso', 'talla', 'pc']:
#             if measure in ant_filters.birth_ant_cuts and ant_filters.birth_ant_cuts[measure]:
#                 filter, f_text = filter_by_standard_desv(df, ant_filters.birth_ant_cuts[measure], 'labels_AC_' + measure.capitalize())
#                 filter_text.append(f_text)
#                 filter_mask = filter_mask & filter

#     return df[filter_mask], filter_text

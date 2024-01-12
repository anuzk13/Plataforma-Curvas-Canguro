"""
Este módulo procesa las tablas intermedias creadas por procesar_tablas_intermedias junto
con los archivos de curvas de Fenton y WHO para crear tablas de los pacientes anotadas según
las antropometrías.
"""

from functools import reduce
import argparse
import pandas as pd
from utils import Z_SCORE_COLS, COLORES_RANGOS, leer_datos_curvas


def calcular_color_ant(fila_ant, col_ant, cols_z_scores, dicc_color):
    """
    Calcula el color de una fila de antropometria segun los rangos de distribucion de 
    antropometrias de fenton o who, retorna dos valores:
        - el color segun el rango, el percentil inmediatamente menor al valor 
    - fila_ant: fila con antropometria de pacientes y valores de los z_scores
    para esa antropometria
    - col_ant: nombre de la columna del valor de la atropometria
    - cols_z_scores: nombres de las columnas de los valores de z_scores
    - dicc_color: diccionario con los colores para los rangos de antropometrias
    """
    for i, col_z_score in enumerate(cols_z_scores):
        if fila_ant[col_ant] < fila_ant[col_z_score]:
            if i == 0:
                # Si el valor es menor al primer rango es outlier negativo
                return dicc_color['outlier_neg'], None
            llave_color = '_'.join([cols_z_scores[i-1], cols_z_scores[i]])
            return dicc_color[llave_color], cols_z_scores[i-1]
    return dicc_color['outlier_pos'], 'outlier_pos'  # Si el valor no esta en los rangos en outlier positivo

def calcular_color_ant_edad(filas_ant, z_scores_df, var_ant, var_z_scores):
    """
    Calcula el color de un df de z_scores para un grupo de edad con fenton o who
    - filas_ant: df con las antropometrias que corresponden a las edades de los z_scores
    - z_scores_df: df con los valores de los z_scores
    - var_ant: llave de la antropometria a calcular 'AC_Peso', 'AC_Talla',o 'AC_PC'
    - var_z_scores: llave de los z_scores a usar: 'fenton' o 'who'
    """
    # si esta por encima o debajo de 5 de se considera un outlier, esta desviacion es aproximada
    medida_outlier = (z_scores_df['des_1'] - z_scores_df['des_0']) * 5
    z_scores_df.insert(0, 'outlier_neg', z_scores_df['des_0'] - medida_outlier)
    z_scores_df['outlier_pos'] = z_scores_df['des_0'] + medida_outlier
    ant = filas_ant[['AC_EG_Dias', var_ant]].join(z_scores_df.set_index('days'), on='AC_EG_Dias')
    colores = ant.apply(lambda fila: calcular_color_ant(fila,
                                                        var_ant,
                                                        Z_SCORE_COLS[var_z_scores],
                                                        COLORES_RANGOS[var_z_scores]), axis=1)
    return colores

def calcular_color_antropometrias(z_scores, pacientes, antropometrias):
    """
    Calcula el color de un df de z_scores para todas las antropometrias
    - z_scores: objeto con los valores de z_scores para fenton o who para Talla, Peso y PC
    - pacientes: df de pacientes con sexo de pacientes
    - antropometrias: df con antropometria de Peso, Talla, PC, dia y Id de paciente
    """
    variables_curvas_ant = {
        'AC_Peso': 'peso',
        'AC_Talla': 'talla',
        'AC_PC': 'pc'
    }

    # Los percentiles estan en kg
    antropometrias['AC_Peso'] = antropometrias['AC_Peso']/1000

    ant_rangos = {}

    for curve_var, z_score_var in variables_curvas_ant.items():
        ant_rangos[curve_var] = {}
        for sex, sex_id in [('ninas', 2), ('ninos', 1)]:
            sex_patient_ids = pacientes[pacientes['Iden_Sexo'] == sex_id]['Paciente_ID']
            sex_filter = antropometrias['Paciente_ID'].isin(sex_patient_ids)
            ant_rangos[curve_var][sex] = {}
            for datos_percentiles in ['fenton', 'who']:
                if datos_percentiles == 'fenton':
                    age_filter = antropometrias['AC_EG_Dias'] <= 280
                else:
                    age_filter = antropometrias['AC_EG_Dias'] > 280
                filas_ant = (antropometrias[sex_filter & age_filter]
                                            [[curve_var,'AC_EG_Dias']])
                z_scores_df = z_scores[datos_percentiles][sex][z_score_var].copy()
                colores = calcular_color_ant_edad(filas_ant, z_scores_df, curve_var, datos_percentiles)
                ant_rangos[curve_var][sex][datos_percentiles] = colores

    return ant_rangos

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

def interpolar_antropometrias(antropometrias):
    """
    Crea un df donde hay un valor de antropometria por cada dia entre la primera y la 
    ultima antropometria del paciente
    Utiliza una interpolación linear para calcular estos valores
    """
    antropometrias['Semana'] = (antropometrias['AC_EG_Dias'] // 7).astype('int')
    ant_promedio_semanas = antropometrias.groupby(['Paciente_ID',
                                                   'Semana'])[['AC_Peso', 
                                                                'AC_PC', 
                                                                'AC_Talla']].mean()

    def interpolar_antropometrias_paciente(antropometrias_paciente):
        # ID de paciente y semana esstan en el indice
        id_paciente, semana_min  = antropometrias_paciente.index.min()
        _, semana_max = antropometrias_paciente.index.max()

        # Crear un MultiIndex con todas los numeros de semanas de antropometrias
        idx = pd.MultiIndex.from_product([[id_paciente], range(semana_min, semana_max + 1)],
                                         names=['Paciente_ID', 'Semana'])

        # Reindexar segun el multiindex e interpolar
        ant_interpoladas = antropometrias_paciente.reindex(idx).interpolate(method='linear')

        return ant_interpoladas

    # Apply the interpolation function to each group
    df_interpolado = pd.concat(interpolar_antropometrias_paciente(grupo) for _,
                               grupo in ant_promedio_semanas.groupby(level=0))
    df_interpolado = df_interpolado.reset_index()
    df_interpolado['AC_EG_Dias'] = df_interpolado['Semana'] * 7
    df_interpolado = df_interpolado.sort_values(['Paciente_ID', 'AC_EG_Dias'])
    df_interpolado['AC_Num'] = df_interpolado.groupby('Paciente_ID').cumcount()
    return df_interpolado

def validar_antropometrias_pacientes(antropometrias, pacientes):
    """
    Filtra las antropometrias y los pacientes segun los filtros:
    - sexo de paciente no es 3
    - edad de antropometria (corregida) es mayor a 171 
    - no tiene valores nulos en la antropometria
    - no tiene valores no validos en antropometrias 
        (AC_Peso < 500 gr, AC_PC < 15 cm,  AC_Talla < 25 cm)
    - tiene antropometria 0 u 1 (y no tiene valores nulos en esas antropometrias)
    """

    # Filtros generales antropometrias
    edad_minima_valida = antropometrias['AC_EG_Dias'] >= 171
    nacimiento_no_nulo = pd.notnull(antropometrias['AC_EG_Dias'])
    peso_no_nulo = pd.notnull(antropometrias['AC_Peso'])
    pc_no_nulo = pd.notnull(antropometrias['AC_PC'])
    talla_no_nulo = pd.notnull(antropometrias['AC_Talla'])
    peso_valido = antropometrias['AC_Peso'] > 500
    pc_valido = antropometrias['AC_PC'] > 15
    talla_valida = antropometrias['AC_Talla'] > 25

    filtros_ant = [ edad_minima_valida, nacimiento_no_nulo,
                    peso_no_nulo , pc_no_nulo , talla_no_nulo,
                    peso_valido, pc_valido, talla_valida]
    nombres_filtros_ant = ['edad < 171' ,
                            'nacimiento nulo', 'peso nulo' , 'pc nulo' , 'talla nula',
                            'peso < 500 gr', 'pc < 15 cm', 'talla < 25 cm']

    # Filtrar pacientes segun la antropometria de nacimiento
    ant_validas = antropometrias[reduce(lambda x, y: x & y, filtros_ant)]
    ant_nac_pacientes = ant_validas[ant_validas['AC_Num'] == 0]['Paciente_ID']
    ant_nacimiento = pacientes['Paciente_ID'].isin(ant_nac_pacientes)
    primera_ant_pacientes = ant_validas[ant_validas['AC_Num'] == 1]['Paciente_ID']
    primera_ant = pacientes['Paciente_ID'].isin(primera_ant_pacientes)

    # Filtros pacientes
    sexo_valido = pacientes['Iden_Sexo'] != 3
    filtros_pac = [sexo_valido, ant_nacimiento, primera_ant]
    pacientes_filtados = pacientes[reduce(lambda x, y: x & y, filtros_pac)]
    nombres_filtros_pac = ["Sexo no es 3",
                            "No tiene antopometria de nacimiento o esta no tiene valores validos",
                            "No tiene anrtopometria de llegada o esta no tiene valores validos"]

    # Filtrar antropometrias que se quedaron sin paciente
    tiene_paciente = antropometrias['Paciente_ID'].isin(pacientes_filtados['Paciente_ID'])
    filtros_ant += [tiene_paciente]
    nombres_filtros_ant += ['no tiene datos paciente']
    ant_filtradas = antropometrias[reduce(lambda x, y: x & y, filtros_ant)]

    with open("reporte_antropometrias.txt", "w", encoding='utf-8') as file:
        for filtro, nombre in zip(filtros_pac, nombres_filtros_pac):
            eliminados = pacientes[~filtro]
            file.write(f"Filas pacientes eliminadas por {nombre}: {eliminados.shape[0]}\n")
            for paciente in eliminados['Paciente_ID'].tolist():
                file.write(f"paciente con id {paciente} eliminado \n")
        for filtro, nombre in zip(filtros_ant, nombres_filtros_ant):
            eliminados = antropometrias[~filtro]
            file.write(f"Filas antropometrias eliminadas por {nombre}: {eliminados.shape[0]}\n")
            for ant in eliminados[['Paciente_ID', 'AC_Num']].to_dict('records'):
                file.write(f"ant {ant['AC_Num']} de paciente {ant['Paciente_ID']} eliminada\n")

    return ant_filtradas, pacientes_filtados

def crear_bandera_rciu(pacientes, antropometrias, percentiles):
    """
    Crea una bandera para los pacientes que se encuentran por debajo del percentil 10 
    de fenton en la antropometria de nacimiento ('AC_Num' es 0)
    """
    for ant_var, fenton_var in [('Peso', 'peso'), ('Talla', 'talla'), ('PC', 'pc')]:
        nombre_col = "RCIU_" + ant_var
        pacientes[nombre_col] = False
        for sexo, id_sexo in [('ninas', 2), ('ninos', 1)]:
            var = 'AC_' + ant_var
            pacientes_sexo = pacientes[pacientes['Iden_Sexo'] == id_sexo]['Paciente_ID']
            ant = antropometrias[(antropometrias['Paciente_ID'].isin(pacientes_sexo))
                                & (antropometrias['AC_Num'] == 0)][[var,'AC_EG_Dias','Paciente_ID']]
            ant_comp_df = ant.join(percentiles['fenton'][sexo][fenton_var]
                                   .set_index('days')[['10']],
                                   on='AC_EG_Dias')
            pacientes_rciu = (ant_comp_df[ant_comp_df[var] < ant_comp_df['10']])['Paciente_ID']
            pacientes.loc[pacientes['Paciente_ID'].isin(pacientes_rciu), nombre_col] = True
    return pacientes

def crear_bandera_rceu(pacientes, antropometrias, percentiles):
    """
    Crea una bandera para los pacientes que se encuentran por debajo del percentil 10 
    de fenton en la antropometria de llegada a canguro ('AC_Num' es 1)
    """
    for ant_var, fenton_var in [('Peso', 'peso'), ('Talla', 'talla'), ('PC', 'pc')]:
        nombre_col = "RCEU_" + ant_var
        pacientes[nombre_col] = False
        for sexo, id_sexo in [('ninas', 2), ('ninos', 1)]:
            var = 'AC_' + ant_var
            pacientes_sexo = pacientes[pacientes['Iden_Sexo'] == id_sexo]['Paciente_ID']
            ant = antropometrias[(antropometrias['Paciente_ID'].isin(pacientes_sexo))
                                & (antropometrias['AC_Num'] == 1)][[var,'AC_EG_Dias','Paciente_ID']]
            ant_comp_df = ant.join(percentiles['fenton'][sexo][fenton_var]
                                    .set_index('days')[['10']],
                                    on='AC_EG_Dias')
            pacientes_rceu = (ant_comp_df[ant_comp_df[var] < ant_comp_df['10']]).index
            pacientes.loc[pacientes.index.isin(pacientes_rceu), nombre_col] = True
        return pacientes

def combinar_rangos_antropometrias(ant_rangos, ant_interpoladas):
    """
    Concatenar todos los labels en un df
    """
    var_ants = {}
    for curve_var in ['AC_Peso','AC_Talla','AC_PC']:
        ants = []
        for sex in ['ninas','ninos']:
            for datos_percentiles in ['fenton', 'who']:
                # Extrayendo resultados como DataFrame
                resultados = ant_rangos[curve_var][sex][datos_percentiles]
                # Separando los resultados en columnas individuales
                df_resultados = pd.DataFrame(resultados.tolist(), columns=[f"{curve_var}_color", f"{curve_var}_min"], index=resultados.index)
                ants.append(df_resultados)
        var_ants[curve_var] = pd.concat(ants)

    # Concatenar todas las columnas de resultados
    data = pd.concat([var_ants[curve_var] for curve_var in ['AC_Peso', 'AC_Talla', 'AC_PC']], axis=1)
    return ant_interpoladas.join(data)

def procesar_tablas_visualizacion(dir_tablas_intermedias, dir_datos_crecimiento):
    """
    Crea las tablas para la visualizacion de los datos
    """
    z_scores, percentiles = leer_datos_curvas(dir_datos_crecimiento)
    antropometrias, pacientes = leer_tablas_intermedias(dir_tablas_intermedias)
    antropometrias, pacientes = validar_antropometrias_pacientes(antropometrias, pacientes)
    pacientes = crear_bandera_rciu(pacientes, antropometrias, percentiles)
    pacientes = crear_bandera_rceu(pacientes, antropometrias, percentiles)
    ant_interpoladas = interpolar_antropometrias(antropometrias)
    ant_rangos = calcular_color_antropometrias(z_scores, pacientes, ant_interpoladas)
    ant_interpoladas_rangos = combinar_rangos_antropometrias(ant_rangos, ant_interpoladas)
    ant_interpoladas_rangos.to_pickle("ant_interpoladas_rangos.pkl")
    pacientes.to_pickle("pacientes_banderas.pkl")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("directorio_tablas_intermedias",
                        help="Directorio de las tablas creadas por procesar_tablas_intermedias")
    parser.add_argument("directorio_datos_crecimiento",
                        help="Directorio con archivos pkl de crecimiento de pacientes")
    args = parser.parse_args()
    procesar_tablas_visualizacion(args.directorio_tablas_intermedias,
                                  args.directorio_datos_crecimiento)

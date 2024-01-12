"""
Este módulo contiene constantes usadas en los diferentes programas
"""

import pandas as pd

# El orden relativo de estos labels es importante para funciones como filtrado segun desviacion estandar de variable al nacer
Z_SCORE_COLS = {
    'fenton': ['des_3Neg', 'des_2Neg', 'des_1Neg', 'des_0', 'des_1', 'des_2', 'des_3'],
    'who': ['des_2Neg', 'des_1Neg', 'des_0', 'des_1', 'des_2']
}

COLORES_RANGOS = {
    'fenton': {
        'outlier_neg': "#ff7402",
        'outlier_pos': "#ff7402",
        'outlier_neg_des_3Neg': "#bf0036",
        'des_3Neg_des_2Neg': "#8310cc",
        'des_2Neg_des_1Neg': "#0e7dc2",
        'des_1Neg_des_0': "#08c754",
        'des_0_des_1': "#08c754",
        'des_1_des_2': "#0e7dc2",
        'des_2_des_3': "#8310cc",
        'des_3_outlier_pos': "#bf0036"
    },
    'who': {
        'outlier_neg':'#bd8f5d',
        'outlier_pos':'#bd8f5d',
        'des_3Neg_des_2Neg':'#bd5d79',
        'des_2Neg_des_1Neg':'#749eb8',
        'des_1Neg_des_0':'#73bc90',
        'des_0_des_1':'#73bc90',
        'des_1_des_2':'#749eb8',
        'des_2_des_3':'#bd5d79'
    }
}

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
                z_scores[curve][sex][var] = pd.read_csv(name, index_col=0)
                name = f"{folder}_percentiles_fenton/percentiles_{var}_{sex}_fenton.csv"
                percentiles["fenton"][sex][var] = pd.read_csv(name, index_col=0)
    return z_scores, percentiles

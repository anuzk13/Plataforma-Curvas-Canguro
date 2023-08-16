# Plataforma Curvas Canguro

## Proceso de ETL

### **procesar_tablas_intermedias.py**
*procesar_tablas_intermedias.py* transforma los datos obtenidos del sistema Karen en forma de JSON a una serie de tablas intermedias (pandas dataframe) en formato .pkl que se usan para crear las tablas de análisis.

Para ejecutar reemplazar:
`<archivo_pacientes>` con el nombre del archivo de datos de pacientes, 
`<datos_codigo_iden>` con el nombre del archivo de datos de identidad de pacientes,
`<datos_destete_alimentacion>` con el nombre del archivo de datos de destete de oxigeno y alimentación

```
python procesar_tablas_intermedias.py `<archivo_pacientes>`.json `<datos_codigo_iden>`.json `<datos_destete_alimentacion>`.xlsx
```

**input** `<archivo_pacientes>`

Nombre del archivo de Json obtenido del sistema Karen. Para obtener las tablas intermedias se espera que cupla con la siguiente estructura:


**input** `<datos_codigo_iden>`

Nombre de archvio de Json obtenido del sistema Karen que contiene datos complementarios a archivo_pacientes (Iden_Codigo, Iden_Sede)

**input** `<datos_destete_alimentacion>`

Archivo que contiene las variables 'edaddestete', 'oxigenoalaentrada', 'pesodesteteoxigeno', 'algoLM3meses','algoLM6meses','algoLM40sem','LME40','LME3m','LME6m' generadas por un script externo. Cada paciente se identifica con Iden_Codigo e Iden_Sede

## Script the visualización


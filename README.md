# Plataforma Curvas Canguro

## Proceso de ETL

### **procesar_tablas_intermedias.py**
*procesar_tablas_intermedias.py* transforma los datos obtenidos del sistema Karen en forma de JSON a una serie de tablas intermedias (pandas dataframe) en formato .pkl que se usan para crear las tablas de análisis.

Para ejecutar reemplazar `<archivo_pacientes>` con el nombre del archivo de datos de pacientes

```
python procesar_tablas_intermedias.py `<archivo_pacientes>`.json
```

**input** `<archivo_pacientes>`

Nombre del archivo de Json obtenido del sistema Karen. Para obtener las tablas intermedias se espera que cupla con la siguiente estructura:



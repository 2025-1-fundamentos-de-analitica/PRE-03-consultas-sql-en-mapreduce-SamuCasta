"""Taller evaluable"""

# pylint: disable=broad-exception-raised
# pylint: disable=import-error

from homework.mapreduce import run_mapreduce_job # type: ignore


#
# Columns:
# total_bill, tip, sex, smoker, day, time, size
#

#
# SELECT *, tip/total_bill as tip_rate
# FROM tips;
#
def mapper_query_1(sequence):
    """Mapper"""
    # Inicializamos una lista para almacenar los resultados procesados
    result = []
    
    # Iteramos sobre cada fila de la secuencia de entrada
    for index, (_, row) in enumerate(sequence):
        # Si es la primera fila (encabezado), añadimos una nueva columna llamada "tip_rate"
        if index == 0:
            result.append((index, row.strip() + ",tip_rate"))
        else:
            # Dividimos la fila en valores individuales usando la coma como separador
            row_values = row.strip().split(",")
            
            # Obtenemos los valores de total_bill y tip como números flotantes
            total_bill = float(row_values[0])
            tip = float(row_values[1])
            
            # Calculamos la tasa de propina (tip_rate) redondeada a 2 decimales
            tip_rate = round(tip / total_bill, 2)
            
            # Añadimos la fila original junto con el nuevo valor de tip_rate
            result.append((index, row.strip() + "," + str(tip_rate)))
    
    # Retornamos la lista de resultados procesados
    return result


def reducer_query_1(sequence):
    """Reducer"""
    # Este reducer no realiza ninguna operación adicional sobre los datos.
    # Simplemente retorna la secuencia tal como fue procesada por el mapper.
    # En este caso, el mapper ya calculó y añadió la columna "tip_rate",
    # por lo que no es necesario realizar más transformaciones.
    return sequence


#
# SELECT *
# FROM tips
# WHERE time = 'Dinner';
#
def mapper_query_2(sequence):
    """Mapper"""
    # Inicializamos una lista para almacenar los resultados procesados
    result = []
    
    # Iteramos sobre cada fila de la secuencia de entrada
    for index, (_, row) in enumerate(sequence):
        # Si es la primera fila (encabezado), la añadimos directamente al resultado
        if index == 0:
            result.append((index, row.strip()))
        else:
            # Dividimos la fila en valores individuales usando la coma como separador
            row_values = row.strip().split(",")
            
            # Verificamos si el valor de la columna "time" (índice 5) es igual a "Dinner"
            if row_values[5] == "Dinner":
                # Si la condición se cumple, añadimos la fila al resultado
                result.append((index, row.strip()))
    
    # Retornamos la lista de resultados procesados
    return result


def reducer_query_2(sequence):
    """Reducer"""
    # Este reducer no realiza ninguna operación adicional sobre los datos.
    # Simplemente retorna la secuencia tal como fue procesada por el mapper.
    # En este caso, el mapper ya filtró las filas donde la columna "time" es igual a "Dinner",
    # por lo que no es necesario realizar más transformaciones.
    return sequence



#
# SELECT *
# FROM tips
# WHERE time = 'Dinner' AND tip > 5.00;
#
def mapper_query_3(sequence):
    """Mapper"""
    # Inicializamos una lista para almacenar los resultados procesados
    result = []
    
    # Iteramos sobre cada fila de la secuencia de entrada
    for index, (_, row) in enumerate(sequence):
        # Si es la primera fila (encabezado), la añadimos directamente al resultado
        if index == 0:
            result.append((index, row.strip()))
        else:
            # Dividimos la fila en valores individuales usando la coma como separador
            row_values = row.strip().split(",")
            
            # Verificamos si el valor de la columna "time" (índice 5) es igual a "Dinner"
            # y si el valor de la columna "tip" (índice 1) es mayor a 5.00
            if row_values[5] == "Dinner" and float(row_values[1]) > 5.00:
                # Si ambas condiciones se cumplen, añadimos la fila al resultado
                result.append((index, row.strip()))
    
    # Retornamos la lista de resultados procesados
    return result


def reducer_query_3(sequence):
    """Reducer"""
    # Este reducer no realiza ninguna operación adicional sobre los datos.
    # Simplemente retorna la secuencia tal como fue procesada por el mapper.
    # En este caso, el mapper ya filtró las filas donde la columna "time" es igual a "Dinner"
    # y el valor de la columna "tip" es mayor a 5.00, por lo que no es necesario realizar más transformaciones.
    return sequence


#
# SELECT *
# FROM tips
# WHERE size >= 5 OR total_bill > 45;
#
def mapper_query_4(sequence):
    """Mapper"""
    # Inicializamos una lista para almacenar los resultados procesados
    result = []
    
    # Iteramos sobre cada fila de la secuencia de entrada
    for index, (_, row) in enumerate(sequence):
        # Si es la primera fila (encabezado), la añadimos directamente al resultado
        if index == 0:
            result.append((index, row.strip()))
        else:
            # Dividimos la fila en valores individuales usando la coma como separador
            row_values = row.strip().split(",")
            
            # Verificamos si el valor de la columna "size" (índice 6) es mayor o igual a 5
            # o si el valor de la columna "total_bill" (índice 0) es mayor a 45
            if int(row_values[6]) >= 5 or float(row_values[0]) > 45:
                # Si alguna de las condiciones se cumple, añadimos la fila al resultado
                result.append((index, row.strip()))
    
    # Retornamos la lista de resultados procesados
    return result


def reducer_query_4(sequence):
    """Reducer"""
    # Este reducer no realiza ninguna operación adicional sobre los datos.
    # Simplemente retorna la secuencia tal como fue procesada por el mapper.
    # En este caso, el mapper ya filtró las filas donde la columna "size" es mayor o igual a 5
    # o el valor de la columna "total_bill" es mayor a 45, por lo que no es necesario realizar más transformaciones.
    return sequence


#
# SELECT sex, count(*)
# FROM tips
# GROUP BY sex;
#
def mapper_query_5(sequence):
    """Mapper"""
    # Inicializamos una lista para almacenar los resultados procesados
    result = []
    
    # Iteramos sobre cada fila de la secuencia de entrada
    for index, (_, row) in enumerate(sequence):
        # Si es la primera fila (encabezado), la omitimos ya que no contiene datos relevantes
        if index == 0:
            continue
        
        # Dividimos la fila en valores individuales usando la coma como separador
        row_values = row.strip().split(",")
        
        # Añadimos una tupla al resultado donde el primer elemento es el valor de la columna "sex" (índice 2)
        # y el segundo elemento es el número 1, representando una ocurrencia de ese valor
        result.append((row_values[2], 1))
    
    # Retornamos la lista de resultados procesados
    return result


def reducer_query_5(sequence):
    """Reducer"""
    # Inicializamos un diccionario para contar las ocurrencias de cada valor de la columna "sex"
    counter = dict()
    
    # Iteramos sobre cada par clave-valor en la secuencia de entrada
    for key, value in sequence:
        # Si la clave (valor de "sex") no está en el diccionario, la inicializamos con 0
        if key not in counter:
            counter[key] = 0
        # Incrementamos el contador de la clave con el valor actual
        counter[key] += value
    
    # Convertimos el diccionario en una lista de tuplas (clave, valor) y la retornamos
    return list(counter.items())

#
# ORQUESTADOR:
#
def run():
    """Orquestador"""

    # Ejecuta el trabajo de MapReduce para la consulta 1
    # Calcula la tasa de propina (tip_rate) y la añade como una nueva columna
    run_mapreduce_job(
        mapper=mapper_query_1,  # Función mapper para procesar los datos
        reducer=reducer_query_1,  # Función reducer para consolidar los datos
        input_directory="files/input",  # Directorio de entrada con los datos
        output_directory="files/query_1",  # Directorio de salida para los resultados
    )    

    # Ejecuta el trabajo de MapReduce para la consulta 2
    # Filtra las filas donde el valor de la columna "time" es igual a "Dinner"
    run_mapreduce_job(
        mapper=mapper_query_2,  # Función mapper para procesar los datos
        reducer=reducer_query_2,  # Función reducer para consolidar los datos
        input_directory="files/input",  # Directorio de entrada con los datos
        output_directory="files/query_2",  # Directorio de salida para los resultados
    )    

    # Ejecuta el trabajo de MapReduce para la consulta 3
    # Filtra las filas donde "time" es "Dinner" y "tip" es mayor a 5.00
    run_mapreduce_job(
        mapper=mapper_query_3,  # Función mapper para procesar los datos
        reducer=reducer_query_3,  # Función reducer para consolidar los datos
        input_directory="files/input",  # Directorio de entrada con los datos
        output_directory="files/query_3",  # Directorio de salida para los resultados
    )    

    # Ejecuta el trabajo de MapReduce para la consulta 4
    # Filtra las filas donde "size" es mayor o igual a 5 o "total_bill" es mayor a 45
    run_mapreduce_job(
        mapper=mapper_query_4,  # Función mapper para procesar los datos
        reducer=reducer_query_4,  # Función reducer para consolidar los datos
        input_directory="files/input",  # Directorio de entrada con los datos
        output_directory="files/query_4",  # Directorio de salida para los resultados
    )

    # Ejecuta el trabajo de MapReduce para la consulta 5
    # Agrupa las filas por el valor de la columna "sex" y cuenta las ocurrencias
    run_mapreduce_job(
        mapper=mapper_query_5,  # Función mapper para procesar los datos
        reducer=reducer_query_5,  # Función reducer para consolidar los datos
        input_directory="files/input",  # Directorio de entrada con los datos
        output_directory="files/query_5",  # Directorio de salida para los resultados
    )

if __name__ == "__main__":

    run()



# pylint: disable=broad-exception-raised

import fileinput
import glob
import os
import shutil
import time
from itertools import groupby
from toolz.itertoolz import concat, pluck


def copy_raw_files_to_input_folder(n):
    """Generate n copies of the raw files in the input folder"""
    os.makedirs("files/input", exist_ok=True)
    raw_files = glob.glob("files/raw/*")
    if not raw_files:
        raise Exception("No hay archivos en files/raw para copiar.")

    for i in range(n):
        for f in raw_files:
            base = os.path.basename(f)
            dst = f"files/input/{i}_{base}"
            shutil.copy(f, dst)


def load_input(input_directory):
    """Leer todos los archivos del input como un generador de líneas"""
    files = glob.glob(os.path.join(input_directory, "*"))
    return fileinput.input(files, openhook=fileinput.hook_encoded("utf-8"))


def preprocess_line(x):
    """Preprocesar una línea"""
    return x.strip().lower()


def map_line(x):
    """Map: convierte línea en pares (palabra, 1)"""
    return [(word, 1) for word in x.split() if word]


def mapper(sequence):
    """Mapper: aplica map_line a todas las líneas"""
    return concat(map(map_line, map(preprocess_line, sequence)))


def shuffle_and_sort(sequence):
    """Agrupar por clave (palabra)"""
    sorted_seq = sorted(sequence, key=lambda x: x[0])
    return ((k, list(pluck(1, g))) for k, g in groupby(sorted_seq, key=lambda x: x[0]))


def compute_sum_by_group(group):
    """Suma la lista de valores"""
    key, values = group
    return key, sum(values)


def reducer(sequence):
    """Reducer"""
    return list(map(compute_sum_by_group, sequence))


def create_directory(directory):
    """Crear directorio de salida"""
    os.makedirs(directory, exist_ok=True)


def save_output(output_directory, sequence):
    """Guardar resultados en archivo"""
    output_file = os.path.join(output_directory, "result.txt")
    with open(output_file, "w", encoding="utf-8") as f:
        for key, value in sequence:
            f.write(f"{key}\t{value}\n")


def create_marker(output_directory):
    """Crear archivo de finalización"""
    marker_file = os.path.join(output_directory, "_SUCCESS")
    with open(marker_file, "w", encoding="utf-8") as f:
        f.write("Job completed successfully.\n")


def run_job(input_directory, output_directory):
    """Ejecución del Job MapReduce"""
    sequence = load_input(input_directory)
    sequence = mapper(sequence)
    sequence = shuffle_and_sort(sequence)
    sequence = reducer(sequence)
    create_directory(output_directory)
    save_output(output_directory, sequence)
    create_marker(output_directory)


if __name__ == "__main__":

    copy_raw_files_to_input_folder(n=1000)

    start_time = time.time()

    run_job(
        "files/input",
        "files/output",
    )

    end_time = time.time()
    print(f"Tiempo de ejecución: {end_time - start_time:.2f} segundos")

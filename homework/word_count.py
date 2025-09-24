# pylint: disable=broad-exception-raised

import glob
import os
import string
import time


def read_lines_from_files(input_dir):
    """Lee todos los archivos del input y devuelve lista de (archivo, línea)."""
    sequence = []
    files = glob.glob(f"{input_dir}/*")
    for file in files:
        with open(file, "r", encoding="utf-8") as f:
            for line in f:
                sequence.append((file, line))
    return sequence


def apply_shuffle_and_sort(pairs_sequence):
    """Ordena los pares por clave (palabra)."""
    return sorted(pairs_sequence)


def write_results_to_file(result, output_dir):
    """Escribe resultados en un archivo de salida."""
    with open(f"{output_dir}/part-00000", "w", encoding="utf-8") as f:
        for key, value in result:
            f.write(f"{key}\t{value}\n")


def create_success_file(output_dir):
    """Crea archivo _SUCCESS para marcar finalización."""
    with open(f"{output_dir}/_SUCCESS", "w", encoding="utf-8") as f:
        f.write("")


def create_output_dir_or_fail(output_dir):
    """Crea el directorio de salida, o falla si ya existe."""
    if os.path.exists(output_dir):
        raise Exception(f"Output directory '{output_dir}' already exists.")
    else:
        os.makedirs(output_dir)


def initialize_directory(directory):
    """Vacía un directorio o lo crea si no existe."""
    if os.path.exists(directory):
        for file in glob.glob(f"{directory}/*"):
            os.remove(file)
    else:
        os.makedirs(directory)


def copy_and_number_raw_files_to_input_folder(raw_dir, input_dir, n=5):
    """Copia y multiplica los archivos de raw en input con numeración."""
    for file in glob.glob(f"{raw_dir}/*"):
        with open(file, "r", encoding="utf-8") as f:
            text = f.read()

        for i in range(1, n + 1):
            raw_filename_with_extension = os.path.basename(file)
            raw_filename_without_extension = os.path.splitext(
                raw_filename_with_extension
            )[0]
            new_filename = f"{raw_filename_without_extension}_{i}.txt"
            with open(f"{input_dir}/{new_filename}", "w", encoding="utf-8") as f2:
                f2.write(text)


def wordcount_mapper(sequence):
    """Transforma líneas en pares (palabra, 1)."""
    pairs_sequence = []
    for _, line in sequence:
        line = line.lower()
        line = line.translate(str.maketrans("", "", string.punctuation))
        line = line.replace("\n", "")
        words = line.split()
        pairs_sequence.extend((word, 1) for word in words)

    return pairs_sequence


def wordcount_reducer(pairs_sequence):
    """Agrupa y suma los valores por clave (palabra)."""
    result = []
    for key, value in pairs_sequence:
        if result and result[-1][0] == key:
            result[-1] = (key, result[-1][1] + value)
        else:
            result.append((key, value))
    return result


def run_experiment(n, raw_dir, input_dir, output_dir):
    """Ejecuta el experimento completo sin mapreduce()."""
    # preparar directorios
    initialize_directory(input_dir)
    copy_and_number_raw_files_to_input_folder(raw_dir, input_dir, n)

    start_time = time.time()

    sequence = read_lines_from_files(input_dir)
    pairs_sequence = wordcount_mapper(sequence)
    pairs_sequence = apply_shuffle_and_sort(pairs_sequence)
    result = wordcount_reducer(pairs_sequence)
    create_output_dir_or_fail(output_dir)
    write_results_to_file(result, output_dir)
    create_success_file(output_dir)

    end_time = time.time()
    print(f"Tiempo de ejecución: {end_time - start_time:.2f} segundos")


if __name__ == "__main__":
    run_experiment(
        n=10,
        raw_dir="files/raw",
        input_dir="files/input",
        output_dir="files/output",
    )

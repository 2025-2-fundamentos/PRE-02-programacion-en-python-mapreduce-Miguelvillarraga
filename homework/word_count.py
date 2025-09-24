# pylint: disable=broad-exception-raised

import glob
import os
import shutil
import string
import time
from itertools import groupby


def copy_raw_files_to_input_folder(n):
    """Generate n copies of the raw files in the input folder"""
    os.makedirs("files/input", exist_ok=True)
    raw_files = glob.glob("files/raw/*")
    if not raw_files:
        raise Exception("No hay archivos en files/raw para copiar.")

    for i in range(1, n + 1):
        for f in raw_files:
            base = os.path.basename(f)
            name, ext = os.path.splitext(base)
            dst = f"files/input/{name}_{i}{ext}"
            shutil.copy(f, dst)


def load_input(input_directory):
    """Read all lines from input directory"""
    sequence = []
    files = glob.glob(os.path.join(input_directory, "*"))
    for file in files:
        with open(file, "r", encoding="utf-8") as f:
            for line in f:
                sequence.append((file, line))
    return sequence


def preprocess_line(x):
    """Preprocess the line x"""
    line = x.lower()
    line = line.translate(str.maketrans("", "", string.punctuation))
    return line.strip()


def map_line(x):
    """Convert a line into (word,1) pairs"""
    return [(word, 1) for word in x.split() if word]


def mapper(sequence):
    """Mapper"""
    pairs_sequence = []
    for _, line in sequence:
        line = preprocess_line(line)
        pairs_sequence.extend(map_line(line))
    return pairs_sequence


def shuffle_and_sort(sequence):
    """Shuffle and Sort"""
    return sorted(sequence, key=lambda x: x[0])


def compute_sum_by_group(group):
    """Sum values for a key"""
    key, values = group
    return key, sum(v for _, v in values)


def reducer(sequence):
    """Reducer"""
    result = []
    for key, group in groupby(sequence, key=lambda x: x[0]):
        result.append(compute_sum_by_group((key, group)))
    return result


def create_directory(directory):
    """Create Output Directory"""
    if os.path.exists(directory):
        raise Exception(f"Output directory '{directory}' already exists.")
    os.makedirs(directory)


def save_output(output_directory, sequence):
    """Save Output in part-00000"""
    output_file = os.path.join(output_directory, "part-00000")
    with open(output_file, "w", encoding="utf-8") as f:
        for key, value in sequence:
            f.write(f"{key}\t{value}\n")


def create_marker(output_directory):
    """Create Marker"""
    marker_file = os.path.join(output_directory, "_SUCCESS")
    with open(marker_file, "w", encoding="utf-8") as f:
        f.write("")


def run_job(input_directory, output_directory):
    """Job"""
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

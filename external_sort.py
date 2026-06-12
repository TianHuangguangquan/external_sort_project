import argparse
import heapq
import os
import shutil
import tempfile
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from pathlib import Path

# parse the cmd-line arguments
def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_file")
    parser.add_argument("max_numbers_in_memory", type=int)
    return parser.parse_args()

# build the output path
def build_output_path(input_path):
    return input_path.with_name(f"{input_path.stem}_sorted{'txt'}")

# read the input file and yield one chunk of integers at a time
def read_chunks(input_path, chunk_size):
    with input_path.open("r", encoding="utf-8") as source:
        chunk = []
        for line in source:
            chunk.append(int(line.strip()))
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

# sort one chunk in a worker process and write it to a temporary file
def sort_chunk(numbers, temp_dir, index):
    numbers.sort()
    chunk_path = (Path(temp_dir)/ f"chunk_{index:06d}.txt")
    with chunk_path.open("w",encoding="utf-8",newline="\n") as target:
        target.writelines(f"{number}\n" for number in numbers)
    return str(chunk_path)

# collect completed worker tasks and their sorted chunk paths
def collect_done_futures(pending_futures,chunk_files,wait_all):
    while pending_futures:
        done, _ = wait(pending_futures,return_when=FIRST_COMPLETED)
        for future in done:
            chunk_files.append(Path(future.result()))
            pending_futures.remove(future)
        if not wait_all:
            break

# create sorted chunk files with all available worker processes
def create_sorted_chunks(input_path,temp_dir,chunk_size,workers):
    chunk_files = []
    pending_futures = set()
    with ProcessPoolExecutor(max_workers=workers) as executor:
        for index, numbers in enumerate(read_chunks(input_path, chunk_size),start=1):
            future = executor.submit(sort_chunk,numbers,str(temp_dir),index)
            pending_futures.add(future)
            if len(pending_futures) >= workers:
                collect_done_futures(pending_futures,chunk_files,wait_all=False)
        collect_done_futures(pending_futures,chunk_files,wait_all=True)
    return chunk_files

# merge all sorted chunk files into one sorted output file
def merge_files(input_paths, output_path):
    handles = []
    heap = []
    try:
        for file_index, input_path in enumerate(input_paths):
            handle = input_path.open( "r", encoding="utf-8")
            handles.append(handle)
            line = handle.readline()
            if line:
                heap.append((int(line.strip()),file_index))
        heapq.heapify(heap)
        with output_path.open("w",encoding="utf-8",newline="\n") as target:
            while heap:
                value, file_index = (heapq.heappop(heap))
                target.write(f"{value}\n")
                line = handles[file_index].readline()
                if line:
                    heapq.heappush(heap,(int(line.strip()),file_index))
    finally:
        for handle in handles:
            handle.close()

# merge all chunks and keep both the merged file and final output
def merge_all_chunks(chunk_files,temp_dir,output_path):
    merged_output = (temp_dir / "merged_output.txt")
    merge_files(chunk_files,merged_output)
    shutil.copyfile(merged_output,output_path)
    return merged_output

# run the complete multi-process external merge sort workflow
def main():
    args = parse_arguments()
    input_path = (Path(args.input_file).expanduser().resolve())
    workers = os.cpu_count() or 1
    chunk_size = args.max_numbers_in_memory // workers
    output_path = build_output_path(input_path)
    temp_dir = Path(tempfile.mkdtemp(prefix="external_sort_temp_",dir=str(input_path.parent)))
    print("sorting started")
    chunk_files = create_sorted_chunks(input_path,temp_dir,chunk_size,workers)
    merged_output = merge_all_chunks(chunk_files,temp_dir,output_path)
    print("sorting finished")
    print(f"output file: {output_path}")

if __name__ == "__main__":
    main()

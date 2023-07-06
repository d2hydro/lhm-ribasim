import numpy as np

def report_progress(iteration, total, interval=10, print_at_interval=False):
    percent = (iteration / total) * 100
    progress = int(percent / interval)
    line = f"[{'#' * progress}{' ' * (int(100 / interval) - progress)}]"
    if print_at_interval:
        _percent = ((iteration - 1) / total) * 100
        if (int(_percent / interval) < progress) | (iteration == 0):
            print(f'{line} {percent:.1f}% completed', end='\r')
    else:
        print(f'{line} {percent:.1f}% completed', end='\r')

def next_index(arr):
    sorted_arr = np.sort(arr)
    unique_values = np.unique(sorted_arr)
    max_value = unique_values.max()
    lowest_non_used_integer = next((i for i in range(1, max_value + 2) if i not in unique_values), None)
    return lowest_non_used_integer

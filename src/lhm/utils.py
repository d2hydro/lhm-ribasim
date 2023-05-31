
def report_progress(iteration, total, interval=10):
    percent = (iteration / total) * 100
    progress = int(percent / interval)
    line = '[' + '#' * progress + ' ' * (int(100 / interval) - progress) + ']'
    print(f'{line} {percent:.1f}% completed', end='\r')
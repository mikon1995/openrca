
import concurrent.futures
import os, glob

def parallel_run_filelist(fls_or_file, n_procs, func, slice_idx=-1, slice_num=-1, *args, **kwargs):
    if isinstance(fls_or_file, str):
        file_lines = open(fls_or_file).readlines()
    else:
        file_lines = fls_or_file
    n_procs = min(n_procs, len(file_lines))
    csize = len(file_lines) // n_procs
    if  len(file_lines) % n_procs != 0:
        csize += 1

    res_list = []
    thread_list = []
    if slice_idx != -1:
        assert slice_idx >= 0 and slice_num > slice_idx
        n_per = n_procs // slice_num
        if n_procs % slice_num != 0:
            n_per += 1
        s_tidx = n_per * slice_idx
        e_tidx = min(n_per * (slice_idx + 1), n_procs)
    else:
        s_tidx = 0
        e_tidx = n_procs
    with concurrent.futures.ProcessPoolExecutor() as executor:
        # 提交任务给线程池
        for tidx in range(n_procs):
            if not (tidx >= s_tidx and tidx < e_tidx):
                continue
            s = tidx * csize
            if s >= len(file_lines):
                break
            e = min((tidx+1) * csize, len(file_lines))
            t = executor.submit(func, tidx, file_lines[s:e], *args, **kwargs)
            # 等待任务完成，并获取返回值
            thread_list.append(t)
        for t in concurrent.futures.as_completed(thread_list): # 并发执行
            res_list.append(t.result())
    return res_list

def parallel_run_filesplit(file_name, n_procs, func, tmp_split_dir, slice_idx=-1, slice_num=-1, *args, **kwargs):

    assert isinstance(file_name, str)
    use_cache = False
    status_file = list(glob.glob(f'{tmp_split_dir}/{n_procs}_*.status'))
    print('check if exists split status')
    if len(status_file) == 1:
        status_file = status_file[0]
        n_procs, n_file_lines, n_line_per_file, n_file = os.path.basename(status_file).rsplit('.', 1)[0].split('_')
        split_files = sorted(list(glob.glob(f'{tmp_split_dir}/part-*')), key=lambda a:int(a.split('-')[-1]))
        print('check if file num match')
        if len(split_files) == int(n_file):
            n_line = len(open(split_files[0]).readlines())
            if n_line == int(n_line_per_file):
                use_cache = True
    if not use_cache:
        n_file_lines = 0
        for _ in open(file_name):
            n_file_lines += 1
        n_line_per_file = n_file_lines// n_procs
        if  n_file_lines % n_procs != 0:
            n_line_per_file += 1
        n_file = n_file_lines // n_line_per_file
        if n_file_lines % n_line_per_file != 0:
            n_file += 1
        os.system(f'rm -rf {tmp_split_dir}/* && split -l {n_line_per_file} {file_name} -d -a {len(str(n_procs))} {tmp_split_dir}/part-')
        os.system(f'cd {tmp_split_dir} && touch {n_procs}_{n_file_lines}_{n_line_per_file}_{n_file}.status')
    else:
        print('use cached split json')
    res_list = []
    thread_list = []
    n_files = len(list(glob.glob(f'{tmp_split_dir}/part-*')))
    if slice_idx != -1:
        assert slice_idx >= 0 and slice_num > slice_idx
        n_per = n_files // slice_num
        if n_files % slice_num != 0:
            n_per += 1
        s = n_per * slice_idx
        e = min(n_per * (slice_idx + 1), n_files) 
    else:
        s = 0
        e = n_files
    
    with concurrent.futures.ProcessPoolExecutor() as executor:
        # 提交任务给线程池
        for tidx, fname in enumerate(sorted(glob.glob(f'{tmp_split_dir}/part-*'), key=lambda a:int(a.split('-')[-1]))):
            if not (tidx >= s and tidx < e):
                continue
            t = executor.submit(func, tidx, fname ,*args, **kwargs)
            # 等待任务完成，并获取返回值
            thread_list.append(t)
        for t in concurrent.futures.as_completed(thread_list): # 并发执行
            res_list.append(t.result())
    return res_list, n_file_lines
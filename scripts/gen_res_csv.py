# -*- coding: utf-8 -*-

# **NOTE**, this file should locate in the same directory as the result folder
# Set the query result parameter, {type}{round}{suffix}
type = ('C', 'L', 'S', 'SF') # query file type (e.g. `('C', 'L', 'SF', 'S')` for WatDiv), or file suffix (e.g. `('lubm_q', )` for LUBM)
suffix= '.txt'              # the suffix of the result file (e.g. `.out`, `.txt`, etc)
rounds = 20                 # the query file number


with open('_result.csv', 'w', encoding='utf-8') as wf:
    for t in type:
        stats = []
        for r in range(1, rounds + 1):
            with open(f'{t}{r}{suffix}', 'r', encoding='utf-8') as rf:
                lines = rf.readlines()
                time = lines[3].split(':')[1][:-4].strip()
                result = lines[-1].split(' ')[0]
                stats.append([f'{t}{r}', time, result])
        for i in range(len(stats)):
            print(stats[i][0], end=',', file=wf) 
        print(file=wf)
        for i in range(len(stats)):
            print(stats[i][1], end=',', file=wf) 
        print(file=wf)
        for i in range(len(stats)):
            print(stats[i][2], end=',', file=wf) 
        print(file=wf)


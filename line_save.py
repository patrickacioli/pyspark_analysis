#!/usr/bin/python
# Read file and save only specific number of lines
f = open("access_log_Aug95")
i = 0
for line in f:
    ff = open("test", "a")
    print(r'%s' % line[:-1])
    ff.write(line)
    if i == 1000: break
    else: i+=1

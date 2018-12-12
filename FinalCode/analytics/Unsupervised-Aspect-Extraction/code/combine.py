

for i in range(200):
	index = str(i).zfill(3)
	fprob = open("train_tpl_20/restaurant/aspect_probs"+index)
	ftext = open("../datasets/restaurant/test/part-00"+index)
	fout = open("train_tpl_20/restaurant/results_lv/part-00"+index,'w')
	plist = []
	tlist = []
	for line in fprob:
		line = line[:-1]
		if len(line) > 0:
			plist.append(line)
	for line in ftext:
		line = line[:-1]
		if len(line) > 0:
			temp = (line.split('\t'))[2:5]
			tlist.append(','.join(temp))
	pnum = len(plist)
	tnum = len(tlist)
	print pnum, tnum
	for j in range(pnum):
		first = plist[j]
		second = tlist[j]
		if second != "NULL":
			fout.write(first+','+second+'\n')
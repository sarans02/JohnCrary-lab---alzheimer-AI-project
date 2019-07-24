from os import walk, path

def getAll(masterDirectory): 
	files = []
	# r=root, d=directories, f = files
	for r, d, f in walk(masterDirectory):
	    for file in f:
	        files.append(path.join(r, file))

	return files 
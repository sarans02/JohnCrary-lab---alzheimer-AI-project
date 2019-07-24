import boto3 
import batch_functions as bf 
import configparser 

KEY            ='G:\\' 				# Where the files to be upload are stored 
BUCKET         ='crary'				# Bucket destination (should not change)
RESULTPATH     ='BronxVA'			# Destination folder of the files in s3 
MAX_FILES      = 20000 				# Maximum number of files to be uploaded 
TEST           = False  			# Set to false to use/true to test 

config = configparser.ConfigParser() 
config.sections() 

config.read('s3.config')

YOUR_ACCESS_KEY=config['CREDENTIALS']['YOUR_ACCESS_KEY'] 
YOUR_SECRET_KEY=config['CREDENTIALS']['YOUR_SECRET_KEY']

# Create a connection to S3 
s3 = boto3.client(
	's3', 
	aws_access_key_id=YOUR_ACCESS_KEY,
	aws_secret_access_key=YOUR_SECRET_KEY
)

# query s3 for files already in RESULTPATH 
def getFiles(s3Client, bucket=BUCKET, prefix=RESULTPATH): 
	kwargs = {'Bucket': bucket, 'Prefix': prefix}

	iterate = True
	i = 0 
	files = {}
	while iterate: 
		resp = s3Client.list_objects_v2(**kwargs)
		for obj in resp['Contents']:
			filename = obj['Key']
			files[i] = filename 
			i+=1 

		try: 
			kwargs['ContinuationToken'] = resp['NextContinuationToken']
		except KeyError: 
			iterate = False 
	return files 

def uploadGood(directoryToUpload, s3Client, filesInCloud, s3DumpFileLocation): 
	i = 0 
	batched = []

	localFiles = bf.getAll(directoryToUpload)

	for localFile in localFiles: 
		targetFile = localFile.split(directoryToUpload)[1].replace('\\','/')

		tempFileName = s3DumpFileLocation+'/'+targetFile
		if tempFileName in filesInCloud.values(): 
			print(tempFileName + ' already in files')
		else: 
			print("Attempting to upload " + localFile + "...")
			if not TEST: s3Client.upload_file(localFile, BUCKET, tempFileName)
			print("Successfully uploaded " + localFile + ' to ' + tempFileName)

			batched.append(tempFileName)

			i += 1 
			if i==MAX_FILES: break 
	return batched, i 

#TODO: Writeout files batched to a save file? 
batched_files, batch_size = uploadGood(KEY, s3, getFiles(s3), RESULTPATH)

print("Batched", batch_size, "files.")
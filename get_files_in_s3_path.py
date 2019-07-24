import boto3 
import batch_functions as bf 
import configparser 
import numpy as np 

KEY            ='G:\\' 				# Where the files to upload are stored 
BUCKET         ='crary'				# Bucket destination (should not change)
RESULTPATH     ='BronxVA'			# Destination folder for files in s3 
MAX_FILES      = 20000 				# Max number of files to upload 
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

def getSaveFileLocation(): return "bucket=" + BUCKET + "_" + RESULTPATH + "_contents.csv"

filesInS3 = np.array(list(getFiles(s3).values()))
#np.savetxt(getSaveFileLocation(), filesInS3, delimiter="\t", fmt='%s')
#filesInS3 = np.loadtxt(getSaveFileLocation(), delimiter='\t', dtype=[('filepath', 'S100')])
filesInS3 = np.array([f.split('BronxVA/')[1] for f in filesInS3])
filesInS3.sort()

filesOnDrive = bf.getAll('G:\\')
filesOnDrive = [f.split('G:\\')[1].replace('\\', '/') for f in filesOnDrive]
filesOnDrive.sort()

for i in range(0, 1000): 
	if filesInS3[i] not in filesOnDrive: 
		print(filesInS3[i])

print(len(np.unique(filesInS3)), len(filesInS3))
print(len(filesInS3) - len(filesOnDrive))


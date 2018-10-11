import boto3
import io
import pickle
import numpy
import tqdm


s3 = boto3.client('s3', aws_access_key_id='AKIAJ23M6YJYX47DEN5Q',
                  aws_secret_access_key='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')

# How to save an pickled object
obj = numpy.random.uniform(size=9 * 10**6)
pkl_obj = io.BytesIO()
pickle.dump(obj, pkl_obj)

s3.put_object(Body=pkl_obj.getvalue(), Bucket='chlangstorage', Key='testing3.pkl')
s3.put_object(Body=pkl_obj.getvalue(), Bucket='chlangstorage', Key='tmp/testing3.pkl')

for i in tqdm.tqdm(range(1100), ncols=100):
    obj = numpy.random.uniform(size=9 * 10**2)
    pkl_obj = io.BytesIO()
    pickle.dump(obj, pkl_obj)

    filename = 'dump/' + str(i).zfill(4) + '.pkl'

    s3.put_object(Body=pkl_obj.getvalue(), Bucket='chlangstorage',
                  Key=filename)
# List objects on S3
s3.list_buckets()

s3_obj = s3.list_objects(Bucket='chlangstorage')  # list at root
s3_obj = s3.list_objects(Bucket='chlangstorage', Prefix='tmp/')
obj_names = [i['Key'] for i in s3_obj['Contents']]

# If objects is more than 1000, use paginator
paginator = s3.get_paginator("list_objects")
page_iterator = paginator.paginate(Bucket='chlangstorage', Prefix='dump/')
obj_names = list()
for page in page_iterator:
    obj_names.extend([i['Key'] for i in page['Contents']])

# How to read pickled object
obj = s3.get_object(Bucket='chlangstorage', Key='testing3.pkl')
bytes_obj = io.BytesIO(obj['Body'].read())
read_pkl = pickle.load(bytes_obj)

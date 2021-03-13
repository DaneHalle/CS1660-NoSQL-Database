import boto3
s3 = boto3.resource('s3', aws_access_key_id='AKIA6H5ZMTKILDBH3LO7', aws_secret_access_key='OMIT')
# Commented out after first creation
# s3.create_bucket(Bucket='datacont-dmh148', CreateBucketConfiguration={'LocationConstraint':'us-west-2'})
s3.Object('datacont-dmh148', 'test.jpg').put(Body=open('./test.jpg', 'rb'))

dyndb = boto3.resource('dynamodb',
		region_name='us-west-2',
		aws_access_key_id='AKIA6H5ZMTKILDBH3LO7',
		aws_secret_access_key='OMIT'
	)

try:
	table = dyndb.create_table(
		TableName='DataTable-dmh148',
		KeySchema=[
			{
				'AttributeName': 'PartitionKey',
				'KeyType': 'HASH'
			},
			{
				'AttributeName': 'RowKey',
				'KeyType': 'RANGE'
			}
		],
		AttributeDefinitions=[
			{
				'AttributeName': 'PartitionKey',
				'AttributeType': 'S'
			},
			{
				'AttributeName': 'RowKey',
				'AttributeType': 'S'
			},
		],
		ProvisionedThroughput={
			'ReadCapacityUnits': 5,
			'WriteCapacityUnits': 5
		}
	)
except:
	#if there is an exception, the table may already exist. if so...
	table = dyndb.Table("DataTable-dmh148")
#wait for the table to be created
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable-dmh148')
# print(table.item_count)

import csv
import codecs

print("\n\nUploading data from the master csv file now...")
with open('C:/Users/daneh/OneDrive - University of Pittsburgh/Spring2021/CS1660/CS1660-NoSQL-Database/experiments.csv', 'r') as csvfile:
	csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
	for item in csvf:
		try:
			body = open('C:/Users/daneh/OneDrive - University of Pittsburgh/Spring2021/CS1660/CS1660-NoSQL-Database/'+item[4], 'rb')
			print(item)
		except:
			continue
		s3.Object('datacont-dmh148', item[4]).put(Body=body)
		md = s3.Object('datacont-dmh148', item[4]).Acl().put(ACL='public-read')

		url = " https://s3-us-west-2.amazonaws.com/datacont-dmh148/"+item[4]
		metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
		'description' : item[3], 'date' : item[2], 'url':url}
		try:
			table.put_item(Item=metadata_item)
		except:
			print("item may already be there or another failure")


print("...Done Uploading data from the master csv file\n\n")

print("\n\nChecking the response from the table now...")

response = table.get_item(
	Key={
		'PartitionKey': 'experiment1',
		'RowKey': 'data1'
	}
)
item = response['Item']
print(item)

print("...Done Checking the response from the table\n\n")
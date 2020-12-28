#!/usr/bin/env python
import skew
import argparse
import boto3
import json
from collections import namedtuple
from pkg_resources import resource_filename


aws_service = namedtuple("aws_service","name service_code skew_resource filters columns")

services = [
            aws_service('ec2','AmazonEC2','reserved',{'State': 'active'},['InstanceType','InstanceCount','Duration','FixedPrice','ProductDescription','Start']),
            aws_service('rds','AmazonRDS','reserved',{'State': 'active'},['DBInstanceClass','DBInstanceCount','Duration','FixedPrice','ReservedDBInstanceId','StartTime','MultiAZ']),
            aws_service('elasticache','AmazonElastiCache','reserved',{'State': 'active'},['CacheNodeType','CacheNodeCount','Duration','FixedPrice','ReservedCacheNodeId','StartTime']),
            aws_service('es','AmazonES','reserved',{'State': 'active'},['ElasticsearchInstanceType','ElasticsearchInstanceCount','Duration','FixedPrice','ReservationName','StartTime']),
            aws_service('redshift','AmazonRedshift','reserved',{'State': 'active'},['NodeType','NodeCount','Duration','FixedPrice','ReservedNodeId','StartTime'])
            ]


# Search product filter
FLT = '['\
      '{{"Field": "operatingSystem", "Value": "{o}", "Type": "TERM_MATCH"}},'\
      '{{"Field": "location", "Value": "{r}", "Type": "TERM_MATCH"}},'\
      '{{"Field": "instanceType", "Value": "{t}", "Type": "TERM_MATCH"}},'\
      '{{"Field": "tenancy", "Value": "shared", "Type": "TERM_MATCH"}},'\
      '{{"Field": "preInstalledSw", "Value": "NA", "Type": "TERM_MATCH"}},'\
      '{{"Field": "capacitystatus", "Value": "Used", "Type": "TERM_MATCH"}}'\
      ']'


# Get current AWS price for an on-demand instance
def get_price(region, service_code,instance, os):
    f = FLT.format(r=region, t=instance, o=os)
    data = client.get_products(ServiceCode=service_code, Filters=json.loads(f))

    idx = len(data['PriceList'])-1
    od = json.loads(data['PriceList'][idx])['terms']['OnDemand']
    od1 = list(od)[0]
    od2 = list(od[od1]['priceDimensions'])[0]
    odp = od[od1]['priceDimensions'][od2]['pricePerUnit']['USD']

    ri = json.loads(data['PriceList'][idx])['terms']['Reserved']
    ri1 = list(ri)[0]
    ri2 = list(ri[ri1]['priceDimensions'])[0]
    rip = ri[ri1]['priceDimensions'][ri2]['pricePerUnit']['USD']

    return odp,rip

# Translate region code to region name
def get_region_name(region_code):
    default_region = 'EU (Ireland)'
    endpoint_file = resource_filename('botocore', 'data/endpoints.json')
    try:
        with open(endpoint_file, 'r') as f:
            data = json.load(f)
        return data['partitions'][0]['regions'][region_code]['description']
    except IOError:
        return default_region

def flatten(d, parent_key='', sep='.'):
    items = []
    for k, v in d.items():
        try:
            items.extend(flatten(v, '%s%s%s' % (parent_key, k,sep)).items())
        except AttributeError:
            items.append(('%s%s' % (parent_key, k), v))
    return dict(items)

if __name__ == '__main__':

    parser=argparse.ArgumentParser(description='Generates AWS Reserved Instance Reports')
    parser.add_argument("-r","--regions",required=True,nargs='*',help="space delimitted aws region e.g. us-east-1 us-west-1") # string
    parser.add_argument("-p","--printheader",required=False,action='store_true',help="output header") # string
    args = parser.parse_args()

    default_regions = ['us-east-1','us-west-2','eu-west-1','eu-central-1']
    regions = args.regions if args.regions else default_regions

    if args.printheader:
        print('region,resource,od_price,ri_price,type,count,duration,fixed_price,fixed_price,id,start_time,dim_1')

    client = boto3.client('pricing', region_name='us-east-1')

    for region in regions:
        for service in services:
            for instance in skew.scan('arn:aws:'+service.name+':'+region+':*:reserved/*'):
                data = flatten(instance.data)
                include=True
                for key in service.filters.keys():
                    include = include and (data[key] == service.filters[key])
                if not include:
                    continue # Skip to the next loop (instance)

                od_price,ri_price = get_price(get_region_name(region), aws_service.service_code, data[service.columns[0]], 'Linux')

                print('{},{}'.format(region,service.name),end='')
                print('{},{}'.format(od_price,ri_price),end='')
                for col in service.columns:
                    print(',{}'.format(data[col]),end='')
                print('')

#!/usr/bin/env python
import skew
import argparse
from collections import namedtuple

aws_service = namedtuple("aws_service","name code filters cols ex_cols")

services = [
            aws_service('ec2','AmazonEC2',{'State': 'active'},['InstanceType','ProductDescription','Start','Duration','InstanceCount'],[]),
            aws_service('rds','AmazonRDS',{'State': 'active'},['DBInstanceClass','ReservedDBInstanceId','StartTime','Duration','DBInstanceCount'],['MultiAZ']),
            aws_service('elasticache','AmazonElastiCache',{'State': 'active'},['CacheNodeType','ReservedCacheNodeId','StartTime','Duration','CacheNodeCount'],[]),
            aws_service('es','AmazonES',{'State': 'active'},['ElasticsearchInstanceType','ReservationName','StartTime','Duration','ElasticsearchInstanceCount'],[]),
            aws_service('redshift','AmazonRedshift',{'State': 'active'},['NodeType','ReservedNodeId','StartTime','Duration','NodeCount'],[])
            ]

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
        print('region,resource,instance_type,id,start_date,duration,count,dim_1')

    for region in regions:
        for service in services:
            for instance in skew.scan('arn:aws:'+service.name+':'+region+':*:reserved/*'):
                data = flatten(instance.data)
                include=True
                for key in service.filters.keys():
                    include = include and (data[key] == service.filters[key])
                if not include:
                    continue # Skip to the next loop (instance)

                print('{},{}'.format(region,service.name),end='')
                for col in service.cols:
                    print(',{}'.format(data[col]),end='')
                for col in service.ex_cols:
                    print(',{}'.format(data[col]),end='')
                print('')
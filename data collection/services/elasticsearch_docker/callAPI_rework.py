import os
from absl import app
from absl import flags
from api.obs_api import *
import re
import csv
import time
from elasticsearch import Elasticsearch,helpers
import yaml
import datetime
from pathos.multiprocessing import ProcessingPool as Pool
from multiprocess import Manager
from multiprocessing import  Value, cpu_count
from ctypes import c_int

#upload folder to obs
FLAGS = flags.FLAGS
flags.DEFINE_string("input_folder","./data/API_input","source folder need to be uploaded")
flags.DEFINE_string("obs_folder","processed/pending-verification","destination folder need to process")

def get_filename(file_dir):
    files = {}
    for root, d_names, f_names in os.walk(file_dir):
        for f in f_names:
            files[f]=os.path.join(root,f)
    return files


def main(_):


	input_folder=FLAGS.input_folder
	obs_folder=FLAGS.obs_folder
	with open("config.yaml", 'r') as stream:
		try:
			es_address=yaml.safe_load(stream)
		except yaml.YAMLError as exc:
			print(exc)
	es_address=es_address['es_address'].split(",")
	es = Elasticsearch(es_address)

	#Set elastic search variables
	index="asr-verification"
	#fields=["filename"]
	body={"query":{"bool":{"must":[{"term":{"success":"false"}},{"query_string":{"default_field":"error","query":"rpc error"}}],"must_not":[],"should":[]}},"from":0,"size":10,"sort":[],"aggs":{}}
	def process_file(result):
		API_failed_folder="data/API_failed"
		pre_API_folder="data/cut_output"
		doc_id=result["_id"]
		filename=result["_source"]["filename"]
		local_file_list=get_filename(API_failed_folder)
		sourcefilepath=os.path.join(API_failed_folder,filename)
		desfilepath=os.path.join(pre_API_folder,filename)
		if filename in local_file_list:
			os.rename(sourcefilepath,desfilepath)
		#es.delete(index=index,id=doc_id,refresh='true')
	i=3
	while(i):
		i=i-1
		results=es.search(index=index,body=body)
		print(results["hits"]["total"])
		if results["hits"]["total"]==0:break
		process_num=int(cpu_count()-2)
		pool=Pool(process_num)
		pool.map(process_file,results["hits"]["hits"])

	
if __name__ == "__main__":

	app.run(main)

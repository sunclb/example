from tensor2tensor.data_generators import generator_utils
from tensor2tensor.data_generators import problem
from tensor2tensor.data_generators import librispeech
from tensor2tensor.utils import registry

import tensorflow as tf
import math
import os
import re
import datetime
import random
from api.obs_api import *
import re
import csv
import time
from elasticsearch import Elasticsearch
import yaml
import logging
import logging.handlers

#from pathos.multiprocessing import ProcessingPool as Pool
from multiprocess import Manager
import multiprocessing
from multiprocessing import  Value, cpu_count,Process,Lock,Array
from ctypes import c_wchar,c_int,c_ulonglong

LOG_FILENAME="datagenerate_log.out"
my_logger=logging.getLogger("mLog")
my_logger.setLevel(logging.INFO)
hander=logging.handlers.RotatingFileHandler(LOG_FILENAME,maxBytes=20480000,backupCount=10)
my_logger.addHandler(hander)
id_init=""


@registry.register_problem
class AsrEn(librispeech.Librispeech):

    with open("config.yaml", 'r') as stream:
        try:
            es_address=yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            my_logger.error("Time: {} Message: connecting elasticsearch error {}".format(str(datetime.datetime.now()),exc))
    #print(es_address)
    train_sources=es_address['train_sources'].split(",")
    eval_sources=es_address['eval_sources'].split(",")
    test_sources=es_address['test_sources'].split(",")
    es_address=es_address['es_address'].split(",")
    es=Elasticsearch(es_address)
    index="asr-en"
    _body={
        "size":0,
        "aggs": {
            "patterns": {
            "terms": {
                "field": "source",
                "size": 100
                }}}}
    _result=es.search(index=index,body=_body)
    source_summary=_result["aggregations"]["patterns"]["buckets"]
    sources=[]
    for _item in source_summary:
        sources.append(_item['key'])
    #current_source=Value(c_wchar_p,"")

    #set multiple process control variables
    process_num=min(10,int(cpu_count()-2))

    
    def generator(self, data_dir, tmp_dir, datasets,lock,shared_dict,how_many=0):
        encoders = self.feature_encoders(data_dir)
        audio_encoder = encoders["waveforms"]
        text_encoder = encoders["targets"]
        while(True):
            lock.acquire()

            my_logger.info("Time:{} process {} get lock".format(str(datetime.datetime.now()),multiprocessing.current_process()))
            my_logger.info("{} before search:{},{},{}".format(multiprocessing.current_process(),shared_dict["source_index"],shared_dict["current_id"],shared_dict["current_last_updated"]))

            
            if shared_dict["current_id"]==id_init:
                if shared_dict["source_index"]>=len(datasets):
                    my_logger.info("Time: {} Process {} No more source to contiue. Last source we tried is {}.".format(
                        str(datetime.datetime.now()),multiprocessing.current_process(),datasets[shared_dict["source_index"]-1]))
                    lock.release()
                    return
                else:
                    try:
                        source=datasets[shared_dict["source_index"]]
                        body={
                            "query": {
                                "bool": {
                                "must": [
                                    {
                                    "term": {
                                        "source": source
                                    }
                                    }
                                ],
                                "must_not": [],
                                "should": []
                                }
                            },
                            "from": 0,
                            "size": 10,
                            "sort": [
                                {
                                "last_updated": "asc"
                                },
                                {
                                "_id": "asc"
                                }
                            ],
                            "aggs": {}
                            }
                        my_logger.info("Time: {} Message: {} converting source {}".format(str(datetime.datetime.now()),multiprocessing.current_process(),source))

                        try:
                            results=self.es.search(index=self.index,body=body)
                            
                            if len(results['hits']['hits'])>0:
                                shared_dict["current_last_updated"]=results['hits']['hits'][-1]['sort'][0]
                                shared_dict["current_id"]=results['hits']['hits'][-1]['sort'][1]
                                print(self.es.info)
                                hits=results['hits']['hits']
                                my_logger.info("change status after first search")
                                my_logger.info("{} status changed to: {},{},{}".format(multiprocessing.current_process(),shared_dict["source_index"],shared_dict["current_id"],shared_dict["current_last_updated"]))
                                my_logger.info("verify against: {},{}".format(results['hits']['hits'][-1]['sort'][0],results['hits']['hits'][-1]['sort'][1]))
                            else:
                                shared_dict["current_id"]=id_init
                                shared_dict["source_index"]=shared_dict["source_index"]+1
                                my_logger.info("No more data from this source {} status changed to: {},{},{}".format(
                                    multiprocessing.current_process(),shared_dict["source_index"],shared_dict["current_id"],shared_dict["current_last_updated"]))
                                lock.release()
                                continue
                        except Exception as e:
                            my_logger.error("Time: {} Error: Elasticsearch failed due to error: {} and search body is {}".format(str(datetime.datetime.now()),e,body))
                            shared_dict["current_id"]=id_init
                            shared_dict["source_index"]=shared_dict["source_index"]+1
                            my_logger.info("No more data from this source {} status changed to: {},{},{}".format(
                                multiprocessing.current_process(),shared_dict["source_index"],shared_dict["current_id"],shared_dict["current_last_updated"]))
                            lock.release()
                            continue
                    except Exception as e:
                        my_logger.info("Time: {} Message: 01 {} No more source to contiue. Last source we tried is {}. Error {} occur".format(
                            str(datetime.datetime.now()),multiprocessing.current_process(),datasets[shared_dict["source_index"]-1],e))
                        lock.release()
                        return
            else:
                
                if shared_dict["source_index"]>=len(datasets):
                    my_logger.info("Time: {} Process {} No more source to contiue. Last source we tried is {}.".format(
                        str(datetime.datetime.now()),multiprocessing.current_process(),datasets[shared_dict["source_index"]-1]))
                    lock.release()
                    return
                else:
                    try:

                        my_logger.info("Time: {} Message: {} continue search: {},{},{},{}".format(
                            str(datetime.datetime.now()),multiprocessing.current_process(),shared_dict["source_index"],shared_dict["current_id"],shared_dict["current_last_updated"],shared_dict["record_num"]))
                        source=datasets[shared_dict["source_index"]]
                        breaker=[shared_dict["current_last_updated"],shared_dict["current_id"]]
                        body={
                            "query": {
                                "bool": {
                                "must": [
                                    {
                                    "term": {
                                        "source": source
                                    }
                                    }
                                ],
                                "must_not": [],
                                "should": []
                                }
                            },
                            "from": 0,
                            "size": 1000,
                            "search_after":breaker,
                            "sort": [
                                {
                                "last_updated": "asc"
                                },
                                {
                                "_id": "asc"
                                }
                            ],
                            "aggs": {}
                            }
                        my_logger.info("Time: {} Message: {} continue after {}".format(str(datetime.datetime.now()),multiprocessing.current_process(),breaker))
                        try:
                            results=self.es.search(index=self.index,body=body)
                            if len(results['hits']['hits'])>0:
                                shared_dict["current_last_updated"]=results['hits']['hits'][-1]['sort'][0]
                                shared_dict["current_id"]=results['hits']['hits'][-1]['sort'][1]
                                hits=results['hits']['hits']
                                my_logger.info("continue to change status")
                                my_logger.info("{} status changed to: {},{},{}".format(
                                    multiprocessing.current_process(),shared_dict["source_index"],shared_dict["current_id"],shared_dict["current_last_updated"]))
                                my_logger.info("verify against: {},{}".format(results['hits']['hits'][-1]['sort'][0],results['hits']['hits'][-1]['sort'][1]))
                            else:
                                shared_dict["current_id"]=id_init
                                shared_dict["source_index"]=shared_dict["source_index"]+1
                                my_logger.info("No more data from this source {} status changed to: {},{},{}".format(
                                    multiprocessing.current_process(),shared_dict["source_index"],shared_dict["current_id"],shared_dict["current_last_updated"]))
                                lock.release()
                                continue
                        except Exception as e:
                            my_logger.error("Time: {} Error: Elasticsearch failed due to error: {} and search body is {}, process {}".format(
                                str(datetime.datetime.now()),e,body,multiprocessing.current_process()))
                            shared_dict["current_id"]=id_init
                            shared_dict["source_index"]=shared_dict["source_index"]+1
                            my_logger.info("No more data from this source {} status changed to: {},{},{}".format(
                                    multiprocessing.current_process(),shared_dict["source_index"],shared_dict["current_id"],shared_dict["current_last_updated"]))
                            lock.release()
                            continue  
                    except Exception as e:
                        my_logger.info("Time: {} Process {} No more source to contiue. Last source we tried is {}. Error occured is {}".format(
                            str(datetime.datetime.now()),multiprocessing.current_process(),datasets[shared_dict["source_index"]-1],e))
                        lock.release()
                        return
            
            my_logger.info("Time: {}, process {} release lock".format(str(datetime.datetime.now()),multiprocessing.current_process()))
            lock.release()

            for hit in hits:
                my_logger.info("how_many is {} num of file is {},{}".format(how_many,shared_dict["record_num"],shared_dict["record_num"] >= how_many))
                if how_many > 0 and shared_dict["record_num"] >= how_many:
                    my_logger.info("end process: {}".format(multiprocessing.current_process()))
                    return
                shared_dict["record_num"] = shared_dict["record_num"]+1
                
                filename=hit['_source']["filename"]
                my_logger.info("Time: {} {} process {} files {}".format(
                    str(datetime.datetime.now()),multiprocessing.current_process(),shared_dict["record_num"],filename))
                obs_path=os.path.join("processed",filename)
                file_id=hit["_id"]
                local_path=os.path.join(tmp_dir,file_id+".wav")
                for i in range(5):
                    try:                        
                        download_file(obs_path,local_path)
                        break
                    except Exception as e:
                        my_logger.error("Time: {} Error: {} download {} failed {}".format(str(datetime.datetime.now()),multiprocessing.current_process(),obs_path,e))
                        time.sleep(3)
                        continue
                try:
                    wav_data = audio_encoder.encode(local_path)
                    text_data=" ".join(hit['_source']['words'])
                    os.remove(local_path)
                except Exception as e:
                    my_logger.error("Time: {} Error: {} encode error as {}".format(str(datetime.datetime.now()),multiprocessing.current_process(),e))
                    continue
                ##########################################
                #print("wavelengh:{} and filename {}".format(len(wav_data),local_path))
                yield {
                    "waveforms": wav_data,
                    "waveform_lens": [len(wav_data)],
                    "targets": text_encoder.encode(text_data),
                    "raw_transcript": [text_data],
                    "utt_id": [file_id]
                    }


    def generate_data(self, data_dir, tmp_dir,task_id=-1):
        test=20
        train_paths = self.training_filepaths(
            data_dir, self.num_shards, shuffled=False)
        dev_paths = self.dev_filepaths(
            data_dir, self.num_dev_shards, shuffled=False)
        test_paths = self.test_filepaths(
            data_dir, self.num_test_shards, shuffled=True)
        try_num=0
        if test:
            try_num=test
        manager=Manager()
        lock=manager.Lock()

        #shared_dict=manager.dict({'current_id':id_init,"current_last_updated":0,"record_num":0,"source_index":0})
        def process_files(train_paths,datasets,num_run,shared_dict):

            total_file_num=len(train_paths)
            num_per_partition=int(math.floor(total_file_num/num_run))
            train_paths_list=[]
            for i in range(num_run):
                if i==num_run-1:
                    train_paths_list.append(train_paths[i*num_per_partition:])
                else:
                    train_paths_list.append(train_paths[i*num_per_partition:(i+1)*num_per_partition])
            generator_list=[]
            for i in range(num_run):
                generator_list.append(self.generator(data_dir, tmp_dir, datasets,lock,shared_dict,how_many=try_num))
            
            p=[]
            for i in range(num_run):
                p.append(Process(target=generator_utils.generate_files,args=(generator_list[i],train_paths_list[i],try_num)))
                p[i].start()
            my_logger.error("Time: {} All processes started".format(str(datetime.datetime.now())))
            for q in p:
                q.join()
            my_logger.error("Time: {} All processes ended".format(str(datetime.datetime.now())))
        shared_dict=manager.dict({'current_id':id_init,"current_last_updated":0,"record_num":0,"source_index":0})
        num_run=min(self.process_num,self.num_shards)
        process_files(train_paths,self.train_sources,num_run,shared_dict)
        if len(self.eval_sources)==0:
            generator_utils.shuffle_dataset(train_paths)

        else:
            shared_dict["current_id"]=id_init
            shared_dict["current_last_updated"]=0
            shared_dict["record_num"]=0
            shared_dict["source_index"]=0
            num_run=min(self.process_num,self.num_dev_shards)
            my_logger.error("Time: {} process dev dataset".format(str(datetime.datetime.now())))
            process_files(dev_paths,self.eval_sources,num_run,shared_dict)
            my_logger.error("Time: {} shuffle dataset".format(str(datetime.datetime.now())))
            generator_utils.shuffle_dataset(train_paths+dev_paths)
        shared_dict["current_id"]=id_init
        shared_dict["current_last_updated"]=0
        shared_dict["record_num"]=0
        shared_dict["source_index"]=0
        num_run=min(self.process_num,self.num_test_shards)
        process_files(test_paths,self.test_sources,num_run,shared_dict)
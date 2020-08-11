#%%
"""
This file implements methods for calculating the WER between a ground-truth
sentence and a hypothesis sentence, commonly a measure of performance for a
automatic speech recognition system
"""
#%%
import os
# #####################
# dir_path = os.path.dirname(os.path.realpath(__file__))

# os.chdir(dir_path)

#%%
# pwd()
#%%################################
import re
import absl.flags
from absl import app
FLAGS = absl.flags.FLAGS
import numpy as np
import csv
import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plot
from typing import Union, List, Tuple
from itertools import chain
from random import shuffle
from elasticsearch import Elasticsearch,helpers
import yaml
import os
from collections import Counter
from multiprocess import Manager
import multiprocessing
from multiprocessing import  Value, cpu_count,Process,Lock,Array
from itertools import compress
from util.logging import My_logger
from datetime import datetime
import smtplib
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate

f = absl.flags
#f.DEFINE_string('logfile_name', 'postprocess_log.out', 'log file name')
f.DEFINE_string('configfile_name', 'config_in.yaml', 'config file name')
f.DEFINE_string('output_dir', 'output', 'config file name')
f.DEFINE_boolean('post_process', True, 'whether to do post process')
f.DEFINE_string('email_config', 'email.yaml', 'files contains all emails addresses (management [] and individual users)')
f.DEFINE_string('email_address', 'oneconnectrnd@gmail.com', 'files contains all emails addresses (management [] and individual users)')
f.DEFINE_string('email_password', 'qtzivbuytuqptupq', 'files contains all emails addresses (management [] and individual users)')

#%%
################################################################################
# Implementation of the WER method, exposed publicly

my_logger=My_logger(filename='postprocess_log.out')
def wer(truth: Union[str, List[str], List[List[str]]],
        hypothesis: Union[str, List[str], List[List[str]]],
        standardize=False,
        words_to_filter=None
        ) -> float:
    """
    Calculate the WER between a ground-truth string and a hypothesis string
    :param truth the ground-truth sentence as a string or list of words
    :param hypothesis the hypothesis sentence as a string or list of words
    :param standardize whether to apply some standard rules to the given string
    :param words_to_filter a list of words to remove from the sentences
    :return: the WER, the distance (also known as the amount of
    substitutions, insertions and deletions) and the length of the ground truth
    """
    truth = _preprocess(truth, standardize=standardize, words_to_remove=words_to_filter)
    hypothesis = _preprocess(hypothesis, standardize=standardize, words_to_remove=words_to_filter)

    if len(truth) == 0:
        raise ValueError("truth needs to be a non-empty list of string")

    # Create the list of vocabulary used
    vocab = list()

    for w in chain(truth, hypothesis):
        if w not in vocab:
            vocab.append(w)

    # recreate the truth and hypothesis string as a list of tokens
    t = []
    h = []

    for w in truth:
        t.append(vocab.index(w))

    for w in hypothesis:
        h.append(vocab.index(w))

    # now that the words are tokenized, we can do alignment
    distance,path = _edit_distance(t, h)

    # and the WER is simply distance divided by the length of the truth
    n = len(truth)
    error_rate = distance / n

    return distance,path,error_rate

################################################################################
# Implementation of helper methods, private to this package


# _common_words_to_remove = ["yeah", "so", "oh", "ooh", "yhe"]


def _preprocess(text: Union[str, List[str], List[List[str]]],
                standardize:bool = False,
                words_to_remove=None):
    """
    Preprocess the input, be it a string, list of strings, or list of list of
    strings, such that the output is a list of strings.
    :param text:
    :return:
    """
    if isinstance(text, str):
        return _preprocess_text(text,
                                standardize=standardize,
                                words_to_remove=words_to_remove)
    elif len(text) == 0:
        raise ValueError("received empty list")
    elif len(text) == 1:
        return _preprocess(text[0])
    elif all(isinstance(e, str) for e in text):
        return _preprocess_text(" ".join(text), standardize=standardize,
                                words_to_remove=words_to_remove)
    elif all(isinstance(e, list) for e in text):
        for e in text:
            if not all(isinstance(f, str) for f in e):
                raise ValueError("The second list needs to only contain "
                                 "strings")
        return _preprocess_text("".join(["".join(e) for e in text]),
                                        standardize = standardize,
                                        words_to_remove=words_to_remove)
    else:
        raise ValueError("given list should only contain lists or list of "
                         "strings")


def _preprocess_text(phrase: str,
                     standardize: bool = False,
                     words_to_remove: List[str] = None)\
        -> List[str]:
    """
    Applies the following preprocessing steps on a string of text (a sentence):
    * optionally expands common abbreviated words such as he's into he is, you're into
    you are, ect
    * makes everything lowercase
    * tokenize words
    * optionally remove common words such as "yeah", "so"
    * change all numbers written as one, two, ... to 1, 2, ...
    * remove strings between [] and <>, such as [laughter] and <unk>
    :param s: the string, which is a sentence
    :param standardize: standardize the string by removing common abbreviations
    :param words_to_remove: remove words in this list from the phrase
    :return: the processed string
    """
    if type(phrase) is not str:
        raise ValueError("can only preprocess a string type, got {} of type {}"
                         .format(phrase, type(phrase)))

    # lowercase
    phrase = phrase.lower()

    # deal with abbreviated words
    if standardize:
        phrase = _standardise(phrase)

    # remove words between [] and <>
    phrase = re.sub('[<\[](\w)*[>\]]', "", phrase)

    # remove redundant white space
    phrase = phrase.strip()
    phrase = phrase.replace("\n", "")
    phrase = phrase.replace("\t", "")
    phrase = phrase.replace("\r", "")
    phrase = phrase.replace(",", "")
    phrase = phrase.replace(".", "")
    phrase = re.sub("\s\s+", " ", phrase)  # remove more than one space between words

    # tokenize
    phrase = phrase.split(" ")

    # remove common stop words (from observation):
    if words_to_remove is not None:
        for word_to_remove in words_to_remove:
            if word_to_remove in phrase:
                phrase.remove(word_to_remove)

    return phrase


def _standardise(phrase: str):
    """
    Standardise a phrase by removing common abbreviations from a sentence
    as well as making everything lowercase
    :param phrase: the sentence
    :return: the sentence with common stuff removed
    """
    # lowercase
    if not phrase.islower():
        phrase = phrase.lower()

    # specific
    phrase = re.sub(r"won't", "will not", phrase)
    phrase = re.sub(r"can\'t", "can not", phrase)
    phrase = re.sub(r"let\'s", "let us",  phrase)

    # general
    phrase = re.sub(r"n\'t", " not", phrase)
    phrase = re.sub(r"\'re", " are", phrase)
    phrase = re.sub(r"\'s", " is", phrase)
    phrase = re.sub(r"\'d", " would", phrase)
    phrase = re.sub(r"\'ll", " will", phrase)
    phrase = re.sub(r"\'t", " not", phrase)
    phrase = re.sub(r"\'ve", " have", phrase)
    phrase = re.sub(r"\'m", " am", phrase)

    return phrase


def _edit_distance(a: List[int], b:List[int]) -> int:
    """
    Calculate the edit distance between two lists of integers according to the
    Wagner-Fisher algorithm. Reference:
    https://en.wikipedia.org/wiki/Wagner%E2%80%93Fischer_algorithm)
    :param a: the list of integers representing a string, where each integer is
    a single character or word
    :param b: the list of integers representing the string to compare distance
    with
    :return: the calculated distance
    """
    if len(a) == 0:
        raise ValueError("the reference string (called a) cannot be empty!")
    elif len(b) == 0:
        return len(a)

    # Initialize the matrix/table and set the first row and column equal to
    # 1, 2, 3, ...
    # Each column represent a single token in the reference string a
    # Each row represent a single token in the reference string b
    #
    m = np.zeros((len(b) + 1, len(a) + 1)).astype(dtype=np.int32)
    
    m[0, 1:] = np.arange(1, len(a) + 1)
    m[1:, 0] = np.arange(1, len(b) + 1)
    path_array=m.copy()

    # Now loop over remaining cell (from the second row and column onwards)
    # The value of each selected cell is:
    #
    #   if token represented by row == token represented by column:
    #       value of the top-left diagonal cell
    #   else:
    #       calculate 3 values:
    #            * top-left diagonal cell + 1 (which represents substitution)
    #            * left cell + 1 (representing deleting)
    #            * top cell + 1 (representing insertion)
    #       value of the smallest of the three
    #
    
    for i in range(1, m.shape[0]):
        for j in range(1, m.shape[1]):
            if a[j-1] == b[i-1]:
                m[i, j] = m[i-1, j-1]
                path_array[i,j]=0
            else:
                m[i, j] = min(
                    m[i-1, j-1] + 1, #replace:1
                    m[i, j - 1] + 1, #insertion:2
                    m[i - 1, j] + 1 #deletion:3
                )
                path_array[i,j]=np.argmin([m[i-1, j-1],m[i, j - 1],m[i - 1, j]])+1
    path=[]
    a_index=j
    b_index=i
    while(a_index or b_index):
        if b_index==0:
            path.append(2)
            a_index-=1
            continue
        if a_index==0:
            path.append(3)
            b_index-=1
            continue
        if path_array[b_index,a_index]==0 or path_array[b_index,a_index]==1:
            path.append(path_array[b_index,a_index])
            a_index-=1
            b_index-=1
            continue
        if path_array[b_index,a_index]==2:
            path.append(path_array[b_index,a_index])
            a_index-=1
            continue
        if path_array[b_index,a_index]==3:
            path.append(path_array[b_index,a_index])
            b_index-=1
            continue
    path=path[::-1]



    # and the minimum-edit distance is simply the value of the down-right most
    # cell

    return m[len(b), len(a)],path

#%%
def calculate_final_transcript(strings,thread=3):
    score_1,path_1,error_rate_1=wer(strings[0],strings[1])
    score_2,path_2,error_rate_2=wer(strings[0],strings[2])
    score_3,path_3,error_rate_3=wer(strings[1],strings[2])
    #correctness =1 correct, 2 wrong, 3 unknown
    correctness=[3,3,3]

    if score_1<=thread or score_2<=thread or error_rate_1<=0.5 or error_rate_2<=0.5:
        correctness[0]=1
    else:correctness[0]=2
    if score_1<=thread or score_3<=thread or error_rate_1<=0.5 or error_rate_3<=0.5:
        correctness[1]=1
    else: correctness[1]=2
    if score_2<=thread or score_3<=thread or error_rate_2<=0.5 or error_rate_3<=0.5:
        correctness[2]=1
    else: correctness[2]=2

    if correctness[0]==1 or correctness[1]==1 or correctness[2]==1:
        min_score=min(score_1,score_2,score_3)
        min_score_num=np.argmin([score_1,score_2,score_3])
        path_list=[path_1,path_2,path_3]
        best_path=path_list[min_score_num]
        if min_score_num==0:
            string_a=strings[0]
            string_b=strings[1]
            string_c=strings[2]
        elif min_score_num==1:
            string_a=strings[0]
            string_b=strings[2]
            string_c=strings[1]
        else:
            string_a=strings[1]
            string_b=strings[2]
            string_c=strings[0]

        element_a=string_a.lower().split(" ")
        element_b=string_b.lower().split(" ")
        element_c=string_c.lower().split(" ")
        def best_guess(potential_list):
            if len(potential_list)==1:
                if potential_list[0] in element_c:
                    return potential_list[0]
                else: return None
            for item in potential_list:
                if item in element_c:
                    return item
            shuffle(potential_list)
            return potential_list[0]

        transcript=[]
        a_index=0
        b_index=0
        a_join_short=False
        join_short=False
        for i, element in enumerate(best_path):
            
            if element==0:
                
                transcript.append(element_a[a_index])
                a_index+=1
                b_index+=1
                join_short=False
                a_join_short=False    
                continue
            elif element==1:
                potential_list=[element_a[a_index],element_b[b_index]]
                potential=best_guess(potential_list)
                if join_short:
                    joined_text="".join([join_pre,element_b[b_index]])
                    if joined_text==element_a[a_index] or (joined_text in element_c):
                        potential=joined_text
                if a_join_short:
                    a_joined_text="".join([a_join_pre,element_a[a_index]])
                    if a_joined_text==element_b[b_index] or (a_joined_text in element_c):
                        potential=a_joined_text
                transcript.append(potential)
                a_index+=1
                b_index+=1
                a_join_short=False
                join_short=False
            elif element==2:
                potential_result=best_guess([element_a[a_index]])
                if potential_result:
                    transcript.append(potential_result)
                    a_join_short=False
                else:
                    a_join_pre=element_a[a_index]
                    a_join_short=True
                a_index+=1
                
                join_short=False
            else:
                potential_result=best_guess([element_b[b_index]])
                if potential_result:
                    transcript.append(potential_result)
                    join_short=False
                else: 
                    join_pre=element_b[b_index]
                    join_short=True
                b_index+=1
                a_join_short=False
        transcript=" ".join(transcript)
        re_score,re_path,error_rate=wer(transcript,element_c)
        if re_score<=thread:
            correctness[2-min_score_num]=1
        return transcript,correctness
    else: return None, [3,3,3]



#%%
class ES(object):
    def __init__(self,config_file):
        self.config_file=config_file
        with open(self.config_file, 'r') as stream:
            try:
                es_address=yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        self.es_address=es_address['es_address'].split(',')
        self.es=Elasticsearch(self.es_address)
        self.index=es_address['index']


#%%
def _generator(lock,shared_dict,es):

    while(True):
        lock.acquire()
        breaker=[shared_dict['time'],shared_dict['id']]
        body= {
                "_source":['transcript','final_flag'],
                "query": {
                "bool": {
                "must": [
                    {
                    "has_child": {
                        "type": "verified",
                        
                        "query": {
                            'bool':{
                                'must':[{
                                    'match':{
                                        'correctness':3
                                    }
                                
                                }]
                            }

                        },
                        "min_children": 3,
                        "inner_hits": {
                            '_source':['result','transcript','correctness']
                        }  
                    }
                    }
                ]
                }
            },
            'from':0,
            'size':1000,
            'search_after':breaker,
            'sort':[
                {
                    "last_updated":"asc"
                },
                {
                '_id':"asc"
                }
            ]
            }

        result_1=es.es.search(index=es.index,body=body)
        hits=result_1['hits']['hits']
        if len(hits)==0:
            lock.release()
            break
        shared_dict['time']=hits[-1]['sort'][0]
        shared_dict['id']=hits[-1]['sort'][1]
        lock.release()

        for hit in hits:
            now=datetime.now()
            #yield parent id, parent transcript,child_1 id, childrens
            message='Time: {} Before process process number is {} and message is \n {}'.format(now,multiprocessing.current_process(),hit)
            my_logger.log_debug(message)
            yield hit['_id'],hit['_source']['transcript'],hit['inner_hits']['verified']['hits']['hits'][:3]
            
            #hit['inner_hits']['hits'][1]['_id'],hit['inner_hits']['hits'][1]['_source'],
            #hit['inner_hits']['hits'][2]['_id'],hit['inner_hits']['hits'][2]['_source'],

#%%

#%%
def _process_unknown_record(generator):
    thread=4
    es = ES(FLAGS.configfile_name)
    for parent_id,parent_transcript,children in generator:
        body= {
                "_source":['transcript','final_flag','final_transcript'],
                "query": {
                "bool": {
                "must": [
                    {
                        'match':{
                            "_id":parent_id
                        }},
                    {
                    "has_child": {
                        "type": "verified",
                        'query':{
                            'bool':{
                                'must':[
                                    {
                                        'match_all':{}
                                    }
                                ]
                            }

                        },
                        
                        "inner_hits": {
                            '_source':['result','transcript','correctness']
                        }  
                    }
                    }
                    
                    
                ]
                }
            },
            'from':0,
            'size':3,

            }
        transcripts=[]
        ids=[]
        results=[]
        for child in children:
            ids.append(child['_id'])
            results.append(child['_source']['result'])
            if child['_source']['result']=='M':
                transcripts.append(child['_source']['transcript'])
            elif child['_source']['result']=='A':
                transcripts.append(parent_transcript)
            else:
                transcripts.append(None)
        message='During process  {} ids are {}. results are {}. transcripts are {}'.format(multiprocessing.current_process(),ids,results,transcripts)
        my_logger.log_debug(message)
        result_dict=Counter(results)
        if result_dict['M']==1:
            message='During process  {} only 1 M in results'.format(multiprocessing.current_process())
            my_logger.log_debug(message)
            if result_dict['R']==1:
                message='During process  {} only 1 M and 1 R in results'.format(multiprocessing.current_process())
                my_logger.log_debug(message)
                sub_transcripts=[]
                sub_ids=[]
                for i,sub_result in enumerate(results):
                    if sub_result=="M" or sub_result=="A":
                        sub_transcripts.append(transcripts[i])
                        sub_ids.append(ids[i])
                    else:
                        reject_id=ids[i]
                score,path,error_rate=wer(sub_transcripts[0],sub_transcripts[1])
                message='During process  {} score is {} error rate is {}'.format(multiprocessing.current_process(),score,error_rate)
                my_logger.log_debug(message)
                if score<=thread:
                    shuffle(sub_transcripts)
                    final_transcript=sub_transcripts[0]
                    es.es.update(index=es.index,id=reject_id,doc_type='_doc',routing=parent_id,body={'doc':{'correctness':2}})
                    es.es.update(index=es.index,id=sub_ids[0],doc_type='_doc',routing=parent_id,body={'doc':{'correctness':1}})
                    es.es.update(index=es.index,id=sub_ids[1],doc_type='_doc',routing=parent_id,body={'doc':{'correctness':1}})
                    es.es.update(index=es.index,id=parent_id,doc_type='_doc',refresh=True,body={'doc':{'final_flag':1,'final_transcript':final_transcript}})
                    message='During process  {} update database'.format(multiprocessing.current_process())
                    my_logger.log_debug(message)
                temp_result=es.es.search(index=es.index,body=body)
                temp_hits=temp_result['hits']['hits']
                message='After process  {} result is \n {}'.format(multiprocessing.current_process(),temp_hits)
                my_logger.log_debug(message)
            continue
        if result_dict['M']==2:
            message='During process  {} 2 Ms in result'.format(multiprocessing.current_process())
            my_logger.log_debug(message)
            if "R" in results:
                message='During process  {} 2 Ms and 1 R in result'.format(multiprocessing.current_process())
                my_logger.log_debug(message)
                sub_transcripts=[]
                sub_ids=[]
                for i,sub_result in enumerate(results):
                    if sub_result=="M":
                        sub_transcripts.append(transcripts[i])
                        sub_ids.append(ids[i])
                    else: reject_id=ids[i]
                score,path,error_rate=wer(sub_transcripts[0],sub_transcripts[1])
                message='During process  {} score is {} error rate is {}'.format(multiprocessing.current_process(),score,error_rate)
                my_logger.log_debug(message)
                if score<=thread:
                    shuffle(sub_transcripts)
                    final_transcript=sub_transcripts[0]
                    es.es.update(index=es.index,id=reject_id,doc_type='_doc',routing=parent_id,body={'doc':{'correctness':2}})
                    es.es.update(index=es.index,id=sub_ids[0],doc_type='_doc',routing=parent_id,body={'doc':{'correctness':1}})
                    es.es.update(index=es.index,id=sub_ids[1],doc_type='_doc',routing=parent_id,body={'doc':{'correctness':1}})
                    es.es.update(index=es.index,id=parent_id,doc_type='_doc',refresh=True,body={'doc':{'final_flag':1,'final_transcript':final_transcript}})
                    message='During process  {} update database'.format(multiprocessing.current_process())
                    my_logger.log_debug(message)
                temp_result=es.es.search(index=es.index,body=body)
                temp_hits=temp_result['hits']['hits']
                message='After process {} result is \n {}'.format(multiprocessing.current_process(),temp_hits)
                my_logger.log_debug(message)
                continue
        if result_dict['M']==3 or (result_dict['M']==2 and result_dict['A']==1):
            message='During process  {} 3 Ms or 2 Ms and 1 A in result'.format(multiprocessing.current_process())
            my_logger.log_debug(message)
            assert None not in transcripts
            final_transcript,correctnesses=calculate_final_transcript(transcripts,thread)
            message='During process  {} final_transcript is {} and correctness is {}'.format(multiprocessing.current_process(),final_transcript,correctnesses)
            my_logger.log_debug(message)
            if final_transcript:
                es.es.update(index=es.index,id=parent_id,doc_type='_doc',body={'doc':{'final_flag':1,'final_transcript':final_transcript}})
                es.es.update(index=es.index,id=ids[0],doc_type='_doc',routing=parent_id,body={'doc':{'correctness':correctnesses[0]}})
                es.es.update(index=es.index,id=ids[1],doc_type='_doc',routing=parent_id,body={'doc':{'correctness':correctnesses[1]}})
                es.es.update(index=es.index,id=ids[2],doc_type='_doc',routing=parent_id,refresh=True,body={'doc':{'correctness':correctnesses[2]}})
                message='During process  {} update database'.format(multiprocessing.current_process())
                my_logger.log_debug(message)
                temp_result=es.es.search(index=es.index,body=body)
                temp_hits=temp_result['hits']['hits']
                message='After process {} result is \n {}'.format(multiprocessing.current_process,temp_hits)
                my_logger.log_debug(message)



#%%
def run_post_process():
    es = ES(FLAGS.configfile_name)
    manager=Manager()
    lock=manager.Lock()
    shared_dict=manager.dict({'time':0,"id":""})
    process_num=int(cpu_count()-2)

    generator_list=[]
    for i in range(process_num):
        generator_list.append(_generator(lock,shared_dict,es))

    #%%
    p=[]
    for i in range(process_num):
        p.append(Process(target=_process_unknown_record,args=(generator_list[i],)))
        p[i].start()

    for q in p:
        q.join()
#%%generate reports
def generate_reports():
    user_report_names={'management':[]}
    dir=FLAGS.output_dir
    es = ES(FLAGS.configfile_name)
    index=es.index
    breaker=[0,'']
    table_dict=[]
    while(True):
        body={
            "query": {
                "bool": {
                "must": [
                    {
                    "match": {
                        "type": "verified"
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

        result_1=es.es.search(index=index,body=body)

        if len(result_1['hits']['hits'])==0:
            break
        breaker=[result_1['hits']['hits'][-1]['sort'][0],result_1['hits']['hits'][-1]['sort'][1]]

        for dict in result_1['hits']['hits']:
            new_dict=dict['_source']
            new_dict['_id']=dict['_id']
            new_dict['joined_id']=dict['_source']['join_field']['parent']
            table_dict.append(new_dict)
    result_df_1=pd.DataFrame.from_dict(table_dict)
    transcript_file=os.path.join(dir,"verified.csv")
    result_df_1.to_csv(transcript_file)
    result_df_1["date"]=pd.to_datetime(result_df_1["last_updated"])
    result_df_1["last_updated"]=pd.to_datetime(result_df_1["last_updated"])
    result_df_1=result_df_1.sort_values(by='date')
    def get_groups(value):
        today_time=pd.Timestamp.now()
        if value.week == today_time.week:
            result=value.strftime('%Y-%m-%d')
        elif value.month == today_time.month:
            result='W'+str(value.week)
        elif value.year == today_time.year:
            result=value.month_name()[0:3]
        else: result='Y'+str(value.year)
        return result
    def is_thismonth(value):
        today_time=pd.Timestamp.now()
        if value.month == today_time.month:
            return True
        else:
            return False
    result_df_1['thismonth']=result_df_1.date.apply(is_thismonth)
    result_df_1["date"]=result_df_1.date.apply(get_groups)
    
    #final table as result_df_1
    total_count_summary=result_df_1.groupby(["date","username"]).count()["_id"]
    total_count_summary=pd.DataFrame({"count":total_count_summary}).reset_index()
    user_input_result_summary=result_df_1.pivot_table(index=['date','username'],
                            columns=['result'],aggfunc='size',fill_value=0)
    user_input_result_summary=user_input_result_summary.reset_index()
    user_input_result_summary['total']=user_input_result_summary['A']+user_input_result_summary['M']+user_input_result_summary['R']
    user_input_result_summary['Modify_rate']=user_input_result_summary['M']/user_input_result_summary['total']
    user_input_result_summary['Reject_rate']=user_input_result_summary['R']/user_input_result_summary['total']
    user_input_result_summary['Accept_rate']=user_input_result_summary["A"]/user_input_result_summary['total']
    user_input_result_summary=user_input_result_summary[['date','username','Modify_rate','Reject_rate','Accept_rate','total']].round(2)
    result_df_1.correctness.fillna(4,inplace=True)
    result_df_1.to_csv(os.path.join(dir,'result_df_1.csv'))
    user_input_result_summary.to_csv(os.path.join(dir,'user_input_result_summary.csv'))
    user_result_summary=result_df_1
    user_result_summary=user_result_summary.pivot_table(index=['date','username'],
                                columns=['correctness'],aggfunc='size',fill_value=0)
    user_result_summary=user_result_summary.reset_index()
    user_result_summary['total']=user_result_summary.iloc[:,2]+user_result_summary.iloc[:,3]+user_result_summary.iloc[:,4]+user_result_summary.iloc[:,5]
    user_result_summary['Correct_rate']=user_result_summary.iloc[:,2]/user_result_summary['total']
    user_result_summary['Wrong_rate']=user_result_summary.iloc[:,3]/user_result_summary['total']
    user_result_summary['Unknown_rate']=user_result_summary.iloc[:,4]/user_result_summary['total']
    user_result_summary['pending_verification']=user_result_summary.iloc[:,5]/user_result_summary['total']
    user_result_summary=user_result_summary[['date','username','Correct_rate','Wrong_rate','Unknown_rate','pending_verification','total']].round(2)
    user_result_summary.to_csv(os.path.join(dir,'result_df_1.csv'))
    def box_group(ax,df,title=None):
        #df format, column 1: x_values, all the rest columns are values to plot
        # number of columns -1 is the group
        x_values=df.index
        groups=df.columns

        ind=np.arange(len(x_values))
        width=1/(len(groups)+1)
        p=[]
        for i,group in enumerate(groups):
            plot=ax.bar(ind+i*width,df.loc[:,group],width)
            p.append(plot[0])
        if title:
            ax.set_title(title)
        ax.set_xticks(ind+width/2)
        ax.set_xticklabels(x_values)
        ax.legend(tuple(p),tuple(groups))
        ax.autoscale_view()
        return ax
    fig, axs = plot.subplots(nrows=9, ncols=1,figsize=(10, 36))
    for i,value in enumerate(user_input_result_summary.columns[-4:][::-1]):
        draw_df=user_input_result_summary.pivot(index='date',
                                    columns='username',values=value)

        box_group(axs[i],draw_df,title=value)
    axs[4].axis("off")
    axs[4].table(cellText=total_count_summary.values,
                colLabels=total_count_summary.columns,
                cellLoc = 'right', rowLoc = 'center',
                loc='right', bbox=[0,0,1,1]
                )
    for j,value in enumerate(user_result_summary.columns[-4:-1]):
        draw_df=user_result_summary.pivot(index='date',
                                        columns='username',values=value)
        box_group(axs[j+5],draw_df,title=value)
    axs[8].axis("off")
    axs[8].table(cellText=user_result_summary.values,
                colLabels=user_result_summary.columns,
                cellLoc = 'right', rowLoc = 'center',
                loc='right', bbox=[0,0,1,1]
                )
    plot.subplots_adjust(top=0.97)
    
    #overall report
    figure_file_name=os.path.join(dir,"dailyreport-"+datetime.today().strftime('%Y-%m-%d')+".pdf")

    fig.savefig(figure_file_name,facecolor='w',edgecolor='w',transparent=False,format="pdf")
    user_report_names['management'].append(figure_file_name)
    del total_count_summary
    del user_input_result_summary
    del user_result_summary
    #generate overal matrix
    user_list=result_df_1['username'].unique()
    valid_user=np.delete(user_list,1)
    def check_valid(value):
        return value in valid_user
    def qualitycheck_group(value):
        return value//1000
    
    valid_data=result_df_1[result_df_1.username.apply(check_valid)]
    valid_data=valid_data.sort_values(by=['username','last_updated'])
    valid_data['sn']=range(valid_data['username'].count())
    valid_data['sn']=valid_data.sn.apply(qualitycheck_group)
    input_result=valid_data.pivot_table(index=['sn'],
                                columns=['result'],aggfunc='size',fill_value=0)
    input_result=input_result.reset_index()
    input_result['total']=input_result['A']+input_result['M']+input_result['R']
    input_result['Modify_rate']=input_result['M']/input_result['total']
    input_result['Reject_rate']=input_result['R']/input_result['total']
    input_result['Accept_rate']=input_result["A"]/input_result['total']
    input_result=input_result[['sn','Modify_rate','Reject_rate','Accept_rate','total']].round(2)

    valid_result=valid_data.pivot_table(index=['sn'],
                                columns=['correctness'],aggfunc='size',fill_value=0)
    valid_result=valid_result.reset_index()
    valid_result['total']=valid_result.iloc[:,1]+valid_result.iloc[:,2]+valid_result.iloc[:,3]
    valid_result['Correct_rate']=valid_result.iloc[:,1]/valid_result['total']
    valid_result['Wrong_rate']=valid_result.iloc[:,2]/valid_result['total']
    valid_result['Unknown_rate']=valid_result.iloc[:,3]/valid_result['total']
    valid_result=valid_result[['sn','Correct_rate','Wrong_rate','Unknown_rate','total']].round(2)

    input_quantile_result=input_result.quantile([0,0.01,0.05,0.1,0.5,0.9,0.95,0.99,1])
    input_quantile_result['quantile']=['0%','1%','5%','10%','50%','90%','95%','99%','100%']
    valid_quantile_result=valid_result.quantile([0,0.01,0.05,0.1,0.5,0.9,0.95,0.99,1])
    valid_quantile_result['quantile']=['0%','1%','5%','10%','50%','90%','95%','99%','100%']
    input_quantile_result.to_csv(os.path.join(dir,'user_input_matrix.csv'))
    valid_quantile_result.to_csv(os.path.join(dir,'user_result_matrix.csv'))
    del valid_data
    del input_result
    del valid_result
    del input_quantile_result
    del valid_quantile_result
    user_report_names['management'].append(os.path.join(dir,'user_input_matrix.csv'))
    user_report_names['management'].append(os.path.join(dir,'user_result_matrix.csv'))
    #generate per user report
    
    for user in user_list:
        user_table=result_df_1[result_df_1['username']==user]
        user_table_totaly=user_table.groupby(["date","username"]).count()["_id"]
        user_table_totaly=pd.DataFrame({"count":user_table_totaly}).reset_index()
        user_table_input=user_table.pivot_table(index=['date','username'],
                                columns=['result'],aggfunc='size',fill_value=0)
        user_table_input=user_table_input.reset_index()
        user_table_input['total']=user_table_input['A']+user_table_input['M']+user_table_input['R']
        user_table_input['Modify_rate']=user_table_input['M']/user_table_input['total']
        user_table_input['Reject_rate']=user_table_input['R']/user_table_input['total']
        user_table_input['Accept_rate']=user_table_input["A"]/user_table_input['total']
        user_table_input=user_table_input[['date','username','Modify_rate','Reject_rate','Accept_rate','total']].round(2)


        #%%

        user_table_result=user_table.pivot_table(index=['date','username'],
                                    columns=['correctness'],aggfunc='size',fill_value=0)
        user_table_result=user_table_result.reset_index()
        user_table_result['total']=user_table_result.iloc[:,2]+user_table_result.iloc[:,3]+user_table_result.iloc[:,4]
        user_table_result['Correct_rate']=user_table_result.iloc[:,2]/user_table_result['total']
        user_table_result['Wrong_rate']=user_table_result.iloc[:,3]/user_table_result['total']
        user_table_result['Unknown_rate']=user_table_result.iloc[:,4]/user_table_result['total']
        user_table_result=user_table_result[['date','username','Correct_rate','Wrong_rate','Unknown_rate','total']].round(2)
        # user_table=user_table[user_table['thismonth']==True]
        # user_table['sn']=range(user_table['username'].count())
        # user_table['sn']=user_table.sn.apply(qualitycheck_group)
        fig, axs = plot.subplots(nrows=9, ncols=1,figsize=(10, 45))
        for i,value in enumerate(user_table_input.columns[-4:][::-1]):
            draw_df=user_table_input.pivot(index='date',
                                        columns='username',values=value)

            box_group(axs[i],draw_df,title=value)
        axs[4].axis("off")
        axs[4].table(cellText=user_table_totaly.values,
                    colLabels=user_table_totaly.columns,
                    cellLoc = 'right', rowLoc = 'center',
                    loc='right', bbox=[0,0,1,1]
                    )
        for j,value in enumerate(user_table_result.columns[-4:-1]):
            draw_df=user_table_result.pivot(index='date',
                                            columns='username',values=value)
            box_group(axs[j+5],draw_df,title=value)
        axs[8].axis("off")
        axs[8].table(cellText=user_table_result.values,
                    colLabels=user_table_result.columns,
                    cellLoc = 'right', rowLoc = 'center',
                    loc='right', bbox=[0,0,1,1]
                )
        plot.subplots_adjust(top=0.97)

        figure_file_name=os.path.join(dir,"dailyreport-"+user+'-'+datetime.today().strftime('%Y-%m-%d')+".pdf")

        fig.savefig(figure_file_name,facecolor='w',edgecolor='w',transparent=False,format="pdf")
        plot.clf()
        user_report_names[user]=[figure_file_name]
    
    #email report
    with open(FLAGS.email_config, 'r') as stream:
        try:
            email_addresses=yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    management_address=email_addresses['management'].split(",")
    s = smtplib.SMTP(host='smtp.gmail.com', port=587)
    s.starttls()
    s.login(FLAGS.email_address, FLAGS.email_password)
    def send_mail(send_from, send_to,cc, subject, text,smtp, files=None):
        assert isinstance(send_to, list)

        msg = MIMEMultipart()
        msg['From'] = send_from
        msg['To'] = COMMASPACE.join(send_to)
        if cc:
            msg['CC']= COMMASPACE.join(cc)
        msg['Date'] = formatdate(localtime=True)
        msg['Subject'] = subject

        msg.attach(MIMEText(text))

        for f in files or []:
            with open(f, "rb") as fil:
                part = MIMEApplication(
                    fil.read(),
                    Name=basename(f)
                )
            # After the file is closed
            part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
            msg.attach(part)
        if cc:
            send_to+=cc
        smtp.sendmail(send_from, send_to, msg.as_string())
    for user in email_addresses:
        print(user)
        if user=='management':
            send_to=management_address
            cc=None
            subject="Bahasa indonesia ASR data verification dailyreport {}".format(datetime.today().strftime('%Y-%m-%d'))
            text="Hi All, \nThe daily report attached. FYI.\n\nThanks and Best Regards"
            files=user_report_names['management']
            send_mail(FLAGS.email_address,send_to,cc,subject,text,s,files)
        else:
            send_to=email_addresses[user].split(',')
            cc=management_address
            subject="Bahasa indonesia ASR data verification personal performance dailyreport {}".format(datetime.today().strftime('%Y-%m-%d'))
            text="Dear {}, \nYour daily performance report is attached. FYI.\n\nThanks and Best Regards".format(user)
            files=user_report_names[user]
            send_mail(FLAGS.email_address,send_to,cc,subject,text,s,files)
    s.quit()





def main(_):
    
    if FLAGS.post_process:
	    run_post_process()
    generate_reports()

	
if __name__ == "__main__":

	app.run(main)

#%%python post_batch_process.py --configfile_name=config.yaml  --output_dir=output

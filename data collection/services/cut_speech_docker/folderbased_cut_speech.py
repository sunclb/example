import webvtt
import re
import datetime
import os
import ffmpeg
import logging
import csv
from absl import app
from absl import flags
import math
import wave
# from pydub.silence import detect_nonsilent
from pydub import AudioSegment
from pathos.multiprocessing import ProcessingPool as Pool
from multiprocess import Manager
import multiprocessing
from multiprocessing import  Value, cpu_count
from ctypes import c_int
import logging
import logging.handlers
import itertools

from pydub.utils import db_to_float
LOG_FILENAME="cut_audio_log.out"
# logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)
my_logger=logging.getLogger("mLog")
my_logger.setLevel(logging.DEBUG)
hander=logging.handlers.RotatingFileHandler(LOG_FILENAME,maxBytes=2048000,backupCount=5)
# hander.setLevel(logging.DEBUG)
my_logger.addHandler(hander)

FLAGS = flags.FLAGS
flags.DEFINE_string("input_folder","./data/cut_input",
	"input folder")
flags.DEFINE_string("output_folder","./data/cut_output","The dir of output files")
flags.DEFINE_integer("cut_lower_limit",1,"cut lower limit in seconds")
flags.DEFINE_integer("cut_upper_limit",5,"cut upper limit in seconds")
#summary counting for convert to wav process
total_num=Value(c_int,0)
success_num=Value(c_int,0)
fail_num=Value(c_int,0)
#summary counting for cut speech process
file_num=Value(c_int,0)
out_file_num=Value(c_int,0)
fail_file=Value(c_int,0)
#set multiple process control variables
process_num=int(cpu_count()-2)
def detect_silence(audio_segment, min_silence_len=1000, silence_thresh=-16, seek_step=1):
    seg_len = len(audio_segment)

    # you can't have a silent portion of a sound that is longer than the sound
    if seg_len < min_silence_len:
        return []

    # convert silence threshold to a float value (so we can compare it to rms)
    # silence_thresh = db_to_float(silence_thresh) * audio_segment.max_possible_amplitude
    silence_thresh = db_to_float(silence_thresh) * audio_segment.max

    # find silence and add start and end indicies to the to_cut list
    silence_starts = []

    # check successive (1 sec by default) chunk of sound for silence
    # try a chunk at every "seek step" (or every chunk for a seek step == 1)
    last_slice_start = seg_len - min_silence_len
    slice_starts = range(0, last_slice_start + 1, seek_step)

    # guarantee last_slice_start is included in the range
    # to make sure the last portion of the audio is seached
    if last_slice_start % seek_step:
        slice_starts = itertools.chain(slice_starts, [last_slice_start])

    for i in slice_starts:
        audio_slice = audio_segment[i:i + min_silence_len]
        if audio_slice.rms <= silence_thresh:
            silence_starts.append(i)

    # short circuit when there is no silence
    if not silence_starts:
        return []

    # combine the silence we detected into ranges (start ms - end ms)
    silent_ranges = []

    prev_i = silence_starts.pop(0)
    current_range_start = prev_i

    for silence_start_i in silence_starts:
        continuous = (silence_start_i == prev_i + seek_step)

        # sometimes two small blips are enough for one particular slice to be
        # non-silent, despite the silence all running together. Just combine
        # the two overlapping silent ranges.
        silence_has_gap = silence_start_i > (prev_i + min_silence_len)

        if not continuous and silence_has_gap:
            silent_ranges.append([current_range_start,
                                  prev_i + min_silence_len])
            current_range_start = silence_start_i
        prev_i = silence_start_i

    silent_ranges.append([current_range_start,
                          prev_i + min_silence_len])

    return silent_ranges


def detect_nonsilent(audio_segment, min_silence_len=1000, silence_thresh=-16, seek_step=1):
    silent_ranges = detect_silence(audio_segment, min_silence_len, silence_thresh, seek_step)
    len_seg = len(audio_segment)

    # if there is no silence, the whole thing is nonsilent
    if not silent_ranges:
        return [[0, len_seg]]

    # short circuit when the whole audio segment is silent
    if silent_ranges[0][0] == 0 and silent_ranges[0][1] == len_seg:
        return []

    prev_end_i = 0
    nonsilent_ranges = []
    for start_i, end_i in silent_ranges:
        nonsilent_ranges.append([prev_end_i, start_i])
        prev_end_i = end_i

    if end_i != len_seg:
        nonsilent_ranges.append([prev_end_i, len_seg])

    if nonsilent_ranges[0] == [0, 0]:
        nonsilent_ranges.pop(0)

    return nonsilent_ranges

def mp3gen(folder):
	for root, dirs, files in os.walk(folder):
		for filename in files:
			my_logger.debug("process file:{}".format(filename))
			yield os.path.join(root, filename)

def convert_to_wav(filename,output_folder):
	if not os.path.exists(output_folder):
		os.mkdir(output_folder)
	audio=ffmpeg.input(filename)
	newfilename=os.path.basename(filename)
	newfilename=os.path.splitext(newfilename)[0]
	newfilename=newfilename+".wav"
	newfilename=os.path.join(output_folder,newfilename)
	audio=ffmpeg.output(audio,newfilename,ac=1,ar=16000,sample_fmt="s16")
	#audio=ffmpeg.overwrite_output(audio)
	ffmpeg.run(audio)

	return 1

def folderbase_convert_to_wave(webmfolder,wavefolder):
	def process_convert(lock,filename):
		my_logger.debug("filename is {}".format(filename))
		with total_num.get_lock():
			total_num.value+=1
		try:
			success=convert_to_wav(filename,wavefolder)
			with success_num.get_lock():
				success_num.value+=success
			os.remove(filename)
		except Exception as e:
			line="\t".join([str(datetime.datetime.now()),filename,str(e)])
			my_logger.info(line)
			fail_folder="data/convert_failed"
			if not os.path.exists(fail_folder):
				os.mkdir(fail_folder)
			filebase=os.path.basename(filename)
			failed_file=os.path.join(fail_folder,filebase)
			os.rename(filename,failed_file)
			with fail_num.get_lock():
				fail_num.value+=1
		return 1
	filenames=[]
	for file in mp3gen(webmfolder):
		if re.search("wav",file):continue
		filenames.append(file)
	pool = Pool(process_num)
	m=Manager()
	lock=m.Lock()
	locks=[lock]*len(filenames)
	pool.map(process_convert, locks,filenames)

	my_logger.info("{}/{} files successfully converted to wave and {} files failed".format(success_num.value,total_num.value,fail_num.value))

def unix_time_millis(dt):
	epoch_str='00:00:00.000'
	epoch = datetime.datetime.strptime(epoch_str,'%H:%M:%S.%f')
	return (dt - epoch).total_seconds()

def cut_wave(wave_file,cut_start,cut_end,start_bias=0,end_bias=0):
	newAudio_prop={}
	with wave.open(wave_file, mode='rb') as newAudio:
		newAudio_prop["nchannels"]=newAudio.getnchannels()
		newAudio_prop["nframes"]=newAudio.getnframes()
		newAudio_prop["sampwidth"]=newAudio.getsampwidth()
		newAudio_prop["framerate"]=newAudio.getframerate()
		newAudio_prop["comptype"]=newAudio.getcomptype()
		newAudio_prop["compname"]=newAudio.getcompname()
		cut_duration=cut_end+end_bias-cut_start-start_bias
		cut_nframe=int(math.floor(cut_duration*newAudio_prop["framerate"]))
		#start_bias=datetime.timedelta(seconds=start_bias)
		newAudio.setpos(int(math.floor((cut_start+start_bias)*newAudio_prop["framerate"])))
		cut_audio=newAudio.readframes(cut_nframe)
		newAudio_prop["totalduration"]=newAudio_prop["nframes"]/newAudio_prop["framerate"]
		newAudio_prop["nframes"]=int(len(cut_audio)/newAudio_prop["nchannels"]/newAudio_prop["sampwidth"])

	return cut_audio,newAudio_prop


#cutting criteria: drop al segments below 3s
#assumption: all speech can be cut into defined interval. e.g. (3-5s)
def TimestampMillisec64():
	return int((datetime.datetime.utcnow() - datetime.datetime(1970, 1, 1)).total_seconds() * 1000)
def cut_by_silence(lock,precut_audio_path,cut_interval,output_folder,filebasename,total_duration):
	if not os.path.exists(output_folder):
		os.mkdir(output_folder)
	#use pydub AudioSegment &silence module to detect silence, cut and save, 
	#return last chunck's time stamp in millionsecond
	cut_num=0
	
	audio_segment=AudioSegment.from_wav(precut_audio_path)
	my_logger.debug("current process is {} tmp file is {}".format(multiprocessing.current_process(),precut_audio_path))

	silence_thresh_tries=range(-40,-20)
	cut_lower_limit=cut_interval[0]*1000
	cut_upper_limit=cut_interval[1]*1000
	min_silence_thresh=[500,450,400,350,300,250,200,150,100,50]

	para_pair=[(350,-45),(350,-44),(350,-43),(350,-42),(350,-41),(400,-40),(400,-39),(400,-38),(400,-37),(400,-36),(400,-35),(400,-34),(400,-33),(400,-32),(400,-31),(450,-30),(450,-29),(450,-28),(450,-27),(450,-26),(450,-25),(400,-30),(400,-29),
		(400,-28),(400,-27),(400,-26),(400,-25),(450,-24),(450,-23),(450,-22),(450,-21),(450,-20),(400,-24),
		(400,-23),(400,-22),(400,-21),(400,-20),(350,-30),(350,-29),(350,-28),(350,-27),(350,-26),(350,-25),(300,-20),(300,-29),(300,-28),(300,-27),(300,-26),
		(300,-25),(250,-30),(250,-29),(250,-28),(250,-27),(250,-26),(250,-25),(450,-19),(450,-18),(450,-17),(450,-16),(450,-15),(400,-19),
		(400,-18),(400,-17),(400,-16),(400,-15),(450,-14),(450,-13),(450,-12),(450,-11),(450,-10),(400,-14),(400,-13),(400,-12),(400,-11),(400,-10),(450,-9),(450,-8),(450,-7),(450,-6),(450,-5),(400,-9),
		(400,-8),(400,-7),(400,-6),(400,-5),(350,-20),(350,-19),(350,-18),(350,-17),(350,-16),(350,-15),(350,-14),(350,-13),(350,-12),(350,-11),(350,-10),(350,-9),(350,-8),(350,-7),(350,-6),(350,-5),
		(300,-24),(300,-23),(300,-22),(300,-21),(300,-20),(300,-19),(300,-18),(300,-17),(300,-16),(300,-15),(300,-14),(300,-13),(300,-12),(300,-11),(300,-10),(450,-9),(450,-8),
		(450,-7),(450,-6),(450,-5),(400,-9),(400,-8),(400,-7),(400,-6),(400,-5),(350,-9),(350,-8),(350,-7),(350,-6),(350,-5),(300,-9),(300,-8),(300,-7),(300,-6),
		(300,-5),(200,-30),(200,-29),
		(200,-28),(200,-27),(200,-26),(200,-25),(250,-24),(250,-23),(250,-22),(250,-21),(250,-20),(200,-24),(200,-23),
		(200,-22),(200,-21),(200,-20),(150,-40),(150,-39),(150,-38),(150,-37),(150,-36),(150,-35),(150,-34),(150,-33),(150,-32),(150,-31),(150,-30),(250,-29),(250,-28),(250,-27),(250,-26),(250,-25),(200,-29),
		(200,-28),(200,-27),(200,-26),(200,-25),(150,-29),(150,-28),(150,-27),(150,-26),(150,-25),(250,-24),(250,-23),(250,-22),(250,-21),(250,-20),(200,-24),(200,-23),(200,-22),(200,-21),(200,-20),(150,-24),
		(150,-23),(150,-22),(150,-21),(150,-20),(250,-19),
		(250,-18),(250,-17),(250,-16),(250,-15),(200,-19),(200,-18),(200,-17),(200,-16),(200,-15),(150,-19),(150,-18),(150,-17),(150,-16),(150,-15),(250,-14),(250,-13),(250,-12),(250,-11),(250,-10),(200,-14),
		(200,-13),(200,-12),(200,-11),(200,-10),(150,-14),(150,-13),(150,-12),(150,-11),(150,-10),(250,-9),(250,-8),(250,-7),(250,-6),(250,-5),(200,-9),(200,-8),(200,-7),(200,-6),(200,-5),(150,-9),(150,-8),(150,-7),(150,-6),(150,-5)]

	#get current process number
	current_process=str(multiprocessing.current_process())
	digit_pattern="[0-9]+"
	current_process=re.search(digit_pattern,current_process)
	if current_process:
		current_process=current_process.group(0)
	else:
		current_process='0'
	def _offset_chunck(chunck,offset):
		for i,item in enumerate(chunck):
			chunck[i]+=offset
		return chunck

	def _generate_chunch(audio_segment,offset=0):
		init_chunck=[]
		
		# for min_silence in min_silence_thresh:
		# 	for silence_thresh in silence_thresh_tries:
		for (min_silence,silence_thresh) in para_pair:
			temp_chuncks=detect_nonsilent(audio_segment,min_silence_len=min_silence,silence_thresh=silence_thresh)
			# my_logger.debug("current process is {} try {} and silence duration {}".format(current_process,silence_thresh,min_silence))
			if len(temp_chuncks)>=1 and any((ck[1]-ck[0])<cut_upper_limit for ck in temp_chuncks):
				for i,sub_chunck in enumerate(temp_chuncks): 
					if (sub_chunck[1]-sub_chunck[0])<=cut_upper_limit:
						# my_logger.debug("current process is {} current offset is {}, new chunck added to result is {}".format(current_process,offset,sub_chunck))
						sub_chunck=_offset_chunck(sub_chunck,offset)
						init_chunck.append(sub_chunck)
					elif (sub_chunck[1]-sub_chunck[0])>cut_upper_limit:
						new_offset=offset+sub_chunck[0]
						# my_logger.debug("current process is {} new offset is {}, further cut chunck {}".format(current_process,new_offset,sub_chunck))
						new_chuncks=_generate_chunch(audio_segment[sub_chunck[0]:sub_chunck[1]],offset=new_offset)
						if new_chuncks:
							init_chunck=init_chunck+new_chuncks
							# my_logger.debug("current process is {} new offset is {}, add newly cuted chuncks {}".format(current_process,new_offset,new_chuncks))
			
			
				# my_logger.debug("current process is {} current offset is {}, 1 return newly cuted chuncks {}".format(current_process,offset,init_chunck))
				return init_chunck
		
		# my_logger.debug("current process is {} current offset is {},2 return newly cuted chuncks {}".format(current_process,offset,init_chunck))
		return init_chunck
	result_chuncks=_generate_chunch(audio_segment)
	my_logger.debug("result chuncks are {}".format(result_chuncks))
	if len(result_chuncks)>1:
		save_chunck_condition=False
		save_chunck=[result_chuncks[0][0],result_chuncks[0][1]]
		for (i,sub_chunck) in enumerate(result_chuncks):
			# my_logger.debug("analysing chunck {}".format(sub_chunck))
			# my_logger.debug("save condition is {}".format(((sub_chunck[1]-save_chunck[0])>cut_upper_limit or (sub_chunck[0]-save_chunck[1])>750)))
			# my_logger.debug("i is {} number of chuncks is {}".format(i,len(result_chuncks)))

			if (sub_chunck[1]-save_chunck[0])>cut_upper_limit or (sub_chunck[0]-save_chunck[1])>1000:
				save_chunck_condition=True
			if save_chunck_condition:
				if save_chunck[1]-save_chunck[0]>=cut_lower_limit:
					out_audio_file=os.path.join(output_folder,filebasename+"_"+str(cut_num)+str(int(datetime.datetime.now().timestamp()))+".wav")
					audio_segment[save_chunck[0]:save_chunck[1]].export(out_audio_file,format='wav')
					cut_num+=1
					my_logger.debug("current process is {}, write chunck{} to file {}".format(current_process,save_chunck,out_audio_file))
				save_chunck[0]=max(sub_chunck[0]-100,0)
				save_chunck_condition=False
			if i==(len(result_chuncks)-1):
				result_chuncks[-1][0]=save_chunck[0]
				save_chunck[1]=sub_chunck[1]
			else:
				save_chunck[1]=min(sub_chunck[1]+100,cut_upper_limit*1.5)

			
			# my_logger.debug("current process is {}, update save chunck to {} with sub chunck {}".format(current_process,save_chunck,sub_chunck))
		# my_logger.debug("current process is {}, 1 return final chunck {} and cut number {}".format(current_process,result_chuncks[-1],cut_num))
		return result_chuncks[-1][0]/1000,cut_num
	elif len(result_chuncks)==1 and result_chuncks[0][1]<cut_upper_limit*1.5:
		if result_chuncks[-1][1]-result_chuncks[-1][0]>=cut_lower_limit:
			out_audio_file=os.path.join(output_folder,filebasename+"_"+str(cut_num)+str(int(datetime.datetime.now().timestamp()))+".wav")
			audio_segment[max(result_chuncks[0][0]-100,0):min(result_chuncks[0][1]+100,cut_upper_limit*1.5)].export(out_audio_file,format='wav')
			cut_num+=1
			my_logger.debug("current process is {}, write chunck{} to file {}".format(current_process,result_chuncks[0],out_audio_file))
		# my_logger.debug("current process is {}, 2 return final chunck {} and cut number {}".format(current_process,result_chuncks[-1],cut_num))
		return result_chuncks[-1][1]/1000,cut_num
	elif len(result_chuncks)==1:
		# my_logger.debug("current process is {}, 4 return final chunck {} and cut number {}".format(current_process,result_chuncks[-1],cut_num))
		return result_chuncks[-1][0]/1000,cut_num
	else:
		# my_logger.debug("current process is {}, 3 return final chunck {} and cut number {}".format(current_process,cut_upper_limit*1.5/1000,cut_num))
		return cut_upper_limit*1.5/1000,cut_num


def cut_wav_without_subtitle(lock,audio_file_path,output_folder,cut_interval=(1,5)):
	filebasename=os.path.basename(audio_file_path)
	filebasename,_=os.path.splitext(filebasename)
	temp_file=filebasename+"_temp.wav"
	#get audio properties
	audio_prop={}
	with wave.open(audio_file_path, mode='rb') as newAudio:
		audio_prop["nchannels"]=newAudio.getnchannels()
		audio_prop["nframes"]=newAudio.getnframes()
		audio_prop["sampwidth"]=newAudio.getsampwidth()
		audio_prop["framerate"]=newAudio.getframerate()
		audio_prop["comptype"]=newAudio.getcomptype()
		audio_prop["compname"]=newAudio.getcompname()
	audio_duration=audio_prop["nframes"]/audio_prop["framerate"]

	precut_duration=cut_interval[1]*1.5
	cut_start=0
	cut_return=0
	cut_num=0
	my_logger.debug("process worker {} new cut start is {}s".format(multiprocessing.current_process(), cut_start))
	while cut_start<audio_duration:
		cut_end=cut_start+precut_duration
		cut_audio,cutaudio_prop=cut_wave(audio_file_path,cut_start,cut_end,start_bias=0,end_bias=0) 
		with wave.open(temp_file, "wb") as newAudio:
			newAudio.setparams((cutaudio_prop["nchannels"],cutaudio_prop["sampwidth"],
								cutaudio_prop["framerate"],cutaudio_prop["nframes"],
								cutaudio_prop["comptype"],cutaudio_prop["compname"]))
			newAudio.writeframes(cut_audio)
		filebasename_start=filebasename+"_"+str(int(cut_start))
		cut_return,cut=cut_by_silence(lock,temp_file,cut_interval,output_folder,filebasename_start,audio_duration)
		my_logger.debug("cut return is {} and cut is {}".format(cut_return,cut))
		if cut_return:
			cut_start=cut_start+cut_return
			cut_num+=cut
			# my_logger.debug("process worker {} new cut start is {}s".format(multiprocessing.current_process() ,cut_start))
		else:
			cut_start=audio_duration
	os.remove(temp_file)
	return cut_num

def folderbase_cut(input_folder,output_folder,cut_interval):
	#input_folder should have a structure with wav folder containing wav files
	#and vtt folder containing possible subtitle files
	#output_folder will contain output_subtitle and output_without_subtitle folders after processing

	wav_dir=os.path.join(input_folder,"wav")
	output_wo_sub=output_folder

	if not os.path.exists(output_wo_sub):
		os.mkdir(output_wo_sub)
	wav_files=[]
	for root,dirs,files in os.walk(wav_dir):
		for filename in files:
			wav_files.append(filename)
	def process_cut(lock,file):
		try:
			#exclude log.txt file
			if re.search(".+\.wav",file):
				filebasename,_=os.path.splitext(file)
				my_logger.info("process worker {} Processing file {}".format(multiprocessing.current_process(), filebasename))
				wave_file=os.path.join(wav_dir,file)

				wo_num=cut_wav_without_subtitle(lock,wave_file,output_wo_sub,cut_interval)
				my_logger.info("process worker {} file {} has no subtitle".format(multiprocessing.current_process(),filebasename))
				with file_num.get_lock():
					file_num.value+=1
				with out_file_num.get_lock():
					out_file_num.value+=wo_num
				os.remove(wave_file)

		except Exception as e:
			error_folder="data/cut_failed"
			if not os.path.exists(error_folder):
				os.mkdir(error_folder)
			fail_file_path=os.path.join(error_folder,file)
			os.rename(wave_file,fail_file_path)
			line=[str(datetime.datetime.now()),filebasename,"error",str(e)]
			line="\t".join(line)
			my_logger.info(line)

			logging.info(e)
			with fail_file.get_lock():
				fail_file.value+=1
			return 1
	pool = Pool(process_num)
	# pool=Pool(1)
	m=Manager()
	lock=m.Lock()

	locks=[lock]*len(wav_files)
	pool.map(process_cut, locks,wav_files)

	loginfo='''Total number of audio files processed is {}, {} files failed
		Total number of audio files generated is {}'''.format(
			file_num.value,fail_file.value,out_file_num.value)
	my_logger.info(loginfo) 
	
def folderbased_cut_speech(input_folder,output_folder,cut_interval):
	webmfolder=os.path.join(input_folder)
	wavefolder=os.path.join(input_folder,"wav")
	my_logger.debug("webmfolder is {} and wavefolder is{}".format(webmfolder,wavefolder))
	folderbase_convert_to_wave(webmfolder,wavefolder)
	folderbase_cut(input_folder,output_folder,cut_interval)
	
def main(__):
	#load input_file and parse file_name, link
	my_logger.setLevel(logging.DEBUG)

	input_folder=FLAGS.input_folder
	output_folder=FLAGS.output_folder
	cut_interval=(FLAGS.cut_lower_limit,FLAGS.cut_upper_limit)
	folderbased_cut_speech(input_folder,output_folder,cut_interval)

	
if __name__ == "__main__":

	app.run(main)
#%%




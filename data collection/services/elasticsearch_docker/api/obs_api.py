"""
Note:
    To use your own "access key", "secret key", "bucket name", "EndPoint",
    please go to obs/constant.py

    Upload/Download Rules:
        - Directory name must not use dot "." as the first char.
         for example, directory name: .ipythoncheck_point XXX not allowed
        - Directory Name must not end with slash "/".
        - File size not more than 100MB


    upload_dir(object_dir, local_file_dir)
        - object_dir: destination bucket folder path
        - local_file_dir: source folder name

        - example:
            - upload_dir("python_api", "/Users/Documents/ASRLib/PythonBucketAPI")
            - upload_dir("python_api/java_api", "/Users/Documents/ASRLib/PythonBucketAPI/JavaBucketAPI")

    download/upload_file(object_key, local_file):
        - object_key: bucket file path
        - local_file: full local file path and name

    download_dir(object_dir, local_dir):
        - object_dir: source bucket folder path
        - local_file_dir: destination full local folder path

"""

from obs import ObsOperator, ObjectMetadata, MultipartUploadFileRequest
from obs.util import *
import urllib
from urllib.request import urlopen
import os


# upload | download | all
flag = 'all'
user_metadata = {'data': 'speech', 'type': 'wav,mps,txt,csv'}
obs = ObsOperator(obs_host, obs_access_key, obs_secret_key)


def init_obj_metadata():
    object_metadata = ObjectMetadata()
    for k, v in user_metadata.items():
        object_metadata.add_user_metadata(k, v)
    return object_metadata


def upload_file(object_key, local_file):
    object_metadata = init_obj_metadata()
    with open(local_file, "rb") as from_file:
        ret = obs.put_object_from_file(bucket_name, object_key, from_file, object_metadata)
    return object_key+" uploaded: "+ret.get_e_tag()


def upload_dir(object_dir, local_file_dir):
    files = get_dir_files(local_file_dir)
    for idx, f in enumerate(files):
        key = f.replace(local_file_dir, "")
        out = upload_file(object_dir+key, f)
        # out = object_key+"/"+key
        print(idx+1)
        print(out+"\n")


def download_file(object_key, local_file):
    s3_object = obs.get_object(bucket_name, object_key.strip())
    os.makedirs(os.path.dirname(local_file), exist_ok=True)
    with open(local_file, "wb") as to_file:
        s3_object.to_file(to_file)
    return "File Downloaded: " + local_file


def download_dir(object_dir, local_dir):
    files = list_dir_files(object_dir)
    for idx, f in enumerate(files):
        # out = download_file(object_dir + "/" + f, local_dir + "/" + f)
        print(f + "\n")
        f = f.strip()
        out = download_file(f, local_dir + "/" + f)
        print(str(idx + 1) + out + "\n")



def get_dir_files(file_dir):
    files = []
    for root, d_names, f_names in os.walk(file_dir):
        for f in f_names:
            files.append(os.path.join(root, f))
    return files


def list_dir_files(object_dir):
    try:
        with urlopen(bucket_list_url+"/"+bucket_name+"/"+object_dir) as response:
            lines = []
            for idx, line in enumerate(response):
                lines.append(line.decode("utf-8"))
            print("\n".join(lines))
            return lines
    except urllib.error.URLError as e:
        list_dir_files(object_dir)


def main():
    object_key = ""
    file_dir = ""
    # upload_file("python/level.log", "/Users/xianzhihai924/Documents/ASRLib/PythonBucketAPI/storage/level.log")
    #upload_dir("python", "/Users/xianzhihai924/Documents/ASRLib/PythonBucketAPI/storage")
    list_dir_files("abc-abc")


if __name__ == '__main__':
    main()



from __future__ import print_function

import sys

import logging
import logging.handlers

def prefix_print(prefix, message):
    return (prefix + ('\n' + prefix).join(message.split('\n')))
class My_logger():
    def __init__(self,log_level=0,filename='log.out',backup_count=5):
        self.log_level=0
        self.log_filename=filename
        self.my_logger=logging.getLogger('mLog')
        self.hander=logging.handlers.RotatingFileHandler(self.log_filename,maxBytes=20480000,backupCount=backup_count)
        self.my_logger.addHandler(self.hander)
        self.my_logger.setLevel((self.log_level+1)*10)


    # Logging functions
    # =================
    def set_log_level(self,level=0):
        self.log_level=level
        self.my_logger.setLevel((self.log_level+1)*10)

    def log_debug(self,message):
        if self.log_level>=0:
            self.my_logger.info(prefix_print('D ', message))


    def log_info(self,message):
        if self.log_level>=1:
            self.my_logger.info(prefix_print('I ', message))


    def log_warn(self,message):
        if self.log_level>=2:
            self.my_logger.warning(prefix_print('W ', message))


    def log_error(self,message):
        if self.log_level>=3:
            self.my_logger.error(prefix_print('E ', message))

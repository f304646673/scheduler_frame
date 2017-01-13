import os
import sys
import inspect
import logging
import logging.config
from singleton import singleton

@singleton
class loggingex():
    def __init__(self, conf_path):
        error = 0
        while True:
            try:
                logging.config.fileConfig(conf_path)
            except IOError as e:
                if error > 1:
                    raise e
                if 2 == e.errno:
                    if os.path.isdir(e.filename):
                        os.makedirs(e.filename)
                    else:
                        os.makedirs(os.path.dirname(e.filename))
                error = error + 1
            except Exception as e: 
                raise e
            else:
                break
    
    def log_debug(self, msg):
        log_debug = logging.getLogger('logger_LogDebug')        #https://docs.python.org/2/howto/logging.html
        log_debug.debug(msg)
        
    def log_info(self, msg):
        log_info = logging.getLogger('logger_LogInfo')
        log_info.info(msg)
        
    def log_warning(self, msg):
        log_warning_error_critical = logging.getLogger('logger_LogWarningErrorCritical')
        log_warning_error_critical.warning(msg)
        
    def log_error(self, msg):
        log_warning_error_critical = logging.getLogger('logger_LogWarningErrorCritical')
        log_warning_error_critical.error(msg)      
        
    def log_critical(self, msg):
        log_warning_error_critical = logging.getLogger('logger_LogWarningErrorCritical')
        log_warning_error_critical.critical(msg)
 
    def log_error_sql(self, msg):
        log_error_sql = logging.getLogger('logger_SQL_ERROR')
        log_error_sql.critical(msg)

def LOG_INIT(conf_path):
    global logger_obj
    logger_obj = loggingex(conf_path)

def modify_msg(msg):
    stack_info = inspect.stack()
    if len(stack_info) > 2:
        file_name = inspect.stack()[2][1]
        line = inspect.stack()[2][2]
        function_name = inspect.stack()[2][3]
        new_msg = file_name + " ^ " + function_name + " ^ " + str(line) + " ^ " + msg
    return new_msg

def LOG_DEBUG(msg):
    new_msg = modify_msg(msg)
    try:
        logger_obj.log_debug(new_msg)
    except Exception as e:
        print new_msg
    
def LOG_INFO(msg):
    new_msg = modify_msg(msg)
    try:
        logger_obj.log_info(new_msg)
    except Exception as e:
        print new_msg
    
def LOG_WARNING(msg):
    new_msg = modify_msg(msg)
    try:
        logger_obj.log_warning(new_msg)
    except Exception as e:
        print new_msg
    
def LOG_ERROR(msg):
    new_msg = modify_msg(msg)
    try:
        logger_obj.log_error(new_msg)
    except Exception as e:
        print new_msg
    
def LOG_CRITICAL(msg):
    new_msg = modify_msg(msg)
    try:
        logger_obj.log_critical(new_msg)
    except exception as e:
        print new_msg

def LOG_ERROR_SQL(msg):
    try:
        logger_obj.log_error_sql(msg)
    except exception as e:
        print msg


if __name__ == "__main__":
    LOG_INIT("../../conf/log.conf")
    LOG_DEBUG('LOG_DEBUG')
    LOG_INFO('LOG_INFO')
    LOG_WARNING('LOG_WARNING')
    LOG_ERROR('LOG_ERROR')
    LOG_CRITICAL('LOG_CRITICAL')
    LOG_ERROR_SQL("Create XXX Error")
    #global logger_obj
    #logger_obj.log_debug('XXXXXXXXXXXX')
    print "Hello World"

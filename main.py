"""
Main module
"""

# Built-in modules
from os import sep, listdir, fspath, path, remove, stat, scandir, path, name
from pathlib import Path
from shutil import disk_usage
from datetime import datetime
from subprocess import Popen, PIPE
import logging

# Third party modules
from sqlalchemy import create_engine, exc
from apscheduler.schedulers.blocking import BlockingScheduler

# CONSTANTS
### ------------------------------- FOLDERS ----------------------------------- ###
# current project folder for testing
BKP_EXT_MOUNT_FOLDER = 'C:{0}Users{0}Tiago{0}Documents{0}python{0}mysql-bkp{0}media{0}'.format(sep) # <<<--- Manual define your backup media path
BKP_FULL_PREFIX = 'full.bkp'
BKP_INCR_PREFIX = 'incr.bkp'
BKP_BASEDIR = path.abspath(path.dirname(__file__))
### --------------------------------------------------------------------------- ###
### ------------------------------- DATABASE ---------------------------------- ###
DBUSER = "root"
DBPASSWORD = "XXXXX" # <<<--- Manual define your password.
DBHOST = "localhost"
DBPORT= '3306'
DBDATABASE = 'information_schema'
SQLALCHEMY_DATABASE_URI = 'mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}'.format(DBUSER,DBPASSWORD,DBHOST,DBPORT,DBDATABASE)
SQLALCHEMY_TRACK_MODIFICATIONS = False
SQLALCHEMY_ECHO = True
MYSQLDUMP = "\"C:\\Program Files\\MySQL\\MySQL Server 8.0\\bin\\mysqldump.exe\"" # <<<--- on windows the full path had to be set, on unix just mysqldump is enough.
MYSQL_CONF_FILE = '{0}{1}{2}'.format(BKP_BASEDIR, sep, 'mysql.cnf')
MYSQLDUMP_ARGS = "--defaults-file={0} --single-transaction --flush-logs --master-data=2 --all-databases".format(MYSQL_CONF_FILE)
MYSQLADMIN = 'mysqladmin'
MYSQL_BINLOGS_STATUS = ''
### --------------------------------------------------------------------------- ###
### -------------------------------- LOGS ------------------------------------- ###
# types : debug, info, warning, error, critical, exception
logging.basicConfig(filename='logs.log', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S',format='%(asctime)s - %(levelname)s - %(message)s')
### --------------------------------------------------------------------------- ###

# creating the sqlalchemy engine
mysql_engine = create_engine(SQLALCHEMY_DATABASE_URI)


def mysql_exec_stmt(stmt):
    """
    Connects to the database and executes a sql statement.

    """
    try:
        # establishing a connection
        mysql_conn = mysql_engine.connect()
        # executing statement
        result = mysql_conn.execute(stmt)
        # closing the connection
        mysql_conn.close()
        return result
    except exc.DBAPIError as error:
        print(error)
        logging.error(error)
        return False

def log_bin_basename():
    """
    Query the DB for the bin_log folder location.

    """
    result = mysql_exec_stmt("SHOW VARIABLES LIKE 'log_bin_basename'")
    for row in result:
        # return true if bin log is ON or false if not
        return (Path(row["Value"])).parent

# defining this constant which constains the path for the binarylogs directory
MYSQL_BINLOGS_DIR = log_bin_basename()

def get_index_binlog():
    """
    This functions scans the binary logs folder looking for the index file and returns its name.

    """
    try:
        # scanning the binlogs folder
        dir_entries = scandir(MYSQL_BINLOGS_DIR)
        # for each file/dir found
        for entry in dir_entries:
            # if it's a file
            if entry.is_file():
                # if .index is in the name of the file return it, cause it is the index file we are looking for.
                if '.index' in entry.name:
                    return entry.name
    except OSError as error:
        print(get_datetime(), " Failed to find an index bin log file.")
        logging.error("Failed to find an index bin log file. {0}".format(error))
        return False

# setting the name of the binarylog file
MYSQL_BINLOGS_INDEX = get_index_binlog()

def mysql_connection_test():
    """
    Testing the connection with the database sending a query for its uptime.

    """

    # sending a query to the database to test the connection.
    if not mysql_exec_stmt("SHOW STATUS LIKE \'uptime\'"):
        logging.critical("Connection to the DATABASE failed. Check the connection with the database and try to start the app again..")
        return False
    else:
        logging.info("Connected to the database.")
        return True

def is_log_bin_on():
    """
    Query the DB for the bin_log variable and determine if binary log is enabled or not and set the constant MYSQL_BINLOGS_STATUS with the result.

    """
    result = mysql_exec_stmt("SHOW VARIABLES LIKE 'log_bin'")
    for row in result:
        # return true if bin log is ON or false if not
        return True if row["Value"] == "ON" else False


def mysql_db_size():
    """
    Return the size of all databases in MB.

    """

    # query to read all databases and return its size in MB
    result = mysql_exec_stmt("SELECT table_schema AS 'Database',\
                               ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) 'size_mb'\
                              FROM information_schema.tables\
                              GROUP BY table_schema")
    # if the query successed
    if result:
        db_size = 0
        for row in result:
            db_size += row["size_mb"]
        return db_size
    else:
        return False

def is_there_enough_space():
    """
    Compare the free space on the removable storage with the database size to determine if there is enough space to copy it over.

    """

    # getting the free space of the external device
    total, used, free = disk_usage(BKP_EXT_MOUNT_FOLDER)

    # comparing the free space with the DB size
    if mysql_db_size() > (free / 1024 / 1024):
        return False
    else:
        return True

def is_there_full_bkp():
    """
    The function loops through the BKP folder looking for a file wich contains full.bkp in its name to determine if a full backup exists.

    """
    # looping through all the files in the backup folder
    for bkp_file in listdir(BKP_EXT_MOUNT_FOLDER):
        # if the filename contains full.bkp
        if BKP_FULL_PREFIX in bkp_file:
            return True
        #else:
        #    return False
    # moved return false out of the if because an unexpected behavior was happening.
    # listdir apparently is not consistent when listing the files and after the first check
    # if the file was not a full bkp it was ending the loop and with the the False out
    # if fullbkp prefix is not found in any file then return false.
    return False

def get_datetime():
    """
    Get datetime and format for the filename.

    """
    return datetime.now().strftime(("%Y.%m.%d_%H.%M.%S"))

def create_bkp_filename(prefix):
    """
    Creating the bkp filename.

    The last binary log file listed by the index is used in the filename.
    This function should be used before flush-logs to point to the right index file.

    """
    current_binlog_file = get_last_file_line("{0}{1}{2}".format(MYSQL_BINLOGS_DIR,sep, MYSQL_BINLOGS_INDEX))
    return "{0}-{1}[{2}].sql".format( prefix, get_datetime(), current_binlog_file ), current_binlog_file

def get_last_file_line(file_path):
    """
    Read the index file and extract its last line which represents the last binary log file created.

    file_path should be the full path for the index file. The system in which the script is running needs to be checked because unix stores the binary logs as full path.

    """
    # this function is not efficent because it reads all the lines in order to extract the last and in a big file this is an issue.
    f_read = open(file_path, "r")
    last_line = f_read.readlines()[-1]
    f_read.close()
    # replacing the extra chars at the beggining of the file name and returning it
    # this is not dynamic however this function should only be used to read the binlogs index file
    # if not it has to be redesigned.
    # strip was needed to remove the trailling new line \n

    if name == 'posix':
        # if the system is unix just extract the filename from path
        # rstrip to remove the trailing newpace \n
        last_line = Path(last_line.rstrip()).name
    else:
        # if windows just remove the .\ from the beggining of the filename
        last_line = (last_line.lstrip(".\\")).rstrip()

    return last_line

def create_full_bkp():
    """
    mysqldump command is run and the DB backup is created in the external media.

    """

    # The backup is being created directly into the external media, this could cause a slowdown on the database.
    # If storage is not a problem the backup can be created locally and then copied to the external media. However the script will have to be changed.

    # getting the bkpfile name and binlog_filename
    bkp_filename, binlog_filename = create_bkp_filename(BKP_FULL_PREFIX)

    # creating the full path for the backup file
    bkp_file = "{0}{1}".format(BKP_EXT_MOUNT_FOLDER, bkp_filename )
    # FULL BACKUP ---------
    # Mmysqldump full path, mysqldump arguments, external media to copy the backup and file name
    if name == 'nt': # naming should be double checked, maybe ceck for spaces to add quotes to escape the file name spaces.
        cmd = "{0} {1} > \"{2}\"".format(MYSQLDUMP, MYSQLDUMP_ARGS, bkp_file )
    else:
        cmd = "{0} {1} > {2}".format(MYSQLDUMP, MYSQLDUMP_ARGS, bkp_file )
    cmd_execute = Popen(cmd, stdout=PIPE, stdin=PIPE , stderr=PIPE, shell=True, universal_newlines=True) 
    cmd_output, cmd_error = cmd_execute.communicate()
    # ---------------------

    # if an error occured log and return false
    if cmd_error :
        try:
            # even if an error ocurred the bkp file is created and has to be deleted to avoid issues with future backups
            if stat(bkp_file).st_size == 0:
                # deleting the recently created empty file
                if Path(bkp_file).exists():
                    # confirm if exists before deleting, it was failing on UNIX without this check.
                    remove(bkp_file)
        except OSError as error:
            logging.error("Failed to delete the empty backup file. {0}".format(error))

        logging.critical("Full backup failed. {0}".format(cmd_error))
        print('Full backup failed. Check the logs.')
        return True
    else:
        print("{0} - Full backup created.".format(get_datetime()))
        logging.info("Full backup created. {0}".format(cmd_output))
        return False

def create_incr_bkp():
    """
    flush-logs is run to flush the changes to the lastest binlog file and create a new binlog file for new incremental backups.

    """
    # saving the current binlog_filename before flushing the database
    bkp_filename, binlog_filename = create_bkp_filename(BKP_INCR_PREFIX)
    # flushing the database changes to binlog_filename
    mysql_exec_stmt("FLUSH LOGS")
    # joining the recently flushed binlog file with its path
    binlog_flushed_file = path.join(MYSQL_BINLOGS_DIR,binlog_filename)
    # copying the file
    try:
        with Path( path.join(BKP_EXT_MOUNT_FOLDER,bkp_filename) ).open(mode='xb') as fid:
            fid.write( Path(binlog_flushed_file).read_bytes() )
        logging.info("Incrmental backup successfully.")
        return True
    except OSError as error:
        logging.error("Failed to copy the incremental binlog file. {0}".format(error))
        return False

def convert_date(timestamp):
    d = datetime.utcfromtimestamp(timestamp)
    formated_date = d.strftime('"%Y-%m-%d %H:%M:%S"')
    return formated_date

def backup():
    # testing the connection with the database
    if mysql_connection_test():
        # checking if binary logs is enabled.
        if is_log_bin_on():

            # if the index file has been found.
            if MYSQL_BINLOGS_INDEX:
                # if there is no full backup
                if not is_there_full_bkp():
                    create_full_bkp()
                else:
                    if create_incr_bkp():
                        print(get_datetime(),"- Incremental backup succesfully created.")
                    else: # create_incr_bkp()
                        print(get_datetime(), "Failed to create the incremental backup. check the logs.")
            else: # MYSQL_BINLOGS_INDEX
                print(get_datetime(), "Index file not found.")
        else: # is_log_bin_on()
            logging.critical("Binary logs is not ON. Stop your MySQL server enable binary logs start the server and try again.")
            print(get_datetime(), "Binary log option is not ON.")
    else: # mysql_connection_test()
        print("Database not reacheable. If you have not been notified by your personal BOT a primitive tool called log shall be checked for more information.")

backup()

""" if __name__ == "__main__":

    # scheduler object
    scheduler = BlockingScheduler()
    # executing a measure every 60 seconds
    scheduler.add_job(backup, 'cron', hour=1, minute=0)
    # to exit the app
    print('MySQL Auto-Backup tool is running.')
    print('Press Ctrl+{0} to exit'.format('Break' if name == 'nt' else 'C'))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass """



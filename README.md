# Overview

This tool is a cross platform python script to create incremental MySQL backups.

# Requirements

- Python3.3+
- MySQL

### Settings

A few settings should be changed in the source code before running the script :


|                |line|main.py|
|----------------|-------------------------------|-----------------------------|
|Database Variables|26-29            |Change accorndingly your database details.            |
|BKP_EXT_MOUNT_FOLDER|20            |The folder to save the backups.            |
|MYSQLDUMP          |34|If you are on windows you have to set the fullpath for mysqldump utility. set as `mysqldump` on unix.|
|Time Trigger |326| This line defines in which times the script is going to run. You can check the link below for more information on how to set up as desired.


[https://apscheduler.readthedocs.io/en/latest/modules/triggers/cron.html](https://apscheduler.readthedocs.io/en/latest/modules/triggers/cron.html)

#### mysql.cnf
Edit this file to reflect your database requirements.

### General Recommendations
Make sure to allow read,write and execute just to the user which will run the script otherwise the passwords on the files can be easily read by other users. 
A best practice would be to encrypt the external media the backup is being copied to because the script does not deal with any kind of encryption on the backups. 
You can create a tmux session in linux or run in the windows terminal.


## How to use

- Create a python virtual enviroment.
[https://docs.python.org/3/tutorial/venv.html](https://docs.python.org/3/tutorial/venv.html)
- Instal dependencies from the requirements.txt file :
`pip3 install > requirements.txt`
- Create a new tmux session :
`tmux`
- Run the script : `python3.7 main.py`


I hope this script helps people out there and if you find any bugs do not hesitate to contact me and let me know what is wrong.

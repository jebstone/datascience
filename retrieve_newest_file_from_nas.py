#!/bin/env python3
"""
Copies the most recent datafile from a secure NAS to a local file.
"""

key_file = 'key_rsa'  # NOT .pub
import paramiko
import sqlite3
from datetime import datetime

host = 'hostname'
port = 22
username = ''
filecount = 15
localdir = r"C:/"
remotedir = r"/nas/data/"

# SSH Key
my_key = paramiko.RSAKey.from_private_key_file(key_file)


def copy_file(filename):
	localpath = localdir + filename
	remotepath = remotedir + filename

	sftp.get(remotepath, localpath)
	print("Done")

# SFTP Connection
transport = paramiko.Transport((host, port))
transport.connect(username=username, pkey=my_key)
sftp = paramiko.SFTPClient.from_transport(transport)

# Get Top N records
file_list = sftp.listdir(remotedir)
file_list.sort(reverse=True)
file_list = file_list[:filecount]

# get the first record
myfile=file_list.pop()


# Copy the most recent file down to localpath
copy_file(myfile)



# Close connections
sftp.close()
transport.close()

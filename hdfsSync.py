#!/usr/bin/python
import subprocess
import argparse, traceback, time
import getpass
import os
import sets
import sys
# ok (sort of). No ckeck for file resync
def run(localPath, hdfsPath, remoteBridge, user=None, identityFile=None, ):
	if not hdfsPath.startswith("hdfs://"):
		return

	useRsync = subprocess.call(["which", "rsync"]) == 0

	items = os.listdir(localPath)
	
	sshName=remoteBridge
	identity =""


	if user:
		sshName = user + "@" + sshName
	if identityFile:
		identity = '-i ' + identityFile

	folderCreation = subprocess.call('ssh %s %s "hadoop fs -mkdir -p %s"' %(identity, sshName, hdfsPath), shell=True)
	if folderCreation != 0:
		print("ERROR CREATING REMOTE FOLDER")
		sys.exit(1)


	for item in items:
		abspath = os.path.join(localPath, item)
		if os.path.isdir(abspath):
			print item		
			cmd = 'scp -r %s %s %s:~/transfer' % (identity, localPath, sshName)
			if useRsync:
				cmd = 'rsync -avz -e "ssh %s" %s %s:~/transfer' % (identity, localPath, sshName)

			print subprocess.call(cmd, shell=True)
			print subprocess.call('ssh %s %s "hadoop fs -moveFromLocal -f ~/transfer/%s %s"' % (
				identity, 
				sshName, 
				item, 
				hdfsPath), shell=True)

def argument_parser():
    """ Define the arguments and return the parser object"""
    parser = argparse.ArgumentParser(
    description="""Describe me""")
    parser.add_argument('-l','--input',default='',help='Input file / folder path',type=str, required=True)
    parser.add_argument('-r','--output',default='',help='Remote hdfs file / folder',type=str, required=True)
    parser.add_argument('-b','--bridge',default='',help='Remote bridge machine',type=str, required=True)
    parser.add_argument('-u','--user',default='',help='Username for ssh and hdfs',type=str, required=False)
    parser.add_argument('-i','--identity',default='',help='identity file',type=str, required=False)    
    return parser

def main():
    args = argument_parser().parse_args()
    print ('Input folder: ', args.input)
    print ('Output folder: ', args.output)
    print ('Remote Bridge: ', args.bridge)    

    try:
        t0 = time.time()
        run(args.input, args.output, args.bridge, args.user, args.identity)
        print( 'Finished in %.2f seconds' % (time.time() - t0))
    except:
        print ('Execution failed!')
        print( traceback.format_exc())

if __name__ == "__main__":
    main()
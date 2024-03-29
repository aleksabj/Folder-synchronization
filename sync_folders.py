import os, shutil, time
import argparse # used to parse command line arguments
import hashlib # used for calculating md5 hash (checksum calculation)
from datetime import datetime

def calculate_md5(fname, callback=None):
# calculate the MD5 checksum of a file
# parameters:
# fname (str): The path to the file
# callback (function): An optional callback function to report progress
# returns:
# str: The MD5 checksum, or None if the file could not be read.
    hash_md5 = hashlib.md5()
    try: 
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
                if callback : callback() #callback function for progress
    except IOError as e:
        log (f"Error reading file {fname}: {e}", log_file)
        return None         
    return hash_md5.hexdigest()
    
def silent_callback():
    pass
    

def sync_folders(source, replica, log_file):
# synchronizes two folders, including creating missing directories and copying or updating files
# parameters:
# source (str): The source directory path
# replica (str): The replica directory path
# log_file (str): The log file path to write the synchronization log
    stats = {"directories_created": 0, "files_copied": 0, "files_updated": 0, "files_removed": 0, "directories_removed": 0}
    
    def process_file_action(source_file, replica_file, action):
    # performs the specified action (copy, update, remove) on a file
    # parameters:
    # source_file (str): The source file path
    # replica_file (str): The replica file path
    # action (str): The action to perform: "copy", "update", or "remove"
        try: 
            if action == "copy" or action == "update":
                shutil.copy2(source_file, replica_file) # copy file to replica, preserving metadata
                log(f"File {'copied' if action == 'copy' else 'updated'}: {replica_file}", log_file)
                stats[f"files_{'copied' if action == 'copy' else 'updated'}"] += 1
            elif action == "remove":
                os.remove(replica_file)
                log(f"File removed: {replica_file}", log_file)
                stats["files_removed"] += 1
        except IOError as e:
            log (f"Error processing file {replica_file}: {e}", log_file)
    
    def sync_makedirs(directory):
        try: 
            os.makedirs(directory)
            log(f"Directory created: {directory}", log_file)
            stats["directories_created"] += 1
        except IOError as e:
            log (f"Error creating directory {directory}: {e}", log_file)

    for root, _, files in os.walk(source): # walk through all files in the source directory
        relative_path = os.path.relpath(root, source) # get the relative path to use in the replica
        replica_root = os.path.join(replica, relative_path)
        if not os.path.exists(replica_root):
            # if the corresponding directory doesn't exist in the replica, create it 
            os.makedirs(replica_root)
            log(f"Directory created: {replica_root}", log_file)
            stats["directories_created"] += 1

        for file_name in files:
            # for each file in the source, determine if it needs to be copied or updated in the replica
            source_file = os.path.join(root, file_name)
            replica_file = os.path.join(replica_root, file_name)
            action = "none"

            if not os.path.exists(replica_file):
                action = "copy"
            elif os.path.getmtime(source_file) != os.path.getmtime(replica_file) or os.path.getsize(source_file) != os.path.getsize(replica_file):
                # if the file exists but has different size or modification time, check md5 checksum
                source_md5 = calculate_md5(source_file, silent_callback)
                replica_md5 = calculate_md5(replica_file, silent_callback) if source_md5 is not None else None
                if source_md5 is not None and replica_md5 is not None and source_md5 != replica_md5:
                    action = "update"
            process_file_action(source_file, replica_file, action)

        for replica_file in os.listdir(replica_root):
            # remove files that exist in the replica but not in the source
            source_file = os.path.join(root, replica_file)
            if not os.path.exists(source_file):
                process_file_action(None, os.path.join(replica_root, replica_file), "remove")
    
    # remove empty directories in the replica
    for root, dirs, _ in os.walk(replica, topdown=False):
        for dir_ in dirs:
            dir_path = os.path.join(root, dir_)
            if not os.listdir(dir_path):
                try:
                    os.rmdir(dir_path)
                    log(f"Directory removed: {dir_path}", log_file)
                    stats["directories_removed"] += 1
                except OSError as e:
                    log(f"Error removing directory {dir_path}: {e}", log_file)

    log(f"Sync stats: {stats}", log_file) # log the synchronization statistics

def log(message, file_path):
# logs a message with a timestamp to a file and prints it
# parameters:
# message (str): The message to log
# file_path (str): The path to the log file
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message = (f"{timestamp} - {message}")
    with open(file_path, "a") as f:
        f.write(log_message + "\n")
    print(log_message)

def main():
# main function to parse command-line arguments and start folder synchronization
    parser = argparse.ArgumentParser(description="Synchronize two folders.")
    parser.add_argument("--source", help = "Source folder path", required = True)
    parser.add_argument("--replica", help = "Replica folder path", required = True)
    parser.add_argument("--interval", type = int, help="Synchronization interval in seconds", required = True)
    parser.add_argument("--log", help = "Log file path", required = True)

    args = parser.parse_args()
    global log_file
    log_file = args.log
    try: 
        while True:
            sync_folders(args.source, args.replica, args.log) # perform synhronization
            time.sleep(args.interval) # wait for the specified interval before the next sync
    except KeyboardInterrupt:
        print("Synchronization stopped") 

if __name__ == "__main__":
    main()

import argparse
from configparser import ConfigParser
import datetime
import errno
import logging
import os
import shlex
import shutil
import subprocess

class DBbackup():

    def __init__(self):
        # instantiate config parser
        self.config = ConfigParser()
        # parse existing configuration file
        cfg_file_path = os.path.abspath(os.path.join(os.getenv('HOME'),
                                                     'stx_cfg.ini'))
        self.config.read(cfg_file_path)

    def backup_database(self, overwrite, usb_only):
        if usb_only:
            logging.info('No DB backup, only copy existing backups to USB')
            return
        '''get DB name from config file'''
        db_name = self.config.get('postgres_db', 'db_name')
        '''ensure root backup directory is created'''
        db_backup_dir = os.path.join(os.getenv('HOME'), 'db_backup', db_name)
        try:
            os.makedirs(db_backup_dir)
            logging.info('Creating directory {0:s}'.format(db_backup_dir))
        except OSError as e:
            if e.errno != errno.EEXIST:
                logging.error('Exception while creating {0:s}: {1:s}'.
                              format(db_backup_dir, str(e)))
                raise
        db_bkp_dirs = sorted(os.listdir(db_backup_dir))
        db_bkp_dirs_str = '\n  '.join(db_bkp_dirs)
        logging.debug(f"db_bkp_dirs = ")
        logging.debug(f"  {db_bkp_dirs_str}")
        
        ''' If overwrite was specified, remove all DB backup files in the last
        backup directory, otherwise, create a new backup directory,
        named after current timestamp '''
        if overwrite:
            db_bkp_dir = os.path.join(db_backup_dir, db_bkp_dirs[-1])
            logging.info(f'Overwrite {db_name} DB backup.  '
                         f'Remove all previous backup files in {db_bkp_dir}')
            for filename in os.listdir(db_bkp_dir):
                file_path = os.path.join(db_bkp_dir, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    print('Failed to delete %s. Reason: %s' % (file_path, e))
        else:
            crt_date = datetime.datetime.now()
            db_bkp_dir = os.path.join(db_backup_dir,
                                      crt_date.strftime('%Y-%m-%d_%H%M%S'))
            try:
                os.makedirs(db_bkp_dir)
                logging.info('Created directory {0:s}'.format(db_bkp_dir))
            except OSError as e:
                if e.errno != errno.EEXIST:
                    logging.error('Exception while creating {0:s}: {1:s}'.
                                  format(db_bkp_dir, str(e)))
                    raise
        logging.info(f'Backing up DB {db_name} in {db_bkp_dir}')
        '''launch the subprocesses that back up the database'''
        try:
            cmd1 = 'sudo -u postgres pg_dump -Fc {0:s}'.format(db_name)
            cmd2 = 'split -b 1000m - {0:s}/{1:s}'.format(db_bkp_dir,
                                                         db_name)
            p1 = subprocess.Popen(shlex.split(cmd1),
                                  stdout=subprocess.PIPE,
                                  cwd=db_bkp_dir)
            output = subprocess.check_output(shlex.split(cmd2),
                                             stdin=p1.stdout,
                                             cwd=db_bkp_dir)
            res = p1.wait()
            logging.info(f'Backed up DB, return status: {res}')
        except subprocess.CalledProcessError as cpe:
            logging.error(f'Database backup failed: {cpe}')
            '''if backup failed, remove the backup directory'''
            try:
                shutil.rmtree(db_bkp_dir)
            except OSError as e:
                logging.error(f"Error: {e.filename} - {e.strerror}")

    def usb_backup_database(self, overwrite):
        logging.info('Starting USB backup')
        '''get DB name from config file'''
        db_name = self.config.get('postgres_db', 'db_name')
        db_backup_dir = os.path.join(os.getenv('HOME'), 'db_backup', db_name)
        try:
            os.makedirs(db_backup_dir)
            logging.info('Creating directory {0:s}'.format(db_backup_dir))
        except OSError as e:
            if e.errno != errno.EEXIST:
                logging.error('Exception while creating {0:s}: {1:s}'.
                              format(db_backup_dir, str(e)))
                raise
        db_bkp_dirs = sorted(os.listdir(db_backup_dir))
        if not db_bkp_dirs:
            logging.warn(f'No backup directories found for {db_name} DB')
            logging.warn('Nothing to do, exiting')
            return
        db_bkp_dir = db_bkp_dirs[-1]
        db_bkp_dir_path = os.path.join(db_backup_dir, db_bkp_dir)
        logging.info(f'Last DB backup is at {db_bkp_dir_path}')
        usb_list = [
            self.config.get('postgres_db', 'usb_1'),
            self.config.get('postgres_db', 'usb_2'),
            self.config.get('postgres_db', 'usb_3')
        ]
        for usb in usb_list:
            if not os.path.exists(usb):
                logging.info(f'{usb} not found; skipping')
                continue
            logging.info(f'Backing up {db_name} DB to {usb} USB')
            usb_backup_dir = os.path.join(usb, 'db_backup', db_name)
            try:
                os.makedirs(usb_backup_dir)
                logging.info('Creating directory {0:s}'.format(usb_backup_dir))
            except OSError as e:
                if e.errno != errno.EEXIST:
                    logging.error(
                        f'Exception creating {db_backup_dir}: {str(e)}')
                    continue
            usb_db_bkp_dirs = sorted(os.listdir(usb_backup_dir))
            usb_db_bkp_dir_path = os.path.join(usb_backup_dir, db_bkp_dir)
            if db_bkp_dir in usb_db_bkp_dirs:
                if overwrite:
                    ''' remove the previous usb db backup at this location
                    and copy the db backup from the pc '''
                    logging.info(
                        f'Removing last DB backup at {usb_db_bkp_dir_path}')
                    shutil.rmtree(usb_db_bkp_dir_path)
                    logging.info(
                        f'Copying DB backup from {db_bkp_dir_path} '
                        f'to {usb_db_bkp_dir_path}')
                    shutil.copytree(db_bkp_dir_path, usb_db_bkp_dir_path)
                else:
                    logging.info(f'{usb_db_bkp_dir_path} exists, skipping')
            else:
                logging.info(
                    f'Copying DB backup from {db_bkp_dir_path} '
                    f'to {usb_db_bkp_dir_path}')
                shutil.copytree(db_bkp_dir_path, usb_db_bkp_dir_path)

'''

Default: create a new database backup directory, named using the
current timestamp, dump DB contents in the backup directory, copy DB
backup to the connected USBs.

Overwrite: create a database backup that will overwrite last backup,
on computer and connected USBs, if the USB have that backup directory.
If backup directory not found on USBs, create new directory.

USB only: only copy database backup to USBs.

'''
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--overwrite', action='store_true',
                        help='overwrite last DB backup')
    parser.add_argument('-u', '--usb_only', action='store_true',
                        help='only copy missing DB backups to USB')
    args = parser.parse_args()
    logging.basicConfig(
        format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - '
        '%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )

    dbbkp = DBbackup()
    dbbkp.backup_database(args.overwrite, args.usb_only)
    dbbkp.usb_backup_database(args.overwrite)

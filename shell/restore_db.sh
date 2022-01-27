#!/bin/bash

if [ -z "$1" ]
then
    echo "No database name supplied, exiting ..."
    exit 0
else
    DB_NAME=$1
fi

if [ -z "$2" ]
then
    echo "No backup name supplied, using database name $DB_NAME"
    BACKUP_NAME=$DB_NAME
else
    BACKUP_NAME=$2
fi

if [ -z "$DB_BACKUP_DIR" ]
then
    echo "Prior to running this script, define the DB_BACKUP_DIR variable"
    echo "No database backup directory defined, exiting ..."
    exit 0
else
    echo -e 'Concatenating backup files into a single file'
    cat $DB_BACKUP_DIR/$BACKUP_NAME* > /tmp/all
fi

echo "Remove existing $DB_NAME database"
dropdb $DB_NAME

echo "Creating database $DB_NAME, if non-existent"
createdb $DB_NAME

echo "Rebuilding database $DB_NAME from files in $DB_BACKUP_DIR/$BACKUP_NAME"
pg_restore -d $DB_NAME /tmp/all

echo "Deleting the backup concatenation file /tmp/all"
rm -f /tmp/all

echo "Run create_tables.exe to add any missing tables"
${HOME}/stx/c/create_tables.exe

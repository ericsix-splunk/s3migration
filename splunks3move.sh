#!/bin/bash
# For Splunk Cloud usage
#
# esix@splunk.com
# v1 2015/12/08

# set some global variables...
SplunkBucketImportPath=/opt/staging/
SplunkDBLocation=/opt/splunk/var/lib/splunk/
LogFile=/tmp/BucketCopy.log
S3CMD=`which s3cmd`


options=("backup to s3 bucket" "restore to s3 bucket" "move buckets to splunkDB")
PS3="Chose your options  : "

# backup to s3 function. s3cmd doesnt return error codes other then 0
function s3backup {
    clear
    echo " "
    echo "S3 Backup function"
    echo " This will copy all warm / cold buckets from /opt/splunk/var/lib/splunk/ to the designated s3bucket."
    echo " Be aware of what and where this is copying to, along with how long this can take."
    echo "................................................................................."
    echo " Enter the name of the s3 bucket to copy the index buckets to :"
    read S3BucketLocation
    echo " You entered : $S3BucketLocation "
    echo " This will be input as s3://$S3BucketLocation "
    echo " "
    	$S3CMD info s3://$S3BucketLocation 2>&1 | grep ERROR
	if [ $? -eq 1 ]; then
	 read -p "Bucket Exists, ready to copy. Press [Enter]..."
	else
          echo " ERROR with $S3BucketLocation "
	  read -p "Bucket doesnt exist.. please check again and confirm full path. Press [Enter]"
	  s3backup
	fi
    wait
    clear
    echo " "
    echo " From here, sync of all indexes will begin. "
    echo " The following Indexes will be Excluded : _internal, sos, fishbucket, persistentstorage, history, sos_summary_daily, splunklogger"
    echo "	summary, _introspectiondb, _introsection."
    echo " Addtionally the following will be Excldued :  hot buckets, data model summary data"
    echo " "
    echo "Starting copy, this will take time..... "
    echo "Starting S3 Bucket Copy to $S3BucketLocation" | tee -a $LogFile
    echo "Using s3cmd : s3cmd sync --dry-run --rexclude=\"(_internaldb|persistentstorage|fishbucket|sos|history|sos_summary_daily|splunklogger|summary|_introspectiondb|_introspection)
\/.*\""
    echo "  --rexclude=\"\/db\/hot.*\" --rexclude=\"\/datamodel_summary\/.*\" /opt/splunk/var/lib/spunk/ s3://$S3BucketLocation"
    $S3CMD sync --rexclude="(_internaldb|persistentstorage|fishbucket|sos|history|sos_summary_daily|splunklogger|summary|_introspectiondb|_introspection)\/.*" --rexclude="\/db\/hot.*"
 --rexclude="\/datamodel_summary\/.*" $SplunkDBLocation s3://$S3BucketLocation | tee -a $LogFile
    echo "Copy finished. " | tee -a $LogFile
    break
}

# this is the bucket restore - simple test logic to validate bucket. s3cmd doesnt return error codes >0
function s3restore {
    echo "S3 Restore Function"
    echo " This will restore all warm / cold buckets to /opt/temp/staging. After this is done, these should be "
    echo " manually copied to the correct index db path."
    echo ".................................................................................."
    echo " Enter the name of the s3 bucket to copy the index bucket from "
    read S3BucketLocation
    echo " You entered : $S3BucketLocation "
    echo " This will be input as s3://$S3BucketLocation "
    echo " "
        $S3CMD info s3://$S3BucketLocation 2>&1 | grep ERROR
        if [ $? -eq 1 ]; then
         read -p "Bucket Exists, ready to copy. Press [Enter]..."
        else
          echo " ERROR with $S3BucketLocation "
          read -p "Bucket doesnt exist.. please check again and confirm full path. Press [Enter]"
          s3restore
        fi
    wait
    clear
    echo " "
    echo " From here, sync of all indexes will begin. "
    echo " These will be copied to : $SplunkBucketImportPath."
    echo " "
    echo "Starting copy, this will take time....."
    echo "Starting S3 Bucket Copy from $S3BucketLocation to $SplunkBucketImportPath." | tee -a $LogFile
    $S3CMD sync s3://$S3BucketLocation $SplunkBucketImportPath | tee -a $LogFile
    echo "Copy finished. " | tee -a $LogFile
    break
}

# move migrated file to splunkDB directory
function movetosplunkDB {
    local SplunkDBPath=/opt/splunk/var/lib/splunk
    local RSYNC=`which rsync`
    clear
    echo " Index Bucket Restore Function" | tee -a $LogFile
    echo " This will restore all warm / cold buckets to splunk_home/var/lib/splunk/. After this is done, the " | tee -a $LogFile
    echo " indexer should be restarted and validate the buckets are readable." |  tee -a $LogFile
    echo ".................................................................................." |  tee -a $LogFile
    echo " **** Note these are single instance buckets in a Clustered Environment. These buckets" | tee -a $LogFile
    echo " **** will  not be replicated. " | tee -a $LogFile
    echo ".................................................................................." | tee -a $LogFile
    echo " This will copy from $SplunkBucketImportPath to $SplunkDBPath." | tee -a $LogFile
    echo "" | tee -a $LogFile
    wait
    echo "Starting local rsync to splunkdb path. " | tee -a $LogFile
    echo " SRC : $SplunkBucketImportPath    DST : $SplunkDBPath." | tee -a $LogFile
    echo " using : rsync --ignore-existing --update --dry-run $SplunkBucketImportPath $SplunkDBPath"
    $RSYNC -rvPS --update $SplunkBucketImportPath $SplunkDBPath 2>1& | tee -a $LogFile
    echo ".... rsync finished. " | tee -a $LogFile
    break
}



# lets make a logfile, with a timestamp.. perhaps we can splunk it
function createLog {
   local current_time=$(date "+%Y/%m/%d %H:%M:%S")
   echo "$current_time" | tee  $LogFile
   echo "Bucket Operations Log"  | tee -a $LogFile
   echo " " | tee -a $LogFile
}


clear
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
echo "> Splunk Cloud Migration Script          >"
echo "> Used for Rainmakr to Stackmakr Index   >"
echo "> bucket migration. Requires S3cmd       >"
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
echo " "
createLog
select choice in "${options[@]}" "quit"
do
    case $choice in
        "backup to s3 bucket")
        s3backup
        break
        ;;
        "restore to s3 bucket")
        s3restore
        break
        ;;
	"move buckets to splunkDB")
	movetosplunkDB
	break
	;;
        "quit")
        echo "exiting...."
        break
        ;;
        *) echo "invalid---";;
    esac
done



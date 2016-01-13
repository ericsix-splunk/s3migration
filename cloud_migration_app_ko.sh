#!/bin/bash
# cloud_migration_app_ko.sh ----  Migration of App Objects
# This script will recurse through $splunk_home/etc/apps and merge all files into one file
# that can be installed as an APP

SPLUNK_HOME=/opt/splunk
KO_HOME=/tmp/migrated_app_ko
LOOKUPS_HOME=$KO_HOME/lookups
DATE=`date`
AWKcmd=`which awk`
SEDcmd=`which sed`

echo "Making Migration App in $KO_HOME"
mkdir -p $KO_HOME/{local,lookups,default,metadata}/ >/dev/null 2>&1
mkdir -p $KO_HOME/local/data/ui/views/ > /dev/null 2>&1
# create a local meta to change ownership
printf '%s\n%s\n%s\n%s\n%s\n' '#default for everyone to read contents of this app' '# export all objects to system' '[]' 'access = read : [ * ], write : [ sc_admin, admin]' 'export = system' > $KO_HOME/metadata/local.meta
# create app.conf
printf '%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n' '##########################' '# Migrated Knowledge Objects APP' '# Change the build to date implemented' '######################' ' ' '[install]' 'is_configured = true' 'state = enabled' 'build = DATE' ' ' '[ui]' 'is_visible = true' 'label = CA - Migrated Knowledge Objects' ' ' '[launcher]' 'author = Splunk Cloud' ' version = 1.0' 'description = Migrated Knowledge Objects from Splunk Cloud Instance APPS' ' ' '[package]' 'id = migrated_app_ko' > $KO_HOME/default/app.conf

# Log file
printf '%s\n%s\n%s\n' '##### Migration Log File' '## Migration of APP Local configurations' '### Files Migrated ####' > $KO_HOME/migrated.txt
 
# start the real work
echo $DATE ----
echo $SPLUNK_HOME
echo "This will copy APP local knowledge objects..."
echo "## This means only contents in $APP/local and $APP/lookups "
echo " props / transforms / savedsearches / lookups / eventtypes / tags "
echo " "

# start copy of props
echo "Copying APP Props.."
find $SPLUNK_HOME/etc/apps -maxdepth 10 -name "props.conf" -type f  | grep -E "local\/" | while read -r dir
do
    echo "# " >> "$KO_HOME/local/props.conf"
    echo "# ${dir}" >> "$KO_HOME/local/props.conf"
    echo "# " >> "$KO_HOME/local/props.conf"
    cat "${dir}" >> "$KO_HOME/local/props.conf"
    echo "## ${dir}" >> "$KO_HOME/migrated.txt"
done

echo "Copying App Transforms"
find $SPLUNK_HOME/etc/apps -maxdepth 10 -name "transforms.conf" -type f  | grep -E "local\/" | while read -r dir
do
    echo "# " >> "$KO_HOME/local/transforms.conf"
    echo "# ${dir}" >> "$KO_HOME/local/transforms.conf"
    echo "# " >> "$KO_HOME/local/transforms.conf"
    cat "${dir}" >> "$KO_HOME/local/transforms.conf"
    echo "## ${dir}" >> "$KO_HOME/migrated.txt"
done

echo "Copying App Savessearches"
find $SPLUNK_HOME/etc/apps -maxdepth 10 -name "savedsearches.conf" -type f  | grep -E "local\/" | while read -r dir
do
    echo "# " >> "$KO_HOME/local/savedsearches.conf"
    echo "# ${dir}" >> "$KO_HOME/local/savedsearches.conf"
    echo "# " >> "$KO_HOME/local/savedsearches.conf"
    cat "${dir}" >> "$KO_HOME/local/savedsearches.conf"
    echo "## ${dir}" >> "$KO_HOME/migrated.txt" 
    # Need to delete any disabled = flags and disable all saved searches...
    $SEDcmd -e '/^disabled /{N;d;}'  $KO_HOME/local/savedsearches.conf > $KO_HOME/local/temp.conf
    $AWKcmd '/^\[([^\]]+)\]/ { print;print "disabled = 1";next }1' $KO_HOME/local/temp.conf > $KO_HOME/local/savedsearches.conf
    rm $KO_HOME/local/temp.conf
done

echo "Copying App Eventtypes"
find $SPLUNK_HOME/etc/apps -maxdepth 10 -name "eventtypes.conf" -type f  | grep -E "local\/" | while read -r dir
do
    echo "# " >> "$KO_HOME/local/eventtypes.conf"
    echo "# ${dir}" >> "$KO_HOME/local/eventtypes.conf"
    echo "# " >> "$KO_HOME/local/eventtypes.conf"
    cat "${dir}" >> "$KO_HOME/local/eventtypes.conf"	
    echo "## ${dir}" >> "$KO_HOME/migrated.txt"
done

echo "Copying App TAGs"
find $SPLUNK_HOME/etc/apps -maxdepth 10 -name "tags.conf" -type f  | grep -E "local\/" | while read -r dir
do
    echo "# " >> "$KO_HOME/local/tags.conf"
    echo "# ${dir}" >> "$KO_HOME/local/tags.conf"
    echo "# " >> "$KO_HOME/local/tags.conf"
    cat "${dir}" >> "$KO_HOME/local/tags.conf"
    echo "## ${dir}" >> "$KO_HOME/migrated.txt"
done

#echo "Copying APP Lookups"
#find $SPLUNK_HOME/etc/apps -maxdepth 10 -name "lookups" -type d  | while read -r dir
#do
#    echo "# ${dir}"
#    cp `find $SPLUNK_HOME/etc/apps -maxdepth 10 -type f -name "*.csv" -print | grep -E "lookups\/"` $LOOKUPS_HOME
#    echo "## ${dir}" >> "$KO_HOME/migrated.txt"
#done

####    Create tarball of app
tar -C /tmp -cvf migrated_ko.tgz migrated_app_ko

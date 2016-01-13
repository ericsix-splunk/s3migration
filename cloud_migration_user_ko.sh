#!/bin/bash
# cloud_migration_user_ko.sh ----  Migration of User Objects
# This script will recurse through $splunk_home/etc/users and merge all files into one file
# that can be installed as an APP

SPLUNK_HOME=/opt/splunk
KO_HOME=/tmp/migrated_user_ko
LOOKUPS_HOME=$KO_HOME/lookups
DATE=`date`
AWKcmd=`which awk`
SEDcmd=`which sed`

echo "Making Migration App in $KO_HOME"
mkdir -p $KO_HOME/{appserver,local,lookups,default,metadata}/ >/dev/null 2>&1
mkdir -p $KO_HOME/appserver/static/html > /dev/null 2>&1
mkdir -p $KO_HOME/local/data/ui/{nav,views}/ > /dev/null 2>&1

# create a local meta to change ownership
printf '%s\n%s\n%s\n%s\n%s\n' '#default for everyone to read contents of this app' '# export all objects to system' '[]' 'access = read : [ * ], write : [ sc_admin, admin]' 'export = system' > $KO_HOME/metadata/local.meta

# create app.conf
printf '%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n' '##########################' '# Migrated Knowledge Objects APP' '# Change the build to date implemented' '######################' ' ' '[install]' 'is_configured = true' 'state = enabled' 'build = DATE' ' ' '[ui]' 'is_visible = true' 'label = CA - Migrated Knowledge Objects' ' ' '[launcher]' 'author = Splunk Cloud' ' version = 1.0' 'description = Migrated User Knowledge Objects from Splunk Cloud' ' ' '[package]' 'id = migrated_user_ko' > $KO_HOME/default/app.conf

# create migration view
printf '%s\n' '<dashboard>' > $KO_HOME/local/data/ui/views/migrated_objects.xml
printf '%s\n' '<label>Migrated User Objects</label><description>Migrated User Objects</description><row>' >> $KO_HOME/local/data/ui/views/migrated_objects.xml
#printf '%s\n' '<html><h1>Migrated User Objects</h1>' >> $KO_HOME/local/data/ui/views/migrated_objects.xml
printf '%s\n' '<html src="html/migrated.html"></html></row></dashboard>' >> $KO_HOME/local/data/ui/views/migrated_objects.xml

# create navigation view
printf '%s\n' '<nav  color="#2175d9">' > $KO_HOME/local/data/ui/nav/default.xml
printf '%s\n' '<view name="migrated_objects" default="true"/>' >> $KO_HOME/local/data/ui/nav/default.xml
printf '%s\n' '<view name="search" />' >> $KO_HOME/local/data/ui/nav/default.xml
printf '%s\n' '<view name="dashboards" />' >> $KO_HOME/local/data/ui/nav/default.xml
printf '%s\n' '</nav>' >> $KO_HOME/local/data/ui/nav/default.xml

# Log file
printf '%s\n%s\n' '##### Migration Log File<br>' '### Files Migrated ####<br>' > $KO_HOME/migrated.txt
 
# start the real work
echo $DATE ----
echo $SPLUNK_HOME
echo "This will copy user knowledge objects..."
echo " props / transforms / savedsearches / lookups / eventtypes / tags "
echo " "

# start copy of props
echo "Copying User Props.."
find $SPLUNK_HOME/etc/users -maxdepth 10 -name "props.conf" -type f  | while read -r dir
do
    echo "# " >> "$KO_HOME/local/props.conf"
    echo "# ${dir}" >> "$KO_HOME/local/props.conf"
    echo "# " >> "$KO_HOME/local/props.conf"
    cat "${dir}" >> "$KO_HOME/local/props.conf"
    echo "## ${dir} <br>" >> "$KO_HOME/migrated.txt"
done

echo "Copying User Transforms"
find $SPLUNK_HOME/etc/users -maxdepth 10 -name "transforms.conf" -type f  | while read -r dir
do
    echo "# " >> "$KO_HOME/local/transforms.conf"
    echo "# ${dir}" >> "$KO_HOME/local/transforms.conf"
    echo "# " >> "$KO_HOME/local/transforms.conf"
    cat "${dir}" >> "$KO_HOME/local/transforms.conf"
    echo "## ${dir} <br>" >> "$KO_HOME/migrated.txt"
done

echo "Copying Users Savessearches"
find $SPLUNK_HOME/etc/users -maxdepth 10 -name "savedsearches.conf" -type f  | while read -r dir
do
    echo "# " >> "$KO_HOME/local/savedsearches.conf"
    echo "# ${dir}" >> "$KO_HOME/local/savedsearches.conf"
    echo "# " >> "$KO_HOME/local/savedsearches.conf"
    cat "${dir}" >> "$KO_HOME/local/savedsearches.conf"
    echo "## ${dir} <br>" >> "$KO_HOME/migrated.txt"
    # Need to delete any disabled = flags and disable all saved searches...
    $SEDcmd -e '/^disabled /{N;d;}'  $KO_HOME/local/savedsearches.conf > $KO_HOME/local/temp.conf
    $AWKcmd '/^\[([^\]]+)\]/ { print;print "disabled = 1";next }1' $KO_HOME/local/temp.conf > $KO_HOME/local/savedsearches.conf
    rm $KO_HOME/local/temp.conf
done

echo "Copying Users Eventtypes"
find $SPLUNK_HOME/etc/users -maxdepth 10 -name "eventtypes.conf" -type f  | while read -r dir
do
    echo "# " >> "$KO_HOME/local/eventtypes.conf"
    echo "# ${dir}" >> "$KO_HOME/local/eventtypes.conf"
    echo "# " >> "$KO_HOME/local/eventtypes.conf"
    cat "${dir}" >> "$KO_HOME/local/eventtypes.conf"	
    echo "## ${dir} <br>" >> "$KO_HOME/migrated.txt"
done

echo "Copying Users TAGs"
find $SPLUNK_HOME/etc/users -maxdepth 10 -name "tags.conf" -type f  | while read -r dir
do
    echo "# " >> "$KO_HOME/local/tags.conf"
    echo "# ${dir}" >> "$KO_HOME/local/tags.conf"
    echo "# " >> "$KO_HOME/local/tags.conf"
    cat "${dir}" >> "$KO_HOME/local/tags.conf"
    echo "## ${dir} <br>" >> "$KO_HOME/migrated.txt"
done

echo "Copying Users Lookups"
find $SPLUNK_HOME/etc/users -maxdepth 10 -name "lookups" -type d  | while read -r dir
do
    echo "# ${dir}"
    cp `find $SPLUNK_HOME/etc/users -maxdepth 10 -type f -name "*.csv" -print | grep -E "lookups\/"` $LOOKUPS_HOME
    echo "## ${dir} <br>" >> "$KO_HOME/migrated.txt"
done

## Start copying of local props from the APPs.. this is dangerous...
# start copy of props
#echo "Copying App's Local Props.."
#find $SPLUNK_HOME/etc/apps -maxdepth 10 -name "props.conf" -type f  | while read -r dir
#do
#    echo "# " >> "$KO_HOME/local/props.conf"
#    echo "# ${dir}" >> "$KO_HOME/local/props.conf"
#    echo "# " >> "$KO_HOME/local/props.conf"
#    cat "${dir}" >> "$KO_HOME/local/props.conf"
#    echo "## ${dir}" >> "$KO_HOME/migrated.txt"
#done

# Copy log to dashboard view
cp $KO_HOME/migrated.txt $KO_HOME/appserver/static/html/migrated.html

echo ""
echo ""
echo " CHECK $SPLUNK_HOME/etc/system/local/ for additional configuration changes that should be copied "
echo " "
#ls -las $SPLUNK_HOME/etc/system/local
####    Create tarball of app
echo " "
echo "... Creating archive ... "
tar -C /tmp -cvf migrated_user_ko.tgz migrated_user_ko

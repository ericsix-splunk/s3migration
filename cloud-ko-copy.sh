#!/bin/bash
# This script will recurse through $splunk_home/etc/users and merge all files into one file
# that can be installed as an APP

SPLUNK_HOME=/opt/splunk
KO_HOME=/tmp/migrated_apps
LOOKUPS_HOME=$KO_HOME/lookups
DATE=`date`

echo "Making Migration App in $KO_HOME"
mkdir -p $KO_HOME/{local,lookups,default,metadata}/ >/dev/null 2>&1
mkdir -p $KO_HOME/local/data/ui/views/ > /dev/null 2>&1
# create a local meta to change ownership
printf '%s\n%s\n%s\n%s\n%s\n' '#default for everyone to read contents of this app' '# export all objects to system' '[]' 'access = read : [ * ], write : [ sc_admin, admin]' 'export = system' >
$KO_HOME/metadata/local.meta
# create app.conf
printf '%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n' '##########################' '# Migrated Knowledge Objects APP' '# Change the build to date imp
lemented' '######################' ' ' '[install]' 'is_configured = true' 'state = enabled' 'build = DATE' ' ' '[ui]' 'is_visible = true' 'label = CA - Migrated Knowledge Objects' ' ' '[launche
r]' 'author = Splunk Cloud' ' version = 1.0' 'description = Migrated Knowledge Objects from Splunk Cloud' ' ' '[package]' 'id = migrated_ko' > $KO_HOME/default/app.conf

# Log file
printf '%s\n%s\n' '##### Migration Log File' '### Files Migrated ####' > $KO_HOME/migrated.txt

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
    echo "## ${dir}" >> "$KO_HOME/migrated.txt"
done

echo "Copying User Transforms"
find $SPLUNK_HOME/etc/users -maxdepth 10 -name "transforms.conf" -type f  | while read -r dir
do
    echo "# " >> "$KO_HOME/local/transforms.conf"
    echo "# ${dir}" >> "$KO_HOME/local/transforms.conf"
    echo "# " >> "$KO_HOME/local/transforms.conf"
    cat "${dir}" >> "$KO_HOME/local/transforms.conf"
    echo "## ${dir}" >> "$KO_HOME/migrated.txt"
done

echo "Copying Users Savessearches"
find $SPLUNK_HOME/etc/users -maxdepth 10 -name "savedsearches.conf" -type f  | while read -r dir
do
    echo "# " >> "$KO_HOME/local/savedsearches.conf"
    echo "# ${dir}" >> "$KO_HOME/local/savedsearches.conf"
    echo "# " >> "$KO_HOME/local/savedsearches.conf"
    cat "${dir}" >> "$KO_HOME/local/savedsearches.conf"
    echo "## ${dir}" >> "$KO_HOME/migrated.txt"
done

echo "Copying Users Eventtypes"
find $SPLUNK_HOME/etc/users -maxdepth 10 -name "eventtypes.conf" -type f  | while read -r dir
do
    echo "# " >> "$KO_HOME/local/eventtypes.conf"
    echo "# ${dir}" >> "$KO_HOME/local/eventtypes.conf"
    echo "# " >> "$KO_HOME/local/eventtypes.conf"
    cat "${dir}" >> "$KO_HOME/local/eventtypes.conf"
    echo "## ${dir}" >> "$KO_HOME/migrated.txt"
done

echo "Copying Users TAGs"
find $SPLUNK_HOME/etc/users -maxdepth 10 -name "tags.conf" -type f  | while read -r dir
do
    echo "# " >> "$KO_HOME/local/tags.conf"
    echo "# ${dir}" >> "$KO_HOME/local/tags.conf"
    echo "# " >> "$KO_HOME/local/tags.conf"
    cat "${dir}" >> "$KO_HOME/local/tags.conf"
    echo "## ${dir}" >> "$KO_HOME/migrated.txt"
done

echo "Copying Users Lookups"
find $SPLUNK_HOME/etc/users -maxdepth 10 -name "lookups" -type d  | while read -r dir
do
    echo "# ${dir}"
    cp `find $SPLUNK_HOME/etc/users -maxdepth 10 -type f -name "*.csv" -print | grep -E "lookups\/"` $LOOKUPS_HOME
    echo "## ${dir}" >> "$KO_HOME/migrated.txt"
done


echo ""
echo ""
echo " CHECK $SPLUNK_HOME/etc/system/local/ for additional configuration changes that should be copied "
echo " "
#ls -las $SPLUNK_HOME/etc/system/local
####    Create tarball of app
tar -C /tmp -cvf migrated_ko.tgz migrated_apps


import splunk.clilib.cli_common
# Import with "from" to minimize changes to splunkDBPaths().
from splunk.clilib.info_gather import normalize_path
#from info_gather import normalize_path
import argparse, copy, hashlib, json, os, re, shutil, subprocess, sys, time
import collections, glob, hmac, logging, operator, requests, StringIO, socket, urllib
import pprint
import lxml.etree as et

# - Usage:
#   - Scan for files in paths.
#   - Next execute: Checksum stuff.
#   - Next execute: Do S3 uploads.
#     - Optional: Check MD5s before this.
#     - Must also upload checksums file.
#   - Then get list of all buckets in S3.
#   - Generate new owner list.
#   - Download.
#   - Verify checksums.
#   - Stop Splunk, move data into place, start Splunk.

# User configurable.  Don't include leading or trailing slashes.
# S3_BASE_PATH = "splunkcloud-citrix-prod-migration/Production"
S3_BASE_PATH = "splunkcloud-fb-migration/Production"

KEY_MD5HASH    = "md5Hash"
KEY_METADATA   = "metadata"
S3_URL_PREFIX  = "s3://"
S3_HOSTNAME = "s3.amazonaws.com"
XML_AWS_NS     = "http://s3.amazonaws.com/doc/2006-03-01/"
KEY_EXTRADATA  = "EXTRADATA"
KEY_CHKSUMLIST = "checksumlist"
MODE_UPLOAD    = "upload"
MODE_DOWNLOAD  = "download"
STAGING_DIR    = "S3_DATA_IMPORT_STAGING"

class IndexLocation(object):
    VALID_PATHKEYS = ('homePath', 'coldPath', 'thawedPath')
    def __init__(self, indexName, dirPath, dirType):
        if not indexName:
            raise Exception, "Bug: IndexLocation created with empty index name!"
        self.indexName = indexName
        if not dirPath:
            raise Exception, "Bug: IndexLocation created with empty dir!"
        self.dirPath = dirPath
        if dirType not in self.VALID_PATHKEYS:
            raise Exception, "Bug: IndexLocation created with invalid directory" \
                    " type=\"%s\"!" % dirType
        self.dirType = dirType

computed_db_paths = None
def splunkDBPaths(mergedIndexConf):
    """This is stolen directly from splunk.clilib.info_gather.py, with a few
       modifications so we can tell which buckets reside in warm vs cold.  Some
       less than ideal components are left to make a later diff easier to read."""
    # if cached, return answer -- surprisingly computing this takes like 4 seconds
    global computed_db_paths
    if computed_db_paths:
        return computed_db_paths

    # first get all the index path config strings
    # Will contain IndexLocation objects.
    index_paths = []

    index_confs  = mergedIndexConf

    req_parm_warning = 'Indexing stanza [%s] is missing required parameter "%s"'

    volumes = {}
    pathKeys = ['homePath', 'coldPath', 'thawedPath']
    for stanza_name in index_confs.keys():
        if stanza_name == 'default':
            continue
        stanza = index_confs[stanza_name]
        # ignore disabled index stanzas
        if stanza.get('disabled') == "true":
            continue
        if stanza_name.startswith('volume:'):
            # skip broken volume groups
            if not stanza.has_key('path'):
                logging.warn("The indexing volume %s does not have a path defined, this is an error." % (stanza_name))
                continue
            volumes[stanza_name] = stanza['path']
        # ignore all virtual indexes for diag-building purposes, but warn if they seem broken
        elif stanza_name.startswith('provider-family:'):
            if not stanza.has_key('vix.mode'):
                logging.warn(req_parm_warning % (stanza_name, 'vix.mode'))
            if not stanza.has_key('vix.command'):
                logging.warn(req_parm_warning % (stanza_name, 'vix.command'))
            continue
        elif stanza_name.startswith('provider:'):
            if not stanza.has_key('vix.family'):
                logging.warn(req_parm_warning % (stanza_name, 'vix.family'))
            continue
        elif stanza.has_key("vix.provider"):
            logging.info('Virtual index "%s" found, not scanning for diag.' % stanza_name)
            continue
        # it's an index definition, get the paths
        else:
            for pathKey in pathKeys:
                if not stanza.has_key(pathKey):
                    logging.warn("The index %s does not have a value set for %s, this is unusual." % (stanza_name, pathKey))
                else:
                    index_paths.append(IndexLocation(stanza_name,
                        stanza.get(pathKey), pathKey))

    def expand_vol_path(orig_path, volumes=volumes):
        if not orig_path.startswith('volume:'):
            return orig_path
        tmp_path = orig_path
        if os.name == "nt" and  (not '\\' in tmp_path) and ('/' in tmp_path):
            tmp_path = orig_path.replace('/','\\')
        if not os.path.sep in tmp_path:
            logging.warn("Volume based path '%s' contains no directory seperator." % orig_path)
            return None
        volume_id, tail = tmp_path.split(os.path.sep, 1)
        if not volume_id in volumes:
            logging.warn("Volume based path '%s' refers to undefined volume '%s'." % (orig_path, volume_id))
            return None
        return os.path.join(volumes[volume_id], tail)

    # detect and expand volume paths


    paths = copy.deepcopy(index_paths)
    for indexPath in paths:
        indexPath.dirPath = expand_vol_path(indexPath.dirPath)
    paths = filter(lambda x: x.dirPath, paths) # remove chaff from expand_vol_path
    for indexPath in paths:
        indexPath.dirPath = normalize_path(indexPath.dirPath)
    paths = filter(lambda x: x.dirPath, paths) # remove chaff from normalize_paths

    # cache answer
    computed_db_paths = paths

    return paths

def filter_internal_index_dirs(indexLocations):
    """For the purposes of moving indexed data to another instance, the below
       dirs tend to not be very useful.  (Fishbucket, internal, introspection..)"""
    # The slashes here are so we match a dir segment - don't confuse them with
    # regex syntax!
    # TODO: make this configurable at command line.
    blacklistRE = re.compile("/(fishbucket|_introspection)/")
    retList = []
    for thisPath in indexLocations:
        if None == blacklistRE.search(thisPath.dirPath):
            retList.append(thisPath)
    return retList

def failed_test(msg):
    sys.stderr.write("\nTEST FAILED: %s\n")
    sys.exit(1)

class BucketCollection(object):
    KEY_INDEXNAME = "indexName"
    KEY_DIRTYPE   = "dirType"
    KEY_BUCKETPARENTDIR = "bucketParentDir"
    KEY_BUCKETNAME      = "bucketName"
    def __init__(self):
        self._bucketCount = 0
        # Structure is { index_name : { hot/warm/thaw : { path : buckets } } }.
        self._bucketInfo = {}
    def __repr__(self):
        return pprint.pformat(self._bucketInfo)
        #return repr(self._bucketInfo)
    def __len__(self):
        return self._bucketCount
    def addBuckets(self, indexName, bucketType, parentDir, bucketDirs):
        if isinstance(bucketDirs, basestring):
            bucketDirs = (bucketDirs,)
        # Create dict for this index if DNE.
        if not indexName in self._bucketInfo:
            self._bucketInfo[indexName] = {}
        # Create hot/warm/thaw bucket dict if DNE.
        if not bucketType in self._bucketInfo[indexName]:
            self._bucketInfo[indexName][bucketType] = {}

        # Finally, save list of buckets in said dict.
        if not parentDir in self._bucketInfo[indexName][bucketType]:
            self._bucketInfo[indexName][bucketType][parentDir] = []
                
        for bucket in bucketDirs:
            # Don't dupe... helps populating this thing from S3, which lists
            # full file paths rather than directories and such.
            if bucket in self._bucketInfo[indexName][bucketType][parentDir]:
                continue
            self._bucketInfo[indexName][bucketType][parentDir].append(bucket)
            self._bucketCount += 1
            # print bucket
            # print self._bucketCount

    def items(self):
        for indexName, indexInfo in self._bucketInfo.items():
            for dirType, indexLocs in indexInfo.items():
                for bucketParentDir, buckets in indexLocs.items():
                    for bucketName in buckets:
                        yield (indexName, dirType, bucketParentDir, bucketName)
    def indexNames(self):
        return _bucketInfo.keys()
    def toFile(self, filePath):
        """Atomic write by way of temp file.  File format is one json object per line."""
        tmpPath = filePath + ".tmp"
        with open(tmpPath, "w") as outFd:
            for indexName, dirType, bucketParentDir, bucketName in self.items():
                outDict = {self.KEY_INDEXNAME : indexName,
                           self.KEY_DIRTYPE : dirType,
                           self.KEY_BUCKETPARENTDIR : bucketParentDir,
                           self.KEY_BUCKETNAME : bucketName}
                # Sort for easier readability/comparisons.
                outFd.write("%s\n" % json.dumps(outDict, sort_keys=True))
        shutil.move(tmpPath, filePath)
    def fromFile(self, filePath):
        """Hope nobody's corrupted you..."""
        inFd = open(filePath, "r")
        for line in inFd:
            inDict = json.loads(line.strip())
            self.addBuckets( inDict[self.KEY_INDEXNAME],
                             inDict[self.KEY_DIRTYPE],
                             inDict[self.KEY_BUCKETPARENTDIR],
                            (inDict[self.KEY_BUCKETNAME],))

class TransferMetadata(object):
    """Per-file info.  Currently this just holds checksums."""
    _md5Hash = None
    def __init__(self, kvDict):
        # TODO: validate contents look reasonable.
        # TODO: complain about any unrecognized data.
        # XXX ...
        if KEY_MD5HASH in kvDict:
            self._md5Hash = kvDict[KEY_MD5HASH]
    def md5Hash(self):
        return self._md5Hash
    def setMd5Hash(self, md5Hash):
        self._md5Hash = md5Hash
    def asDict(self):
        # Aim to not save incomplete data structures!  Indicates user error.
        # TODO: Maybe we need this so can do scan, then checksum...
        if not self._md5Hash:
            raise Exception, "Bug: Attempting to serialize metadata without computing MD5 checksum."
        return {KEY_MD5HASH : self._md5Hash}

class TransferMetadataCollection(object):
    """Complete list of buckets, their metadata, etc to be transferred."""
    _metadataDict = {}
    _filePath = None
    def __contains__(self, filePath):
        return filePath in self._metadataDict
    def add(self, filePath, metadata):
        """Add metadata for a file."""
        self._metadataDict[filePath] = TransferMetadata(metadata)
    def __init__(self, filePath):
        """Load existing metadata from disk."""
        self._filePath = filePath
        data = splunk.clilib.cli_common.readConfFile(self._filePath)
        # File does not exist, or empty for some reason.
        if not data or (1 == len(data) and "default" in data):
            return
        for filePathStanza, metadata in data.items():
            self.add(filePathStanza, metadata)
    def scan_for_new_files(self, bucketCollection):
        """Scan the bucket parent dirs for any files that we don't already know
           about.  Any missing files will be added to our collection."""
        for indexName, dirType, bucketParentDir, bucketName in bucketCollection.items():
            bucketPath = os.path.join(bucketParentDir, bucketName)
            # Recursive file listing.
            for dirPath, subdirs, files in os.walk(bucketPath):
                for file in files:
                    filePath = os.path.join(dirPath, file)
                    if not filePath in self:
                        self.add(filePath, {}) # Empty metadata for now.
    def generate_missing_checksums(self):
        """Generate MD5 checksums for any files in our existing list that don't
           have them populated.  Note that this does not cause a rescan, but
           after the first requested scan, this will result in checksumming all
           files.  Also ote that this function does not verify existing
           populated checksums."""
        # Check for and generate MD5s.
        for filePath, metadata in self._metadataDict.items():
            if not metadata.md5Hash():
                print "Generating MD5 for file=\"%s\"..." % filePath
                fd = open(filePath, "rb")
                metadata.setMd5Hash(fileMd5Hash(fd))
    def write(self):
        """Save built up dict to disk in one shot.  Not an incremental write,
           but does do an atomic overwrite of the dest file (by way of temp
           file)."""
        writeableDict = {}
        for filePath, metadata in self._metadataDict.items():
            writeableDict[filePath] = metadata.asDict()
        tmpFilePath = self._filePath + ".tmp"
        splunk.clilib.cli_common.writeConfFile(tmpFilePath, writeableDict)
        # Success!  Simulate atomic write.
        shutil.move(tmpFilePath, self._filePath)

### def make_metafile_path(instanceGuid):
###     mfPath = os.path.expanduser(os.path.join("~", "/splunk_bucketfiles_%s.conf" % instanceGuid))
###     if mfPath.startswith("~"):
###         raise Exception, "Could not lookup home dir while creating metafile path!"
###     return mfPath

def fileMd5Hash(fd):
    hasher = hashlib.md5()
    while True:
        data = fd.read(65536)
        if not data:
            break
        hasher.update(data)
    return hasher.hexdigest()

### def generate_s3_upload_commands(instanceGuid, bucketCollection):
###     for indexName, dirType, bucketParentDir, bucketName in bucketCollection.items():
###         print "aws s3 sync %s s3://amrit-test/nike_cluster_indexes/%s/%s/%s/%s" % (
###                 os.path.join(bucketParentDir, bucketName),
###                 indexName, dirType, instanceGuid, bucketName)

def get_instance_guid():
    instanceCfg = splunk.clilib.cli_common.readConfFile(
            os.path.join(splunk.clilib.cli_common.splunk_home, "etc", "instance.cfg"))
    return instanceCfg["general"]["guid"]

class CMD_GenBucketlistWarmCold(object):
    def run(self, args):
        self._args = args
        # Convenience...
        self.instance_guid = get_instance_guid()

        allBuckets = self.get_instance_buckets()
        dstPath = self.make_bucketlist(self.instance_guid)
        allBuckets.toFile(dstPath)
        print "\nSaved bucket listing to file=\"%s\"." % dstPath
        if self._args.print_all:
            pprint.pprint(allBuckets)
    def __init__(self, destSubParser, cmdName):
        myParser = destSubParser.add_parser(cmdName)
        myParser.set_defaults(func=self.run)
        myParser.add_argument("--no-warn-on-hots", action="store_true",
                help="Do not warn if any hot buckets are found.")
        myParser.add_argument("--ignore-manifests", action="store_true",
                help="Don't store .bucketManifest contents (these usually are not needed).")
        myParser.add_argument("--indexes", default="",
                help="CSV whitelist of names of indexes to dump.")
        myParser.add_argument("--print-all", action="store_true",
                help="Also print all index paths that will be processed, and the resulting buckets.")
    def make_bucketlist(self, instanceGuid):
        return "splunk_bucketlist_%s.json" % instanceGuid
    def filter_to_requested_indexes(self, indexLocations, indexWhitelist):
        if not indexWhitelist:
            return indexLocations
        filtered = []
        for index in indexLocations:
            if index.indexName in indexWhitelist:
                filtered.append(index)
        return filtered
    def get_instance_buckets(self):
        """Returns a BucketCollection."""
        indexLocations = splunkDBPaths(splunk.clilib.cli_common.getMergedConf('indexes'))
        ### TODO: review what's being filtered out.
        indexLocations = filter_internal_index_dirs(indexLocations)
        indexLocations = self.filter_to_requested_indexes(indexLocations, self._args.indexes)
        if self._args.print_all:
            print "Enabled index paths:\n\t%s\n" % str.join("\n\t", 
                    [ x.dirPath for x in indexLocations ])
        return self.enumerate_buckets_in_dirs(indexLocations)
    def enumerate_buckets_in_dirs(self, indexLocations):
        """Returns a BucketCollection."""
        IGNORE_FILES = ["GlobalMetaData", "CreationTime"]
        bucketCollection = BucketCollection()

        if not self._args.ignore_manifests:
            timeStr = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
            manifestBaseDir = os.path.join("splunk_bucketmanifests",
                    self.instance_guid, timeStr)
            print "Will save bucket manifests to dir=\"%s\".\n" % manifestBaseDir
            os.makedirs(manifestBaseDir)

        for thisIndexLoc in indexLocations:
            bucketDirs = []
            dirEntries = os.listdir(thisIndexLoc.dirPath)
            for thisEntry in dirEntries:
                if thisEntry.startswith("db_") or thisEntry.startswith("rb_"):
                    bucketDirs.append(thisEntry)
                # Store manifest in splunk_bucketmanifests/guid/time/index/dirType/.
                elif ".bucketManifest" == thisEntry:
                    if self._args.ignore_manifests:
                        continue
                    dstDir = os.path.join(manifestBaseDir,
                            thisIndexLoc.indexName, thisIndexLoc.dirType)
                    os.makedirs(dstDir)
                    # Copy to non-dot filename for easy viewing.
                    shutil.copy(os.path.join(thisIndexLoc.dirPath, ".bucketManifest"),
                            os.path.join(dstDir, "bucketManifest"))
                # These non-bucket files don't need to be transferred, per VP.
                elif thisEntry in IGNORE_FILES:
                    pass
                # Hot bucket or streaming/replicating hot bucket!
                elif thisEntry.startswith("hot_") or thisEntry[0].isdigit():
                    if self._args.no_warn_on_hots:
                        continue
                    print "*** WARNING: Ignoring hot bucket=\"%s\" ***" % \
                            os.path.join(thisIndexLoc.dirPath, thisEntry)
                else:
                    print "*** ODD PATH FOUND: %s ***" % \
                            os.path.join(thisIndexLoc.dirPath, thisEntry)
            if 0 != len(bucketDirs):
                bucketCollection.addBuckets(thisIndexLoc.indexName,
                        thisIndexLoc.dirType, thisIndexLoc.dirPath, bucketDirs)
        return bucketCollection


class CMD_GenChecksumsFromBucketlist(object):
    def run(self, args):
        self._args = args
        # Convenience...
        self.instance_guid = get_instance_guid()

        knownBuckets = BucketCollection()
        knownBuckets.fromFile(self._args.input_file)
        self.process_all_files(knownBuckets)
        ### pprint.pprint(knownBuckets)
        ### metaPath = make_metafile_path(instanceGuid)
        ### metadataCollection = TransferMetadataCollection(metaPath)
        ### metadataCollection.scan_for_new_files(allBuckets)
    def __init__(self, destSubParser, cmdName):
        myParser = destSubParser.add_parser(cmdName)
        myParser.set_defaults(func=self.run)
        myParser.add_argument("input_file")
    def make_checksum_filename(self, instanceGuid):
        return "splunk_%s_%s.md5" % (KEY_CHKSUMLIST, instanceGuid)
    def split_whole_path(self, pathToSplit):
        """Python, why you make me do this..."""
        pieces = []
        while True:
            parents, pathSeg = os.path.split(pathToSplit)
            if parents == pathToSplit:
                pieces.append(pathToSplit)
                break
            pieces.append(pathSeg)
            pathToSplit = parents
        pieces.reverse()
        return pieces
    def bucket_item_rel_path(self, bucketPath, bucketName):
        pieces = self.split_whole_path(bucketPath)
        bucketNameIdx = pieces.index(bucketName)
        # db_1417732610_1417300621_105/rawdata -> _internaldb/db/db_1417732610_1417300621_105/rawdata
        indexNameIdx = bucketNameIdx - 2
        return os.path.join(*pieces[indexNameIdx:])
    def process_all_files(self, knownBuckets):
        fn = self.make_checksum_filename(self.instance_guid)
        print "Writing to path=%s ..." % fn
        outFd = open(fn, "w")
        sys.stdout.write("Working...")
        for index, dirType, dirPath, bucketName in knownBuckets.items():
            for parentDir, subdirs, files in os.walk(os.path.join(dirPath, bucketName)):
                # DEBUG print parentDir, subdirs, files
                for file in files:
                    bucketItemPath = os.path.join(parentDir, file)
                    itemRelPath = self.bucket_item_rel_path(bucketItemPath, bucketName)
                    with open(bucketItemPath, "r") as inFd:
                        # Two spaces here are important for "md5sum -c" to later work!
                        outFd.write("%s  %s\n" % (fileMd5Hash(inFd), itemRelPath))
                        sys.stdout.write(".")
                        sys.stdout.flush()
        sys.stdout.write(" Done!\n\n")


class RoundRobinFileReader(object):
    """Very simple way of round robining line-read requests across a set of
       files.  Note that for a large set of passed-in files, this implementation
       will result in many file descriptors being open concurrently (one per)..."""
    def numFiles(self):
        return len(self._fdList)
    def __init__(self, rawFDs):
        """Accepts list of readable FDs.  This used to take in actual filepaths
           but I changed it to FDs just to make testing a little easier.  Could
           still use another interface for taking filepaths."""
        self._fdList = rawFDs
        self._curFdIdx = 0
        self._closedFds = 0
    def readline(self):
        while True:
            curFd = self._fdList[self._curFdIdx]
            # Already saw EOF on this fd.
            if curFd.closed:
                # Next fd...
                self._curFdIdx = (self._curFdIdx + 1) % self.numFiles()
                continue
            line = curFd.readline()
            # EOF, close this fd.
            if 0 == len(line):
                curFd.close()
                self._closedFds += 1
                # Are all files at EOF?
                if self.numFiles() == self._closedFds:
                    # Return EOF for entire file set.
                    return ""
                # Next fd...
                self._curFdIdx = (self._curFdIdx + 1) % self.numFiles()
                continue
            self._curFdIdx = (self._curFdIdx + 1) % self.numFiles()
            return line
    def __iter__(self):
        """Returns lines until we're out."""
        while True:
            line = self.readline()
            # EOF.
            if 0 == len(line):
                return
            yield line


class CMD_GenRandBucketOwnersFromBucketList(object):
    class AssumedPrimaryBucket(object):
        def __init__(self, jsonBlob):
            attrDict = json.loads(jsonBlob)
            bucketDirName = attrDict[BucketCollection.KEY_BUCKETNAME]
            self.bucketId = str.join("_", bucketDirName.split("_")[1:4])
            self.instanceGuid = bucketDirName.split("_")[4]
            self.indexName = attrDict[BucketCollection.KEY_INDEXNAME]
    class PrimaryBucketCollection(object):
        # { bucketId : { indexName : primaryInstanceGuid } }
        _bucketDict = {}
        def maybeAdd(self, bucket):
            """Returns True if bucket is now stored with this instance GUID as
               primary owner."""
            if not bucket.bucketId in self._bucketDict:
                self._bucketDict[bucket.bucketId] = {}
            if not bucket.indexName in self._bucketDict[bucket.bucketId]:
                self._bucketDict[bucket.bucketId][bucket.indexName] = bucket.instanceGuid
                return True
            return False
        def toFile(self, path):
            fd = open(path, "w")
            for buckId, buckAttrs in self._bucketDict.items():
                for buckName, buckOwnerGuid in buckAttrs.items():
                    fd.write("%s\n" % json.dumps(
                        {buckId : {buckName : buckOwnerGuid}}))
    def run(self, args):
        self._args = args
        if 1 == len(self._args.filenames):
            raise Exception, "This command does not make sense if only one bucketlist file is specified (you specified: %s)" % self._args.filenames[0]
        rawFDs = [open(filepath, "r") for filepath in self._args.filenames]
        rrFd = RoundRobinFileReader(rawFDs)
        primBuckColl = self.PrimaryBucketCollection()
        guidCounter = {}
        for line in rrFd:
            apb = self.AssumedPrimaryBucket(line)
            if primBuckColl.maybeAdd(apb):
                if not apb.instanceGuid in guidCounter:
                    guidCounter[apb.instanceGuid] = 1
                else:
                    guidCounter[apb.instanceGuid] += 1
        print "Stats for created bucket listing:"
        for instance, count in guidCounter.items():
            print "\tInstance=%s is assumed primary for num_buckets=%d." % (instance, count)
        outPath = "splunk_bucketowners.json"
        print "Saving bucket list to path=%s." % outPath
        primBuckColl.toFile(outPath)
    def __init__(self, destSubParser, cmdName):
        myParser = destSubParser.add_parser(cmdName)
        myParser.set_defaults(func=self.run)
        myParser.add_argument("filenames", nargs="+")


def build_s3_download_bucket_command(destDir, s3Path):
    return ("aws", "s3", "sync",
            s3Path.rstrip(os.sep) + os.sep, # this prob doesn't matter, but...
            destDir.rstrip(os.sep) + os.sep)
def build_s3_upload_bucket_command(bucketPath, instanceGuid, indexName, dirType):
    """Kinda stolen from freezeBucketToS3.py (TODO: unify)."""
    bucketName = os.path.basename(bucketPath)
    destS3Uri = "s3://%s/%s/%s/%s/%s" % (
            S3_BASE_PATH.strip("/"),
            instanceGuid,
            indexName,
            dirType,
            bucketName)
    return ("aws", "s3", "sync", bucketPath, destS3Uri)
def build_s3_upload_extradata_command(itemPath, instanceGuid):
    """Kinda stolen from freezeBucketToS3.py (TODO: unify)."""
    uploadMode = os.path.isdir(itemPath) and "sync" or "cp"
    itemPath = os.path.basename(itemPath)
    destS3Uri = "s3://%s/%s/%s/%s" % (
            S3_BASE_PATH.strip("/"),
            instanceGuid,
            KEY_EXTRADATA,
            itemPath)
    return ("aws", "s3", uploadMode, itemPath, destS3Uri)
def run_s3_command(cmdPieces, upOrDown):
    """Kinda stolen from freezeBucketToS3.py (TODO: unify)."""
    assert upOrDown in (MODE_UPLOAD, MODE_DOWNLOAD)
    # 5 retries per upload command, because why not?
    for i in range(5):
        # Hacky. TODO
        sys.stdout.write("%s\n    %s=%s\n    %s=%s\n    ..." % (
            upOrDown == MODE_UPLOAD and "Uploading" or "Downloading",
            upOrDown == MODE_UPLOAD and "src" or "dst", cmdPieces[-2],
            upOrDown == MODE_UPLOAD and "dst" or "src", cmdPieces[-1]))
        sys.stdout.flush()
        # TODO: Do this a bit smarter.. have to make sure we call the system
        #       python here, totally fails on Ubuntu (but not RHEL6)...
        env = os.environ
        env["PATH"] = "/usr/local/bin:/usr/bin:/opt/ec/aws_client/bin"
        env["LD_LIBRARY_PATH"] = ""
        env["PYTHON_PATH"] = ""
        proc = subprocess.Popen(cmdPieces, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, shell=False, env=env)
        (stdout, stderr) = proc.communicate()

        # Success!
        if 0 == proc.returncode:
            sys.stdout.write(" Done!\n")
            sys.stdout.flush()
            return True

        sys.stdout.write("\n")
        sys.stdout.flush()
        sys.stderr.write("FAILED command %s !\n\n==> STDOUT:\n%s\n==> STDERR:\n%s\n\n" % (
            cmdPieces, stdout, stderr))
    # All 5 attempts failed... Caller should consider this fatal for entire job.
    return False


class CMD_UploadBucketsToS3FromBucketList(object):
    def __init__(self, destSubParser, cmdName):
        myParser = destSubParser.add_parser(cmdName)
        myParser.set_defaults(func=self.run)
    def run(self, args):
        self._args = args
        # Convenience...
        self.instance_guid = get_instance_guid()

        # MESSY! Use the builder functions... TODO
        inputFile = "splunk_bucketlist_%s.json" % self.instance_guid
        EXTRADATA = ("splunk_bucketmanifests",
                inputFile,
                # MESSY! Use the builder functions... TODO
                "splunk_%s_%s.md5" % (KEY_CHKSUMLIST, self.instance_guid))

        for file in EXTRADATA:
            if not os.path.exists(file):
                raise Exception, "File %s does not exist - are you sure you" \
                        " followed the directions?" % file

        knownBuckets = BucketCollection()
        knownBuckets.fromFile(inputFile)

        for file in EXTRADATA:
            cmdPieces = build_s3_upload_extradata_command(file, self.instance_guid)
            # DEBUG: print cmdPieces
            if not run_s3_command(cmdPieces, MODE_UPLOAD):
                raise Exception("Upload process failed, please triage and try again!")

        for index, dirType, dirPath, bucketName in knownBuckets.items():
            bucketDirPath = os.path.join(dirPath, bucketName)
            cmdPieces = build_s3_upload_bucket_command(bucketDirPath,
                    self.instance_guid, index, dirType)
            # DEBUG: print cmdPieces
            if not run_s3_command(cmdPieces, MODE_UPLOAD):
                raise Exception("Upload process failed, please triage and try again!")

        print "\nALL UPLOADS COMPLETE!\n"


class CMD_ListAllSplunkBucketsFromS3(object):
    def __init__(self, destSubParser, cmdName):
        myParser = destSubParser.add_parser(cmdName)
        myParser.set_defaults(func=self.run)
        myParser.add_argument("s3url",  help="S3 path to enumerate (like s3://foo/bar)")
        myParser.add_argument("region", help="AWS region (like us-west-2)")
        myParser.add_argument("guid", help="Guid of the on prem server")


    def run(self, args):
        self._args = args

        if not self._args.s3url.startswith(S3_URL_PREFIX):
            raise Exception("Only S3 URLs are supported - URL must begin with s3://...")
        if not "AWS_ACCESS_KEY" in os.environ:
            raise Exception("Command will not work without AWS_ACCESS_KEY set in environment.")
        if not "AWS_SECRET_KEY" in os.environ:
            raise Exception("Command will not work without AWS_SECRET_KEY set in environment.")

        s3FullPath = self._args.s3url[len(S3_URL_PREFIX):]
        if "/" in s3FullPath:
            s3Bucket, s3Path = s3FullPath.split("/", 1)
        else:
            s3Bucket = s3FullPath
            s3Path   = ""
        print "Will list: s3://%s/%s" % (s3Bucket, s3Path)

        onPremGuid = self._args.guid

        coll = BucketCollection()
        checksumURLs = []
        getOffset = None

	s3Path_ccs = s3Path+"/"+onPremGuid
	print "Working on "+s3Path_ccs
       	while True:
       		# throws on any non-200 response.
       		resp = self.issue_s3_request(s3Bucket, s3Path_ccs, getOffset)
            	getOffset = self.parseS3ListingXML(coll, checksumURLs, resp, s3Bucket, s3Path)
            	if not getOffset:
			outPath = "bucketsInS3-"+onPremGuid+".json"
			print "Saving to %s ..." % outPath
			coll.toFile(outPath)
			coll = BucketCollection()
               		break

        print "Done."

        if not checksumURLs:
            print "No checksum files to download."
        else:
            print "Downloading checksum files..." % checksumURLs
            for csumPath in checksumURLs:
                dlCmd = ["aws", "s3", "cp", csumPath, "."]
                if not run_s3_command(dlCmd, MODE_DOWNLOAD):
                    raise Exception("Checksum download process failed, please triage and try again!")
            print "Done."





    def parseS3ListingXML(self, coll, csums, resp, s3Bucket, s3Path):
        """Output: coll (BucketCollection), csums (checksums), nextOffset.
           If we need to fetch more data, returns offset marker."""
        bucketRegex = re.compile("([\\dA-F]{8}-[\\dA-F]{4}-[\\dA-F]{4}-[\\dA-F]{4}-[\\dA-F]{12})" \
                "/([^/]+)/([^/]+)/([dr]b_\\d+_\\d+_\\d*" \
                "(_[\\dA-F]{8}-[\\dA-F]{4}-[\\dA-F]{4}-[\\dA-F]{4}-[\\dA-F]{12})?(-tmp)?)/?(.*)")
        # str - stupid xml lib barfs on unicode str w/ encoding declaration (<?xml>).
        xmlRoot = et.fromstring(str(resp))
        # Exception here would mean S3 response format has changed.
        isTruncated = bool(xmlRoot.find(".//{%s}IsTruncated" % XML_AWS_NS).text)
        lastItem    = None
        for item in xmlRoot.findall(".//{%s}Key" % XML_AWS_NS):
            lastItem = item.text # :-/
            # Strip the leading prefix the user requested... NOTE: this restricts
            # us to only treating prefix as a directory path rather than an
            # partial-filename prefix - this is totally ok for us.
            # Strip in case no trailing slash in user specified S3 path...
            itemPath = item.text[len(s3Path):].strip("/")
            # Silently skip some just-in-case diagnostic info we've uploaded...
            if -1 != itemPath.find(KEY_EXTRADATA):
                # But keep the checksums, we'll use these.
                if KEY_CHKSUMLIST in itemPath:
                    csums.append("s3://%s/%s/%s" % (s3Bucket, s3Path, itemPath))
                continue
            matches = bucketRegex.match(itemPath)
            if not matches:
                raise Exception, "Not proceeding - found itemPath=\"%s\" that" \
                    " does not match bucketRegex - please investigate." % itemPath
            # maybeBucketGuid is unused, the optional guid capture group...
            indexerGuid, indexName, buckDirType, bucketName, \
                    maybeBucketGuid, maybeTmp, afterBucketName = matches.groups()
            parentPathInS3 = "%s/%s/%s/%s/%s" % (s3Bucket, s3Path, indexerGuid,
                indexName, buckDirType)
            # DEBUG: print "GUID: %s, index: %s, dirType: %s,\t name: %s" % (
            # DEBUG:     indexerGuid, indexName, buckDirType, bucketName)
            coll.addBuckets(indexName, buckDirType, "s3://%s" % parentPathInS3,
                (bucketName,))
        return isTruncated and lastItem or None

    def issue_s3_request(self, s3Bucket, s3Path, offset):
        method     = "GET"
        s3FullHost = "%s.%s" % (s3Bucket, S3_HOSTNAME)
        emptyHash  = hashlib.sha256("").hexdigest()
        timeNow    = time.gmtime()
        headers = {
              "Host"                 : s3FullHost
            , "x-amz-content-sha256" : emptyHash # No request body.
            , "x-amz-date"           : time.strftime("%Y%m%dT%H%M%SZ", timeNow)
        }
        getParams = {}
        if len(s3Path):
            getParams["prefix"] = s3Path
        if offset:
            getParams["marker"] = offset
        headers["Authorization"] = self.build_s3_auth_token(method, headers,
            emptyHash, timeNow, getParams)

        req     = requests.Request(method, "https://%s" % s3FullHost,
                  params=getParams, data="", headers=headers)
        sess    = requests.Session()
        # This is better than req.prepare() because it can apply cookies/etc...
        prepReq = sess.prepare_request(req)
        resp    = sess.send(prepReq)

        if 200 != resp.status_code:
            raise Exception, "Request failed, response_code=%d, response=%s !!" \
                    % (resp.status_code, resp.text)
        return resp.text

    def build_s3_auth_token(self, httpMethod, headers, contentHash, requestTime,
            getParams):
        """Did this whole mess (AWS4 auth) to avoid a dependency on boto within
           the Splunk Python installation..."""
        dateOnly   = time.strftime("%Y%m%d", requestTime)
        dateTime   = time.strftime("%Y%m%dT%H%M%SZ", requestTime) # ISO8601 per AWS API.
        # DEBUG: print "headers: %s" % headers

        # All headers that we include in canonicalRequest.
        signedHeaders = str.join(";", sorted(headers.keys())).lower()
        # AWS, why u auth so picky (http://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html)
        # DEBUG: print "signedHeaders: %s" % signedHeaders

        canonicalRequest = str.join("\n", [
              httpMethod
            , "/" # Enter a full path here only to download an actual object...
              # Per http://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html 
            , str.join("&", [urllib.urlencode({k : getParams[k]})
                for k in sorted(getParams.keys())])
              # Don't necessarily have to hash ALL headers, but it's easiest and only more secure.
            , str.join("\n", ["%s:%s" % (key.lower(), headers[key]) for key in sorted(headers.keys())])
            , "" # Empty line after headers, like HTTP.
            , signedHeaders
            , contentHash # Matches headers (no request body).
        ])
        canonicalRequestHash = hashlib.sha256(canonicalRequest).hexdigest()
        # DEBUG: print "canonicalRequest: %s" % canonicalRequest
        # DEBUG: print "canonicalRequestHash: %s" % canonicalRequestHash

        credentialScope = "%s/%s/%s/%s" % (dateOnly, self._args.region, "s3", "aws4_request")
        # DEBUG: print "credentialScope: %s" % credentialScope

        # http://docs.aws.amazon.com/general/latest/gr/sigv4-create-string-to-sign.html
        stringToSign = str.join("\n", [
              "AWS4-HMAC-SHA256"
            , dateTime
            , credentialScope
            , canonicalRequestHash
        ])
        # DEBUG: print "stringToSign: %s" % stringToSign

        def sign(key, content):
            # DEBUG: print "to-sign content=%s with key=%s" % (content, key)
            output = hmac.new(key, content, hashlib.sha256).digest()
            # DEBUG: print "  ==> output=%s" % output
            return output

        a = sign(("AWS4%s" % os.environ["AWS_SECRET_KEY"]), dateOnly)
        b = sign(a, self._args.region)
        c = sign(b, "s3")
        d = sign(c, "aws4_request")
        # Note hexdigest vs digest above...
        signature = hmac.new(d, stringToSign, hashlib.sha256).hexdigest()
        # DEBUG: print "signature=%s" % signature

        return "%s Credential=%s/%s,SignedHeaders=%s,Signature=%s" % (
               "AWS4-HMAC-SHA256", os.environ["AWS_ACCESS_KEY"],
               credentialScope, signedHeaders, signature)


# TODO this needs to accomodate actual index paths, not make assumptions...
def bucketDirTypeToDirName(buckType):
    # I think freezeBucketToS3.py uploads into a "cold" dir, which is a mistake...
    if buckType in ("coldPath", "cold"):
        return "colddb"
    # TODO: test this stuff with bulk upload...
    if buckType == "homePath":
        return "db"
    if buckType == "thawedPath":
        return "thaweddb"
    raise Exception, "Unexpected buckettype=%s." % buckType


# TODO
def getSearchableIndexName(indexName):
    """Names are stored differently in the s3 upload - we use the index name,
       not the index path on disk!!  Hopefully then names and index dirs for
       all non-predefined indexes are not different! TODO"""
    if indexName == "main":
        return "defaultdb"
    if indexName == "_audit":
        return "audit"
    if indexName == "_internal":
        return "_internaldb"
    if indexName == "corp":
        return "corpdb"
    if indexName == "owa":
        return "owadb"
    if indexName == "pan":
        return "pandb"
    if indexName == "pci":
        return "pcidb"
    if indexName == "prod":
        return "proddb"
    if indexName == "summary":
        return "summarydb"
    return indexName

# TODO
def getDisplayIndexName(indexName):
    """Reverse of getSearchableIndexName.  Opencoding rather than collapsing into
       a dict so I can guarantee other codepaths are unaffected."""
    if indexName == "defaultdb":
        return "main"
    if indexName == "audit":
        return "_audit"
    if indexName == "_internaldb":
        return "_internal"
    return indexName



class CMD_GenNewBucketOwnersFromS3BucketList(object):
    def __init__(self, destSubParser, cmdName):
        myParser = destSubParser.add_parser(cmdName)
        myParser.set_defaults(func=self.run)
        myParser.add_argument("--extra-copies", type=int, default=0, metavar="num",
                help="Cause \"num\" extra copies of each bucket to be"      \
                     " downloaded.  Can be of use if only one copy of each" \
                     " bucket was uploaded.\n"                              \
                     "Does NOT take geosites into account.")
        myParser.add_argument("--no-checksums", action="store_true")
        myParser.add_argument("bucket_list",  help="JSON file from list-all-buckets...s3")
        myParser.add_argument("guid_list",    
                help="File: GUIDs for new indexers, one per line, must have .guids extension")
	myParser.add_argument("index",  help="The name of the index to re-assign")

    def run(self, args):
        self._args = args

	selected_index = self._args.index
	print "Limiting reassignemt to "+selected_index

        # Error checks.

        if not self._args.bucket_list.endswith(".json"):
            raise Exception("Bucket list file must have .json extension" \
                    " - are you sure you passed the correct filename"    \
                    " (you passed \"%s\")?" % self._args.bucket_list)
        if not self._args.guid_list.endswith(".guids"):
            raise Exception("GUID list file must have .guids extension" \
                    " - are you sure you passed the correct filename"  \
                    " (you passed \"%s\")?" % self._args.guid_list)

        if self._args.extra_copies:
            if self._args.extra_copies > 5:
                raise Exception("Aborting - highly unlikely you want to create" \
                        " more than 5 copies of each bucket!")
            print "Will create %d additional copies of each bucket." \
                    % self._args.extra_copies

        guidRE = re.compile("^[\\dA-F]{8}-[\\dA-F]{4}-[\\dA-F]{4}-[\\dA-F]{4}-[\\dA-F]{12}$")
        indexerGuids = []
        with open(self._args.guid_list, "r") as guidFd:
            for line in guidFd:
                line = line.strip()
                if not guidRE.match(line):
                    print "WARNING: Skipping non-guid line: \"%s\" from file=%s." % (
                            line, self._args.guid_list)
                    continue
                indexerGuids.append(line)

        # Look for downloaded checksum files.

        csumFiles = []
        if not self._args.no_checksums:
            csumGlob = "splunk_%s_*" % KEY_CHKSUMLIST
            csumFiles = glob.glob(csumGlob)
            if not csumFiles:
                raise Exception, "Found no checksum files matching glob %s." \
                        "  If you want to skip the ability to checksum, use" \
                        " --no-checksums." % csumGlob
            print "Found %d checksum lists." % len(csumFiles)
        # Hi I came to take ur RAM.  Sorry, this has to be relatively quick and dirty for now...
        csumDict = {}
        for file in csumFiles:
            with open(file, "r") as csumFd:
                for line in csumFd:
                    # Slower but more resilient... Could just count the 32 bytes + 2 spaces...
                    left, messyRight = line.split(" ", 1)
                    right = messyRight.lstrip().rstrip("\r\n")
                    # TODO: make sure os.sep is right to use here... should be... (windows).
		    #
                    index, garbage, bucket = right.split(os.sep)[0:3]
                    if not index in csumDict:
                        csumDict[index] = {}
                    if not bucket in csumDict[index]:
                        csumDict[index][bucket] = {}
                    # Store in RAM: bucket name, filepath, checksum.  Will look these up soon..
                    csumDict[index][bucket][right] = left
        # TODO: we prob need a guid dict level too - 2 replicated buckets will have non-unique
        #       names if now living on 2 different GUIDs.
        # DEBUG: print csumDict

        # Read bucket list.

        coll = BucketCollection()
        coll.fromFile(self._args.bucket_list)

        print "Found %d buckets in file=%s." % (
                len(coll), self._args.bucket_list)
        print "Will distribute %d buckets to %d indexers per file=%s." % (
                len(coll) * (1 + self._args.extra_copies), len(indexerGuids),
                self._args.guid_list)

        # Main spraying loop - just builds file lists, doesn't download anything.

        bucketColls = {} # Output - eventually written to file.
        csumColls   = {} # Also output, written to another file.
        # Very much like bucketColls.  However, strips db_ and rb_ so we can
        # see whether a bucket with the remaining portion
        # (timestamps/serial/guid) is already allocated to this indexer.  We can
        # do this WAY smarter, by storing the IDs in bucketColls along with a an
        # attr describing whether it's rb_ or db_.  But not making that change
        # this late in the game... TODO.  Also, BucketCollection could and
        # should do this for is.
        bucketIds   = {}
        for guid in indexerGuids:
            bucketColls[guid] = BucketCollection()
            csumColls[guid] = []
            bucketIds[guid] = []
        guidOff = 0

        bucketIdRE = re.compile('^[dr]b_(.*)')
	found = 0
	prev_indexName = "foo"
	prev_num = 0
	step = 10000
        for indexName, dirType, bucketParentDir, bucketName in coll.items():
	    #if  indexName != prev_indexName:
	#	print indexName
	#	prev_indexName = indexName
	    if indexName != selected_index: continue
	    if found == 0 :
		print "Found index="+indexName
		found = 1
            remainingCopies = 1 + self._args.extra_copies
            # TODO freezeBucketToS3.py
            realIndexDirName = getSearchableIndexName(indexName)
            bucketId = bucketIdRE.match(bucketName).group(1) # eg: db_1416781732_1416781534_9_D1EBB8B2-DA8A-401E-8AC2-A98214754674
	    num_guid_trys = 0
            while remainingCopies > 0:
                guid = indexerGuids[guidOff]
		num_guid_trys = num_guid_trys + 1
                # Does this indexer already have a bucket with this ID assigned
                # to it (maybe an rb_ for this db_, etc)?
                if bucketId in bucketIds[guid]:
                    # print bucketId, guid, remainingCopies
                    # Let's try finding another...
                    guidOff = (guidOff + 1 < len(indexerGuids)) and guidOff + 1 or 0
		    #print str(num_guid_trys)+" BucketId is already in the list. "+bucketId+"   guidOff="+str(guidOff)
		    #  It is possible that there is MORE more buckets than indexers. In which case ... 
		    #     ....  we skip this bucket, cuz we already have buckets by the same name on ALL the indexers
		    if num_guid_trys > len(indexerGuids) : 
			print "It is odd that index "+indexName+" has more than "+str(len(indexerGuids))+" buckets all with the same name "+bucketId
			break
                    continue
                # Store bucket ID.
		#print "GUID = "+guid+"  Found a new bucketID "+bucketId
                bucketIds[guid].append(bucketId)

                # We don't have checksums for c2f buckets...
                # DEBUG: print "%50s\t %s" % (realIndexDirName, bucketName)
                # DEBUG: print "INDEX NAME: %s" % indexName
                if realIndexDirName in csumDict and bucketName in csumDict[realIndexDirName]:
                    # DEBUG: print "%20s\t %s" % (realIndexDirName, bucketName)
                    # FIXME: hack per freezeBucketToS3.py ...
                    csumPrefix = "%s/%s/%s/" % (realIndexDirName, bucketDirTypeToDirName(dirType), bucketName)
                    # DEBUG: print "searching with %s" % csumPrefix
                    count = 0
                    for k, v in csumDict[realIndexDirName][bucketName].items():
                        # DEBUG: print "\t\t\t%s\t %s" % (k, v)
                        if k.startswith(csumPrefix):
                            count += 1
                            # two spaces so works with md5sum cli cmd...
                            csumColls[guid].append("%s  %s" % (v, k))
                    # DEBUG: print "found %d matches" % count
                # Store bucket.
		#print "Storing bucket "+indexName+" "+dirType+" "+bucketParentDir+" "+bucketName
                bucketColls[guid].addBuckets(indexName, dirType, bucketParentDir, (bucketName,))
		#  Report progress ,,,,,
		#if len(bucketColls[guid])/step >  prev_num :
		#	prev_num = len(bucketColls[guid])/step
		#	print selected_index+" => "+str(prev_num)
                # Next guid...
                guidOff = (guidOff + 1 < len(indexerGuids)) and guidOff + 1 or 0
                remainingCopies -= 1
        # DEBUG: print csumColls

        # Save to file and print a bit.

        print
        for guid in indexerGuids:
            buckOutFile = "dest_buckets_%s_%s.json" % (guid, selected_index) 
            print "Saving %d buckets to file=%s ..." % (len(bucketColls[guid]),
                buckOutFile)
            bucketColls[guid].toFile(buckOutFile)
            if len(csumColls[guid]):
                csumOutFile = "dest_checksums_%s_%s.md5" % (guid, selected_index)
                print "Saving %d checksums to file=%s ..." % (len(csumColls[guid]),
                    csumOutFile)
                with open(csumOutFile, "w") as outFd:
                    for line in csumColls[guid]:
                        if -1 != line.find("defaultdb%s" % os.sep): # TODO os.sep ?
                            line = line.replace("defaultdb%s" % os.sep,
                                    "main%s" % os.sep, 1)
                        outFd.write("%s\n" % line)
            else:
                print "No checksums found for buckets to copy to GUID=%s." % guid

        print "\nDone."


class CMD_DownloadBucketsToStagingDir(object):
    def __init__(self, destSubParser, cmdName):
        myParser = destSubParser.add_parser(cmdName)
        myParser.set_defaults(func=self.run)
        myParser.add_argument("bucket_list",  help="JSON file from gen-new-bucket...")
    def run(self, args):
        self._args = args
        # Convenience...
        self.instance_guid = get_instance_guid()

        bucketListRE = "^dest_buckets_([\\dA-F]{8}-[\\dA-F]{4}-[\\dA-F]{4}-[\\dA-F]{4}-[\\dA-F]{12})\\.json$"
        basename = os.path.basename(self._args.bucket_list)
        matches = re.match(bucketListRE, basename)
        if not matches:
            raise Exception, "Bucket list filename must be in the format"   \
                    " dest_buckets_<this_indexer_guid>.json, you provided" \
                    " filename=\"%s\"." % basename
        if not matches.group(1) == self.instance_guid:
            raise Exception, "Aborting: This bucket list is for Splunk indexer" \
                    " with GUID=%s, but my GUID is %s.  It sounds like you've"  \
                    " mixed up the various bucket lists!  This will cause data" \
                    " duplication, so will not proceed." % (
                            matches.group(1), self.instance_guid)

        coll = BucketCollection()
        coll.fromFile(self._args.bucket_list)

        # Build bucket download commands.

        cmds = []
        stagingDestDirBase = os.path.join(os.environ["SPLUNK_DB"], STAGING_DIR)
        print "Will download data to %s.\n" % stagingDestDirBase
        for index, dirType, bucketParentDir, bucketName in coll.items():

            # FIXME: hack per freezeBucketToS3.py ...
            realIndexDirName = getSearchableIndexName(index)

            testIdxPath = os.path.join(os.environ["SPLUNK_DB"], realIndexDirName)
            if not os.path.exists(testIdxPath):
                raise Exception, "Aborting: The path=\"%s\" for index=%s does" \
                        " not exist - it looks like something is different about" \
                        " this indexer's configuration.  Please review." % (
                                testIdxPath, index)
            stagingDestDir = os.path.join(stagingDestDirBase, index, dirType,
                bucketName)
            if not os.path.exists(stagingDestDir):
                os.makedirs(stagingDestDir)
            cmds.append(build_s3_download_bucket_command(
                stagingDestDir,
                # S3 path to download from.
                "%s/%s" % (bucketParentDir, bucketName)))

        # Download!
        for cmd in cmds:
            if not run_s3_command(cmd, MODE_DOWNLOAD):
                raise Exception("Download process failed, please triage and try again!")
        print "Done."


class CMD_VerifyDownloadedFiles(object):
    def __init__(self, destSubParser, cmdName):
        myParser = destSubParser.add_parser(cmdName)
        myParser.set_defaults(func=self.run)
        myParser.add_argument("--c2f", action="store_true", help="Activates require workaround for c2f uploaded buckets.")
        myParser.add_argument("checksum_list",  help="Checksum list generated by gen-new-bucket-...")
    def run(self, args):
        self._args = args
        # Convenience...
        self.instance_guid = get_instance_guid()

        csumListRE = "^dest_checksums_([\\dA-F]{8}-[\\dA-F]{4}-[\\dA-F]{4}-[\\dA-F]{4}-[\\dA-F]{12})\\.md5$"
        basename = os.path.basename(self._args.checksum_list)
        matches = re.match(csumListRE, basename)
        if not matches:
            raise Exception, "Checksum list filename must be in the format"   \
                    " dest_checksums_<this_indexer_guid>.md5, you provided" \
                    " filename=\"%s\"." % basename
        if not matches.group(1) == self.instance_guid:
            raise Exception, "Aborting: This checksum list is for Splunk indexer" \
                    " with GUID=%s, but my GUID is %s.  It sounds like you've"  \
                    " mixed up the various checksum lists!" % (
                            matches.group(1), self.instance_guid)

        def convertFilePath(path):
            pieces = path.split(os.sep) # TODO os.sep??
            pieces[0] = getDisplayIndexName(pieces[0])
            if self._args.c2f:
                for i in range(len(pieces)):
                    if pieces[i] == "colddb":
                        pieces[i] = "cold" # TODO: due to freezeBucketToS3.py
            else:
                for i in range(len(pieces)):
                    if pieces[i] == "colddb":
                        pieces[i] = "coldPath" # TODO: due to freezeBucketToS3.py
                    elif pieces[i] == "db":
                        pieces[i] = "homePath" # TODO: due to freezeBucketToS3.py
            return os.path.join(*pieces)

        stagingDestDirBase = os.path.join(os.environ["SPLUNK_DB"], STAGING_DIR)
        numMatched = 0
        numFailed  = 0
        with open(self._args.checksum_list) as csumFile:
            for line in csumFile:
                matched = True
                # <hash><2 spaces><file path> like what md5sum uses.
                hash, filepath = filter(lambda x: x.strip(), line.split(" "))
                filepath = filepath.strip()
                fullPath = os.path.join(stagingDestDirBase, convertFilePath(filepath))
                try:
                    with open(fullPath, "rb") as verifFd:
                        actualHash = fileMd5Hash(verifFd)
                        if hash != actualHash:
                            matched = False
                            sys.stderr.write("MD5 did not match for file=\"%s\", expected=%s actual=%s!\n" \
                                    % (fullPath, hash, actualHash))
                            sys.stderr.flush()
                except IOError, e: 
                    matched = False
                    sys.stderr.write("No such file: %s on host=%s!\n" % (fullPath, socket.gethostname()))
                    sys.stderr.flush()
                if matched:
                    numMatched += 1
                else:
                    numFailed += 1
        if not numFailed:
            print "Everything matched!  Success!   (%d matched)" % numMatched
        else:
            sys.stderr.write("There were failures. :(   (%d matched, %d failed)\n" % (numMatched, numFailed))



class CMD_MoveStagedDataToIndexes(object):
    def __init__(self, destSubParser, cmdName):
        myParser = destSubParser.add_parser(cmdName)
        myParser.set_defaults(func=self.run)
        myParser.add_argument("--actually-move", action="store_true",
                help="Without this, the actual move commands will not be run.")
    def run(self, args):
        self._args = args

        stagingDestDirBase = os.path.join(os.environ["SPLUNK_DB"], STAGING_DIR)
        if not os.path.exists(stagingDestDirBase):
            raise Exception, "Staging directory path=\"%s\" does not exist, are you sure you downloaded data on this instance?"

        if self._args.actually_move:
            proc = subprocess.Popen(["splunk", "status"], stdout=subprocess.PIPE)
            proc.communicate()
            if 0 == proc.returncode:
                raise Exception, "This command must not be used while Splunk is running."

        movePairs = []
        dirsToRemove = []

        for indexName in os.listdir(stagingDestDirBase):
            thisIdxDir = os.path.join(stagingDestDirBase, indexName)
            dirsToRemove.append(thisIdxDir)
            for bucketType in os.listdir(thisIdxDir):
                thisBucketTypeDir = os.path.join(thisIdxDir, bucketType)
                dirsToRemove.append(thisBucketTypeDir)
                for bucketName in os.listdir(thisBucketTypeDir):
                    thisBucketDir = os.path.join(thisBucketTypeDir, bucketName)
                    # FIXME: hack per freezeBucketToS3.py ...
                    # TODO -- USE SEARCHABLE HERE
                    realIndexDirName = getSearchableIndexName(indexName)
                    destPath = os.path.join(os.environ["SPLUNK_DB"], realIndexDirName,
                            bucketDirTypeToDirName(bucketType), bucketName)
                    if not self._args.actually_move:
                        print "Will move: %s ==> %s" % (thisBucketDir, destPath)
                    movePairs.append((thisBucketDir, destPath))

        if self._args.actually_move:
            for src, dst in movePairs:
                shutil.move(src, dst)
            for oneDir in reversed(dirsToRemove):
                os.rmdir(oneDir)
            os.rmdir(stagingDestDirBase)
        print "\nDone."


def main(rawArgs):
    # Most argument parsing will be delegated to the subcommands.
    mainArgParser = argparse.ArgumentParser()
    subParsers = mainArgParser.add_subparsers()

    # Store all registered commands here to avoid any lifecycle ambiguities.
    cmds = [
        CMD_GenBucketlistWarmCold(subParsers,              "gen-bucketlist-warm-cold")
      , CMD_GenChecksumsFromBucketlist(subParsers,         "gen-checksums-from-bucketlist")
      , CMD_GenRandBucketOwnersFromBucketList(subParsers,  "gen-rand-bucket-owners-from-bucketlists")
      , CMD_UploadBucketsToS3FromBucketList(subParsers,    "upload-buckets-to-s3-from-bucketlist")
      , CMD_ListAllSplunkBucketsFromS3(subParsers,         "list-all-splunk-buckets-from-s3")
      , CMD_GenNewBucketOwnersFromS3BucketList(subParsers, "gen-new-bucket-owners-from-s3-bucketlist")
      , CMD_DownloadBucketsToStagingDir(subParsers,        "download-buckets-to-staging-dir")
      , CMD_VerifyDownloadedFiles(subParsers,              "verify-downloaded-files")
      , CMD_MoveStagedDataToIndexes(subParsers,            "move-staged-data-to-indexes")
      ]

    # Parses sys.argv implicitly.
    args = mainArgParser.parse_args(rawArgs)
    args.func(args)

    ### metadataCollection.generate_missing_checksums()
    ### metadataCollection.write()
    ### ### TODO: lazy-delete aged-out buckets
    ### #generate_missing_md5sums(instanceGuid, allBuckets)
    ### #generate_s3_upload_commands(instanceGuid, allBuckets)

if __name__ == "__main__":
    main(sys.argv[1:])

def test_filter_internal_index_dirs():
    """Test function: make sure the correct index paths are removed."""
    dirsBefore = [
            "/home/amrit/files/cluster/test/splunk/var/lib/splunk/defaultdb/db"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/defaultdb/colddb"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/defaultdb/thaweddb"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/summarydb/db"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/summarydb/colddb"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/summarydb/thaweddb"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/fishbucket/db"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/fishbucket/colddb"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/fishbucket/thaweddb"
        ,   "/tmp/bleh"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/blockSignature/db"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/blockSignature/colddb"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/blockSignature/thaweddb"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/historydb/db"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/historydb/colddb"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/historydb/thaweddb"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/_introspection/db"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/_introspection/colddb"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/_introspection/thaweddb"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/audit/db"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/audit/colddb"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/audit/thaweddb"
    ]
    expectedDirsAfter = [
            "/home/amrit/files/cluster/test/splunk/var/lib/splunk/defaultdb/db"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/defaultdb/colddb"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/defaultdb/thaweddb"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/summarydb/db"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/summarydb/colddb"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/summarydb/thaweddb"
        ,   "/tmp/bleh"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/blockSignature/db"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/blockSignature/colddb"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/blockSignature/thaweddb"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/historydb/db"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/historydb/colddb"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/historydb/thaweddb"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/audit/db"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/audit/colddb"
        ,   "/home/amrit/files/cluster/test/splunk/var/lib/splunk/audit/thaweddb"
    ]
    # Construct the necessary data structures.
    indexPathObjsBefore = [ IndexLocation("foo", x, "homePath") for x in dirsBefore ]
    indexPathObjsAfter  = filter_internal_index_dirs(indexPathObjsBefore)
    dirsAfter = [ x.dirPath for x in indexPathObjsAfter ]
    # Compare the raw filepaths to verify.
    assert dirsAfter == expectedDirsAfter, \
        "Filtered dirs did not match!  EXPECTED:\n%s\n\nACTUAL:\n%s\n" % (
            dirsAfter, expectedDirsAfter)

def test_splunkDBPaths():
    # Must be structured like indexes.conf.  These paths don't need to exist
    # for this test.
    indexConfig = {
        "default" : {
        },
        "volume:foobar" : {
            "path" : "/data/splunkvolume1"
        },
        "volume:badvol" : {
            "badParam" : "bar",
        },
        "idx1" : {
            "homePath"   : "/data/idx1/db",
            "coldPath"   : "/data/idx1/colddb",
            "thawedPath" : "/data/idx1/thawed"
        },
        "idx2" : {
            "homePath"   : "/data/idx2/db",
            "coldPath"   : "/data/idx2/colddb",
            "thawedPath" : "/data/idx2/thawed"
        },
        "idx3" : {
            "homePath"   : "volume:foobar/idx3/db",
            "coldPath"   : "volume:foobar/idx3/colddb",
            "thawedPath" : "volume:foobar/idx3/thawed"
        },
        "idx4" : {
            "badParam"   : "foo"
        },
        "idx5" : {
            "disabled"   : "true",
        }
    }
    outPaths  = splunkDBPaths(indexConfig)
    outPaths2 = splunkDBPaths(indexConfig)
    assert outPaths == computed_db_paths
    assert outPaths2 == outPaths2
    # 3 indexes * 3 paths = 9.
    assert 9 == len(outPaths)
    for idx in outPaths:
        assert isinstance(idx, IndexLocation)
    # This is sorted.
    outPathsTruth = (
        ("idx1", "coldPath",    "/data/idx1/colddb"),
        ("idx1", "homePath",    "/data/idx1/db"),
        ("idx1", "thawedPath",  "/data/idx1/thawed"),
        ("idx2", "coldPath",    "/data/idx2/colddb"),
        ("idx2", "homePath",    "/data/idx2/db"),
        ("idx2", "thawedPath",  "/data/idx2/thawed"),
        ("idx3", "coldPath",    "/data/splunkvolume1/idx3/colddb"),
        ("idx3", "homePath",    "/data/splunkvolume1/idx3/db"),
        ("idx3", "thawedPath",  "/data/splunkvolume1/idx3/thawed"),
    )
    for op in outPaths:
        print op.indexName, op.dirPath, op.dirType
    print outPathsTruth
    # Sort output paths by all 3 keys...
    sortedOP = sorted(outPaths, key=operator.attrgetter(
        "indexName", "dirType", "dirPath"))
    for i in range(len(outPathsTruth)):
        assert outPathsTruth[i] == \
            (sortedOP[i].indexName, sortedOP[i].dirType, sortedOP[i].dirPath)

def test_fileMd5Hash():
    # 128000 aka lazy binary math: require at least two reads in hash function.
    fakeFd = StringIO.StringIO("x" * 128000)
    hash = fileMd5Hash(fakeFd)
    # Gen'd with: echo | awk '{for (i=0; i<128000; ++i){printf("x");}}' | md5sum.
    assert "8b9717dc588d653855659cb3a167ee38" == hash

def test_RoundRobinFileReader():
    fakeFd1 = StringIO.StringIO(str.join("\n",
        ["foo",
         "bar",
         "baz",
         "huh"]) + "\n")
    fakeFd2 = StringIO.StringIO(str.join("\n",
        ["abcde",
         "fghij",
         "klmno",
         "pqrst",
         "uvwxy"]) + "\n")
    fakeFd3 = StringIO.StringIO(str.join("\n",
        ["12",
         "34",
         "56"]) + "\n")
    rrFd = RoundRobinFileReader([fakeFd1, fakeFd2, fakeFd3])
    assert(3 == rrFd.numFiles())
    rrFdReadOrderTruth = (
        "foo",
        "abcde",
        "12",
        "bar",
        "fghij",
        "34",
        "baz",
        "klmno",
        "56",
        "huh",
        "pqrst",
        "uvwxy")
    lineCounter = 0
    for line in rrFd:
        assert(line == rrFdReadOrderTruth[lineCounter] + "\n")
        lineCounter += 1

def test_CMD_GenBucketlistWarmCold():
    # Undo cache.
    global computed_db_paths
    computed_db_paths = None
    args = ["gen-bucketlist-warm-cold"]
    main(args)

### def test_CMD_GenChecksumsFromBucketlist():
###     args = ["gen-checksums-from-bucketlist"]
###     main(args)

### def test_CMD_GenRandBucketOwnersFromBucketList(object):
###     args = ["gen-rand-bucket-owners-from-bucketlists"]
###     main(args)



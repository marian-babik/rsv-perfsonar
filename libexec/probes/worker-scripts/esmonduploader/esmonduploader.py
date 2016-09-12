import os
import time
import inspect
import json
import warnings
from time import strftime
from time import localtime
from optparse import OptionParser

# Using the esmond_client instead of the rpm
from esmond_client.perfsonar.query import ApiFilters
from esmond_client.perfsonar.query import ApiConnect
from esmond_client.perfsonar.post import MetadataPost, EventTypePost, EventTypeBulkPost
from esmond_client.perfsonar.post import EventTypeBulkPostWarning, EventTypePostWarning

# New module with socks5 OR SSL connection that inherits ApiConnect
from SocksSSLApiConnect import SocksSSLApiConnect
from SSLNodeInfo import EventTypeSSL
from SSLNodeInfo import SummarySSL
# Need to push to cern message queue
from messaging.message import Message
from messaging.queue.dqs import DQS


# Set filter object
filters = ApiFilters()
gfilters = ApiFilters()

# Set command line options
parser = OptionParser()
parser.add_option('-d', '--disp', help='display metadata from specified url', dest='disp', default=False, action='store')
parser.add_option('-e', '--end', help='set end time for gathering data (default is now)', dest='end', default=0)
parser.add_option('-l', '--loop', help='include this option for looping process', dest='loop', default=False, action='store_true')
parser.add_option('-p', '--post',  help='begin get/post from specified url', dest='post', default=False, action='store_true')
parser.add_option('-r', '--error', help='run get/post without error handling (for debugging)', dest='err', default=False, action='store_true')
parser.add_option('-s', '--start', help='set start time for gathering data (default is -12 hours)', dest='start', default=960)
parser.add_option('-u', '--url', help='set url to gather data from (default is http://hcc-pki-ps02.unl.edu)', dest='url', default='http://hcc-pki-ps02.unl.edu')
parser.add_option('-w', '--user', help='the username to upload the information to the GOC', dest='username', default='afitz', action='store')
parser.add_option('-k', '--key', help='the key to upload the information to the goc', dest='key', default='fc077a6a133b22618172bbb50a1d3104a23b2050', action='store')
parser.add_option('-g', '--goc', help='the goc address to upload the information to', dest='goc', default='http://osgnetds.grid.iu.edu', action='store')
parser.add_option('-t', '--timeout', help='the maxtimeout that the probe is allowed to run in secs', dest='timeout', default=1000, action='store')
parser.add_option('-x', '--summaries', help='upload and read data summaries', dest='summary', default=True, action='store')
parser.add_option('-a', '--allowedEvents', help='The allowedEvents', dest='allowedEvents', default=False, action='store')
#Added support for SSL cert and key connection to the remote hosts
parser.add_option('-c', '--cert', help='Path to the certificate', dest='cert', default='/etc/grid-security/rsv/rsvcert.pem', action='store')
parser.add_option('-o', '--certkey', help='Path to the certificate key', dest='certkey', default='/etc/grid-security/rsv/rsvkey.pem', action='store')
# Add support for message queue
parser.add_option('-q', '--queue', help='Directory queue (path)', default=None, dest='dq', action='store')
parser.add_option('-m','--tmp', help='Tmp directory to use for timestamps', default='/tmp/rsv-perfsonar/', dest='tmp', action='store')
(opts, args) = parser.parse_args()

class EsmondUploader(object):

    def add2log(self, log):
        print strftime("%a, %d %b %Y %H:%M:%S", localtime()), str(log)
    
    def __init__(self,verbose,start,end,connect,username=None,key=None, goc=None, allowedEvents='packet-loss-rate', cert=None, certkey=None, dq=None, tmp='/tmp/rsv-perfsonar/'):
        # Filter variables
        filters.verbose = verbose
        #filters.verbose = True 
        # this are the filters that later will be used for the data
        self.time_end = int(time.time())
        self.time_start = int(self.time_end - start)
        self.time_max_start = int(time.time()) - 24*60*60
        # Filter for metadata
        filters.time_start = int(self.time_end - 3*start)
        # Added time_end for bug that Andy found as sometime far in the future 24 hours
        filters.time_end = self.time_end + 24*60*60
        # For logging pourposes
        filterDates = (strftime("%a, %d %b %Y %H:%M:%S ", time.gmtime(self.time_start)), strftime("%a, %d %b %Y %H:%M:%S", time.gmtime(self.time_end)))
        #filterDates = (strftime("%a, %d %b %Y %H:%M:%S ", time.gmtime(filters.time_start)))
        self.add2log("Data interval is from %s to %s" %filterDates)
        self.add2log("Metada interval is from %s to now" % (filters.time_start))
        # gfiltesrs and in general g* means connecting to the cassandra db at the central place ie goc
        gfilters.verbose = False        
        gfilters.time_start = int(self.time_end - 5*start)
        gfilters.time_end = self.time_end
        gfilters.input_source = connect
        # Username/Key/Location/Delay
        self.connect = connect
        self.username = username
        self.key = key
        self.goc = goc
        self.conn = SocksSSLApiConnect("http://"+self.connect, filters)
        self.gconn = ApiConnect(self.goc, gfilters)
        self.cert = cert
        self.certkey = certkey
        self.tmpDir = tmp + '/' + self.connect +'/'
        # Convert the allowedEvents into a list
        self.allowedEvents = allowedEvents.split(',')
        # In general not use SSL for contacting the perfosnar hosts
        self.useSSL = False
        #Code to allow publishing data to the mq
        self.mq = None
        self.dq = dq
        if self.dq != None and self.dq!='None':
            try:
                self.mq = DQS(path=self.dq, granularity=5)
            except Exception as e:
                self.add2log("Unable to create dirq %s, exception was %s, " % (self.dq, e))
    
    # Publish message to Mq
    def publishToMq(self, arguments, event_types, datapoints, summaries_data):
        for event in datapoints.keys():
            # filter events for mq (must be subset of the probe's filter)
            if event not in ('path-mtu', 'histogram-owdelay','packet-loss-rate','histogram-ttl','throughput','packet-retransmits','packet-trace'):
                continue
            # skip events that have no datapoints 
            if not datapoints[event]:
                continue
            # compose msg
            msg_head = { 'input-source' : arguments['input_source'],
                        'input-destination' : arguments['input_destination'],
                         'event-type' : event,
                         'rsv-timestamp' : "%s" % time.time(),
                         'summaries' : 0,
                         'destination' : '/topic/perfsonar.' + event}
            msg_body = { 'meta': arguments }
            if summaries_data[event]:
                msg_body['summaries'] = summaries_data[event]
                msg_head['summaries'] = 1
            if datapoints[event]:
                msg_body['datapoints'] = datapoints[event]
            msg = Message(body=json.dumps(msg_body), header=msg_head)
            # add to mq
            try:
                self.mq.add_message(msg)
            except Exception as e:
                self.add2log("Failed to add message to mq %s, exception was %s" % (self.dq, e))

    # Get Data
    def getData(self, disp=False, summary=True):
        self.add2log("Only reading data for event types: %s" % (str(self.allowedEvents)))
        if summary:
            self.add2log("Reading Summaries")
        else:
            self.add2log("Omiting Sumaries")
        metadata = self.conn.get_metadata()
        try:
            #Test to see if https connection is succesfull
            md = metadata.next()
            self.readMetaData(md, disp, summary)
        except Exception as e:
            #Test to see if https connection is sucesful
            self.add2log("Unable to connect to %s, exception was %s, trying SSL" % ("http://"+self.connect, e))
            try:
                metadata = self.conn.get_metadata(cert=self.cert, key=self.certkey)
                md = metadata.next()
                self.useSSL = True
                self.readMetaData(md, disp, summary)
            except Exception as e:
                raise Exception("Unable to connect to %s, exception was %s, " % ("https://"+self.connect, e))
        for md in metadata:
            self.readMetaData(md, disp, summary)

    # Md is a metadata object of query
    def readMetaData(self, md, disp=False, summary=True):
        arguments = {}
        # Building the arguments for the post
        arguments = {
            "subject_type": md.subject_type,
            "source": md.source,
            "destination": md.destination,
            "tool_name": md.tool_name,
            "measurement_agent": md.measurement_agent,
            "input_source": md.input_source,
            "input_destination": md.input_destination,
            "tool_name": md.tool_name
        }
        if not md.time_duration is None:
            arguments["time_duration"] = md.time_duration
        if not md.ip_transport_protocol is None:
            arguments["ip_transport_protocol"] = md.ip_transport_protocol
        # Assigning each metadata object property to class variables
        event_types = md.event_types
        metadata_key = md.metadata_key
        # print extra debugging only if requested
        self.add2log("Reading New METADATA/DATA %s" % (md.metadata_key))
        if disp:
            self.add2log("Posting args: ")
            self.add2log(arguments)
        # Get Events and Data Payload
        summaries = {}
        summaries_data = {}
        # datapoints is a dict of lists
        # Each of its members are lists of datapoints of a given event_type
        datapoints = {}
        datapointSample = {}
        #load next start times
        self.time_starts = {}
        try:
            f = open(self.tmpDir+md.metadata_key, 'r')
            self.time_starts = json.loads(f.read())
            f.close()
        except IOError:
            self.add2log("first time for %s" % (md.metadata_key))
        except ValueError:
            # decoding failed
            self.add2log("first time for %s" % (md.metadata_key))
        for et in md.get_all_event_types():
            if self.useSSL:
                etSSL = EventTypeSSL(et, self.cert, self.certkey)
                et = etSSL
            # Adding the time.end filter for the data since it is not used for the metadata
            #use previously recorded end time if available
            et.filters.time_start = self.time_start
            if et.event_type in self.time_starts.keys():
                et.filters.time_start = self.time_starts[et.event_type]
                self.add2log("loaded previous time_start %s" % et.filters.time_start)
            # Not to go undefitly in the past but up to one day
            if et.filters.time_start < self.time_max_start:
                self.add2log("previous time_start %s too old. New time_start today - 24h: %s" % (et.filters.time_start, self.time_max_start) )
                et.filters.time_start =  self.time_max_start
            et.filters.time_end = filters.time_end
            eventype = et.event_type
            datapoints[eventype] = {}
            #et = md.get_event_type(eventype)
            if summary:
                summaries[eventype] = et.summaries
            else:
                summaries[eventype] = []
            # Skip reading data points for certain event types to improv efficiency  
            if eventype not in self.allowedEvents:                                                                                                  
                continue
            # Read summary data 
            summaries_data[eventype] = []
            for summ in et.get_all_summaries():
                if self.useSSL:
                    summSSL = SummarySSL(summ, self.cert, self.certkey)
                    summ = summSSL
                summ_data = summ.get_data()
                summ_dp = [ (dp.ts_epoch, dp.val) for dp in summ_data.data ]
                if not summ_dp:
                    continue
                summaries_data[eventype].append({'event_type': eventype,
                                                   'summary_type' : summ.summary_type,
                                                   'summary_window' : summ.summary_window,
                                                   'summary_data' : summ_dp })
                # Read datapoints
            dpay = et.get_data()
            tup = ()
            for dp in dpay.data:
                tup = (dp.ts_epoch, dp.val)
                datapoints[eventype][dp.ts_epoch] = dp.val
                # print debugging data
            self.add2log("For event type %s, %d new data points"  %(eventype, len(datapoints[eventype])))
            if len(datapoints[eventype]) > 0 and not isinstance(tup[1], (dict,list)): 
                # picking the first one as the sample
                datapointSample[eventype] = tup[1]
        self.add2log("Sample of the data being posted %s" % datapointSample)
        try:
            self.postData(arguments, event_types, summaries, summaries_data, metadata_key, datapoints, summary, disp)
        except Exception as e:
            raise Exception("Unable to post to %s, because exception %s. Check postgresql and cassandra services are up. Then check user and key are ok "  %(self.goc, e))

    def postDataSlow(self, json_payload, new_metadata_key, original_datapoints, disp=False):
        data = json_payload["data"]
        for data_point in data:
            epoch = data_point['ts']
            datapoints = data_point["val"]
            for datavalue in datapoints:
                new_event_type = datavalue['event-type']
                value = datavalue['val']
                et = EventTypeBulkPost(self.goc, username=self.username, api_key=self.key, metadata_key=new_metadata_key)
                et.add_data_point(new_event_type, epoch, value)
                try:
                    et.post_data()
                    if epoch >= self.time_starts[new_event_type]:
                        self.time_starts[event_type] = epoch + 1
                        f = open(self.tmpDir + metadata_key, 'w')
                        f.write(json.dumps(self.time_starts))
                        f.close()
                except Exception as err:
                    self.add2log("Exception adding new point: %s" % err)
                    self.add2log(et.json_payload())
                    continue
    
    # Experimental function to try to recover from missing packet-count-sent or packet-count-lost data
    def getMissingData(self, timestamp, metadata_key, event_type, disp=False):
        filtersEsp = ApiFilters()
        filtersEsp.verbose = disp
        filtersEsp.metadata_key = metadata_key
        filtersEsp.time_start = timestamp - 30000
        filtersEsp.time_end  = timestamp + 30000 
        conn = SocksSSLApiConnect("http://"+self.connect, filtersEsp)
        if self.useSSL:
            metadata = conn.get_metadata(cert=self.cert, key=self.certkey)
        else:
            metadata = conn.get_metadata()
        datapoints = {}
        datapoints[event_type] = {}
        for md in metadata:
            if not md.metadata_key == metadata_key:
                continue
            et = md.get_event_type(event_type)
            if self.useSSL:
                etSSL = EventTypeSSL(et, self.cert, self.certkey)
                et = etSSL
            dpay = et.get_data()
            for dp in dpay.data:
                if dp.ts_epoch == timestamp:
                    self.add2log("point found")
                    datapoints[event_type][dp.ts_epoch] = dp.val
        return datapoints
                

    def postMetaData(self, arguments, event_types, summaries, summaries_data, metadata_key, datapoints, summary = True, disp=False):
         mp = MetadataPost(self.goc, username=self.username, api_key=self.key, **arguments)
         for event_type in summaries.keys():
            mp.add_event_type(event_type)
            if summary:
                summary_window_map = {}
                #organize summaries windows by type so that all windows of the same type are in an array                                                     
                for summy in summaries[event_type]:
                    if summy[0] not in summary_window_map:
                        summary_window_map[summy[0]] = []
                    summary_window_map[summy[0]].append(summy[1])
                #Add each summary type once and give the post object the array of windows                                                                    
                for summary_type in summary_window_map:
                    mp.add_summary_type(event_type, summary_type, summary_window_map[summary_type])
         # Added the old metadata key
         mp.add_freeform_key_value("org_metadata_key", metadata_key)
         new_meta = mp.post_metadata()
         return new_meta
    
    # Post data points from a metadata
    def postBulkData(self, new_meta, old_metadata_key, datapoints, disp=False):
        et = EventTypeBulkPost(self.goc, username=self.username, api_key=self.key, metadata_key=new_meta.metadata_key)
        for event_type in datapoints.keys():
            for epoch in datapoints[event_type]:
                # packet-loss-rate is read as a float but should be uploaded as a dict with denominator and numerator                                        
                if event_type in ['packet-loss-rate', 'packet-loss-rate-bidir']:
                    # Some extra protection incase the number of datapoints in packet-loss-setn and packet-loss-rate does not match                          
                    packetcountsent = 210
                    packetcountlost = 0
                    specialTypes = ['packet-count-sent', 'packet-count-lost']
                    if event_type == 'packet-loss-rate-bidir':
                        specialTypes = ['packet-count-sent', 'packet-count-lost-bidir']
                    for specialType in specialTypes:
                        if not epoch in datapoints[specialType].keys():
                            self.add2log("Something went wrong time epoch %s not found for %s fixing it" % (specialType, epoch))
                            time.sleep(5)
                            datapoints_added = self.getMissingData(epoch, old_metadata_key, specialType)
                            # Try to get the data once more because we know it is there                                                                     
  
                            try:
                                value = datapoints_added[specialType][epoch]
                            except Exception as err:
                                datapoints_added[specialType][epoch] = 0
                            value = datapoints_added[specialType][epoch]
                            datapoints[specialType][epoch] = value
                            et.add_data_point(specialType, epoch, value)                                                                                   
  
                    packetcountsent = datapoints['packet-count-sent'][epoch]
                    if event_type == 'packet-loss-rate-bidir':
                        packetcountlost = datapoints['packet-count-lost-bidir'][epoch]
                    else:
                        packetcountlost = datapoints['packet-count-lost'][epoch]
                    et.add_data_point(event_type, epoch, {'denominator': packetcountsent, 'numerator': packetcountlost})
                    # For the rests the data points are uploaded as they are read
                else:
                    # datapoint are tuples the first field is epoc the second the value                                                                     
                    et.add_data_point(event_type, epoch, datapoints[event_type][epoch])
        if disp:
            self.add2log("Datapoints to upload:")
            self.add2log(et.json_payload())
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('error',  EventTypePostWarning)
            try:
                et.post_data()
            # Some EventTypePostWarning went wrong:                                                                                                          
            except Exception as err:
                self.add2log("Probably this data already existed")
                #self.postDataSlow(json.loads(et.json_payload()), new_meta.metadata_key, datapoints, disp)                                                   
            for event_type in datapoints.keys():
                if len(datapoints[event_type].keys()) > 0:
                    if event_type not in self.time_starts:
                        self.time_starts[event_type] = 0
                    next_time_start = max(datapoints[event_type].keys())+1
                    if next_time_start > self.time_starts[event_type]:
                        self.time_starts[event_type] = int(next_time_start)
            f = open(self.tmpDir + old_metadata_key, 'w')
            f.write(json.dumps(self.time_starts))
            f.close()
        self.add2log("posting NEW METADATA/DATA %s" % new_meta.metadata_key)

    def postData(self, arguments, event_types, summaries, summaries_data, metadata_key, datapoints, summary = True, disp=False):
        lenght_post = -1
        for event_type in datapoints.keys():
            if len(datapoints[event_type])>lenght_post:
                lenght_post = len(datapoints[event_type])
        new_meta = self.postMetaData(arguments, event_types, summaries, summaries_data, metadata_key, datapoints, summary, disp)
        # Catching bad posts                                                                                                                                 
        if new_meta is None:
                raise Exception("Post metadata empty, possible problem with user and key")
        if lenght_post == 0:
            self.add2log("No new datapoints skipping posting for efficiency")
            return
        step_size = 100
        for step in range(0, lenght_post, step_size):
            chunk_datapoints = {}
            for event_type in datapoints.keys():
                chunk_datapoints[event_type] = {}
                if len(datapoints[event_type].keys())>0:
                    pointsconsider = sorted(datapoints[event_type].keys())[step:step+step_size]
                    for point in pointsconsider:
                        chunk_datapoints[event_type][point] = datapoints[event_type][point]
            self.postBulkData(new_meta, metadata_key, chunk_datapoints, disp=False)
            # Publish to MQ                                                                                                                                 
            if self.mq and new_meta != None:
                self.publishToMq(arguments, event_types, chunk_datapoints, summaries_data)
            

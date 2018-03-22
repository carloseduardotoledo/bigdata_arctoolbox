import arcpy
import json
#import uuid
from pywebhdfs.webhdfs import PyWebHdfsClient

class Toolbox(object):
    def __init__(self):
        self.label =  "Big Data toolbox"
        self.alias  = "bigdata"

        # List of tool classes associated with this toolbox
        self.tools = [hdfs2localfs] 
        

class hdfs2localfs(object):
    
    def __init__(self):
        
        self.label = "Hive (Esri Unenclosed Json formated) to local json"
        self.description = "This tool gets files from a Hadoop WebHDFS REST API path to an Hive table" + \
                           "and concatenetes the part-* files into one output local file. " + \
                           "This tools assumes that the warehouse Hive files are formated with " + \
                           "the Esri Unenclosed Json input format."
                    

    def getParameterInfo(self):
        #Define parameter definitions
        
        # First parameter
        param0 = arcpy.Parameter(
            displayName="HDFS Host",
            name="host",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
        
        # Second parameter
        param1 = arcpy.Parameter(
            displayName="HDFS Host Port",
            name="port",
            datatype="GPString",
            parameterType="Required",
            direction="Input")        
                
        # Third  parameter
        param2 = arcpy.Parameter(
            displayName="HDFS User",
            name="user_name",
            datatype="GPString",
            parameterType="Required",
            direction="Input")

        # Fourth  parameter
        param3 = arcpy.Parameter(
            displayName="WebHDFS Path to File",
            name="webhdfsfile",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
        
        # Fourth  parameter
        param4 = arcpy.Parameter(
            displayName="Output Local File",
            name="outputlocalfile",
            datatype="DEFile",
            parameterType="Required",
            direction="Output")

        param1.defaultEnvironmentName = '50070'
        
        params = [param0, param1, param2, param3, param4]
        return params
    
    
    def execute(self, parameters, messages):
        # Parameters 
        host = parameters[0].valueAsText            #'sandbox-hdp.hortonworks.com'
        port = parameters[1].valueAsText            #'50070' 
        user_name = parameters[2].valueAsText       #'arcgis'
        webhdfsfile = parameters[3].valueAsText     #'apps/hive/warehouse/bins_agg'
        outputlocalfile = parameters[4].valueAsText #str("hfs2local_{}.tmp".format(uuid.uuid4()))

        # Webhdfs Status Dict Structure
        webhdfs_fileStatuses = 'FileStatuses'
        webhdfs_fileStatus = 'FileStatus'
        webhdfs_pathSuffix = 'pathSuffix'
        
        #Messages
        informative = "Processing {:0>3} of {} parts : {} HDFS file to local"

        hdfs = PyWebHdfsClient(host=host,port=port, user_name=user_name)

        fileStatuses = hdfs.list_dir(webhdfsfile)
        fileStatus = (fileStatuses[webhdfs_fileStatuses][webhdfs_fileStatus])

        localfile = open(outputlocalfile,'w')

        localfile.write('{"features" : [\n')

        nfiles = len(fileStatus)
        count = 1

        for file in fileStatus:
            pathSuffix = file[webhdfs_pathSuffix]
            messages.addMessage(informative.format(count, nfiles, pathSuffix))
            localfile.write(hdfs.read_file(
                "{}/{}".format(webhdfsfile,pathSuffix)).
                            decode("utf-8").replace('\n{',',\n{'))

            if count < nfiles:
                localfile.write(',\n')

            count += 1

        localfile.write('] }')
                
        localfile.close()
    

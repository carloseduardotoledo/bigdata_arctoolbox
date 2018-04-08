# -*- coding: utf-8 -*-
import arcpy
import json
import uuid
import numbers
import six
import codecs
from pywebhdfs.webhdfs import PyWebHdfsClient

class Toolbox(object):
    def __init__(self):
        self.label =  "Big Data toolbox"
        self.alias  = "bigdata"

        # List of tool classes associated with this toolbox
        self.tools = [hdfs2localfs] 
        

class hdfs2localfs(object):
    
    def __init__(self):
        
        self.label = "HDFS Esri Json to local"
        self.description = "This tool gets files from a Hadoop WebHDFS REST API path " + \
                           "and concatenetes the mapreduced part-* files into one output local file." + \
                           "This tools assumes that the files are text files containing " + \
						   "Esri JSON formated feature lines."
                    

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
        
        # Fifth  parameter
        param4 = arcpy.Parameter(
            displayName="Coordinate System",
            name="coordinate_system",
            datatype="GPCoordinateSystem",
            parameterType="Optional",
            direction="Input")
        
        # Sixth  parameter
        param5 = arcpy.Parameter(
            displayName="Output Local File",
            name="outputlocalfile",
            datatype="DEFile",
            parameterType="Required",
            direction="Output")

        param1.values = '50070'
        param5.values = str("hfs2local_{}.json".format(uuid.uuid4()))
		
        params = [param0, param1, param2, param3, param4, param5]
		
        return params
    
    
    def execute(self, parameters, messages):
        # Parameters 
        host = parameters[0].valueAsText              #'sandbox-hdp.hortonworks.com'
        port = parameters[1].valueAsText              #'50070' 
        user_name = parameters[2].valueAsText         #'arcgis'
        webhdfsfile = parameters[3].valueAsText       #'apps/hive/warehouse/geo_servicos_municipio_ano'
        coordinate_system = parameters[4].valueAsText #
        outputlocalfile = parameters[5].valueAsText   #str("hfs2local_{}.tmp".format(uuid.uuid4()))

        # Webhdfs Status Dict Structure
        webhdfs_fileStatuses = 'FileStatuses'
        webhdfs_fileStatus = 'FileStatus'
        webhdfs_pathSuffix = 'pathSuffix'
				
        #Messages
        informative = "Processing {:0>3} of {} parts : {} HDFS file to local"
		
		
        def build_fields_json(json_line):

            def generic_type_name(v):
                if isinstance(v, numbers.Integral):
                    return 'esriFieldTypeInteger'
                elif isinstance(v, numbers.Real):
                    return 'esriFieldTypeDouble'
                elif isinstance(v, six.string_types):
                    return 'esriFieldTypeString'
                else:
                    return None


            fieldAliases = '"fieldAliases":{'
            fieldAliases_element = ''

            fields = '"fields":['
            fields_element = ''

            count = 0
            for attribute in json_line["attributes"]:

                if count : 
                    fieldAliases_element += ','
                    fields_element += ','

                fieldAliases_element += '"{}":"{}"'.format(attribute,attribute)

                fields_element += '{' + '"name":"{}","type":"{}","alias":"{}"'.format( \
					attribute,generic_type_name(json_line["attributes"][attribute]),attribute) + '}'

                count += 1

            fieldAliases += fieldAliases_element + '}'
            fields += fields_element + ']'

            return '"displayFieldName":"",\n{},\n{},\n'.format(fieldAliases,fields)
		
		
        hdfs = PyWebHdfsClient(host=host,port=port, user_name=user_name)

        fileStatuses = hdfs.list_dir(webhdfsfile)
        fileStatus = (fileStatuses[webhdfs_fileStatuses][webhdfs_fileStatus])
		
        localfile = codecs.open(outputlocalfile,'w',encoding='utf8')
        #localfile = open(outputlocalfile,'w')

        localfile.write('{\n')

        pathSuffix = fileStatus[0][webhdfs_pathSuffix]
        sample_jsons = hdfs.read_file("{}/{}".format(webhdfsfile,pathSuffix)).decode("utf-8")
        sample = sample_jsons.split('\n')[0]
        localfile.write(build_fields_json(json.loads(sample)))
		
        if coordinate_system is not None:
            spatial_reference = arcpy.SpatialReference()
            spatial_reference.loadFromString(coordinate_system)
            wkid = spatial_reference.GCSCode if spatial_reference.GCSCode \
			                                 else spatial_reference.PCSCode
											 
            if wkid :
                localfile.write('"spatialReference" : {"wkid" : ' + str(wkid)+ '},\n')

        localfile.write('"features" : [\n')

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

        localfile.write(']\n')
                
        localfile.write('}')

        localfile.close()
		
        return
		
		
    

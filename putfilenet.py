#!/usr/bin/python
# usage: ./myfile.py file.txt --p 2
# This Script is used for Dynamically load file into Hadoop
# Author: Bowen Tian, Yijie Sun
# Version:0.1

import sys, getopt
import json
import os.path
from pprint import pprint

'''configuration file format
{
       {"ipaddr":""
       "grade": 0.97273,
       "load": 0.50
       }

}
'''

#Simple JSON load function
def json_load():
       with open('configuration.json') as data_file:    
       	nodes = json.load(data_file)
       return nodes

#Argument Switch
def arg_switch(argv):
    if len(argv) == 0:
        print ("USAGE: putfile.py + FILENAME + SWITCH")
        exit()
    else:
        #Check if file exists
        PATH = argv[0]
        if os.path.isfile(PATH) and os.access(PATH, os.R_OK):
            print ("File ", argv[0]," exists and is readable")

        else:
            print ("Error: Either file is missing or is not readable")
            exit()
       
#This is a switch for different priority level file
#The default priority level is 2
def priority_level(priority, nodes):
       first_level = '1'
       first_level_grade = 0.9
       second_level = '2'
       second_level_grade = 0.8
       third_level = '3'
       third_level_grade = 0.5

       if priority == first_level:
       	grade_range(first_level_grade, nodes)
       elif priority == second_level:
       	grade_range(second_level_grade, nodes)
       elif priority == third_level:
       	grade_range(third_level_grade, nodes)


#Comparasion of grade with priority level
def grade_range(low, nodes):
       accept_node={}
       result={}
       for node in nodes:
       	grade = node['grade']
       	if float(grade) > low :
       		accept_node.update({
       						"ipaddr": node['ipaddr'], 
       						"grade": node['grade']
       					})
       		result.update(accept_node)
       print (result)

       #return result

#Hadoop HDFS Function Call
def hadoop_call(filename, blocksize):
       hadoop_cmd = 'hadoop fs -D dfs.block.size= ' + blocksize + '-put' + filename + 'hdfs://master:9000/user/hduser'
       os.system('hadoop fs -D dfs.block.size= ' + blocksize + '-put' + filename + 'hdfs://master:9000/user/hduser')

#Should not be too long
def main(argv):
       priority = ''
       filename = ''
       arg_switch(argv)
       '''
        try:
       	opts, args = getopt.getopt(sys.argv[1:], "",["filename=", "priority="])
       except getopt.GetoptError as err:
       	print (str(err))
       	sys.exit(2)

        
       for opt, arg in opts:
       	if opt == '-h':
       		print ('myfile.py --p <priority> --b <balance>')
       		sys.exit()
       	#elif opt in ("-f", "--filename"):
       	#	filename = arg
       	elif opt in ("-p", "--priority"):
       		priority = arg
       print ('filename = ', filename)
       print ('priority = ', priority)
       ''' #James' Broken codes

       # got the node info dict
       nodes = {}
       nodes = json_load()
       

       # according priority level choose nodes
       priority_level(priority, nodes)

       #hadoop_call(f)

    #print 'here'



if __name__ == "__main__":
       main(sys.argv[1:])


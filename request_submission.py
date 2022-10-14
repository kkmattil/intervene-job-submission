#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 26 08:44:21 2022

@author: kkmattil
"""

import json
import datetime
import time
import sys
import re
from keystoneauth1 import session
from keystoneauth1.identity import v3
import os
import swiftclient
from swiftclient.multithreading import OutputManager
from swiftclient.service import SwiftError, SwiftService, SwiftUploadObject
import getpass

############################################################################
##upload a large file
def upload_large_file(bucket_name, object_name):
    # limit upload threads to 4
                opts = {'object_uu_threads': 4, 'os_auth_url' : _authurl, 'os_username' : _user, 'os_password' : _key, 'os_project_name' : _project, 'os_project_domain_name' : 'Default' } 
                with SwiftService(options=opts) as swift:
                    for r in swift.upload(bucket_name, [ object_name ], { 'segment_size': 5000000000}):
                        if r['success']:
                            if 'object' in r:
                                print(r['object'])
                            elif 'for_object' in r:
                                print(
                                    '%s segment %s' % (r['for_object'],
                                        r['segment_index'])
                                    )
                        else:
                            print(r)
                            
def download_large_file(bucket_name, object_name, out_file):
    # limit upload threads to 4
                opts = {'object_uu_threads': 4, 'os_auth_url' : _authurl, 'os_username' : _user, 'os_password' : _key, 'os_project_name' : _project, 'os_project_domain_name' : 'Default', 'out_file' : out_file } 
                with SwiftService(options=opts) as swift:
                    for r in swift.download(bucket_name, [ object_name ]):
                        if r['success']:
                            if 'object' in r:
                                print(r['object'])
                            elif 'for_object' in r:
                                print(
                                    '%s segment %s' % (r['for_object'],
                                        r['segment_index'])
                                    )
                        else:
                            print(r)
##
##Subroutine to manage selection from several options
def selectFromDict(options, name):
  index = 0
  indexValidList = []
  print('Select a ' + name + ':')
  for optionName in options:
     index = index + 1
     indexValidList.extend([options[optionName]])
     print(str(index) + ') ' + optionName)
  inputValid = False
  while not inputValid:
     inputRaw = input(name + ': ')
     inputNo = int(inputRaw) - 1
     if inputNo > -1 and inputNo < len(indexValidList):
         selected = indexValidList[inputNo]
         print('Selected ' +  name + ': ' + selected)
         inputValid = True
         break
     else:
         print('Please select a valid ' + name + ' number')
  return selected

###
##parameters for prs-pipe
def set_prspipe_parameters():
     
      #     inputs=["2004504-prspipe/prspipe_8.7.22.tar"]
      inputs=[
        {
        "name": "infile",
        "description": "tar-package containing all data neede",
        "bucket": "2004504-prspip",
        "object": "prspipe_8.7.22.tar",
        "url":  "/2004504-prspipe/prspipe_8.7.22.tar",
        "path": "./prspipe_8.7.22.tar",
        "type": "FILE"
        },
        ]
      output_names=["prspipe_results.zip"]       
      instructions=[
      {
       "bucket": "2004504-prspipe",
       "object": "/prspipe_README.md"
      }
      ]
      requirements=["singularity"]
      
      return(inputs, output_names, instructions, requirements)
  

def set_pgsc_calc_parameters():
      requirements=["singularity", "nextflow"]
      #     inputs=["2004504-prspipe/prspipe_8.7.22.tar"]
      inputs=[
        {
        "name": "pgsc_calc_7.10.22.tar",
        "description": "tar-package containing pgsc_calc",
        "bucket": "2004504-pgsc_calc",
        "object": "gsc_calc_7.10.22.tar",
        "url":  "/2004504-pgsc_calc/pgsc_calc_7.10.22.tar",
        "path": "./pgsc_calc_7.10.22.tar",
        "type": "FILE"
        }
        ]
      output_names=["pgsc_calc_results.zip"]
      #input file

      
      instructions=[
      {
       "bucket": "2004504-pgsc_calc",
       "object": "/pgsc_calc_README.md"
      }   
      ]
      return(inputs, output_names, instructions, requirements)
      



###
#parameters for job file
def upload_new_task(_user,conn):
   print("\n")
   taskname=input("Give a name for your task:\n")
   print("\n")
   email=input("contact email\n")
   available_biobanks=["HUS", "Estonia_Biobank"]
   jobid=(str(int(time.time()))+"-"+_user)
   workdir=(_user+"/"+str(jobid))
   storage="allas"
   print("\n")
   # Currently availalble tasks
   available_tasks = {}
   available_tasks['prspipe'] = 'prspipe'
   available_tasks['pgsc_calc'] = 'pgsc_calc'
   available_tasks['other'] = 'other'
   
   tasktype=selectFromDict(available_tasks, "Select analysis pipeline")
   
   if tasktype == "prspipe":
      task_parameters=set_prspipe_parameters()
      inputs=task_parameters[0]
      output_names=task_parameters[1]
      instructions=task_parameters[2]
      requirements=task_parameters[3]
   
   if tasktype == "pgsc_calc": 
      task_parameters=set_pgsc_calc_parameters()
      
      inputs=task_parameters[0]
      output_names=task_parameters[1]
      instructions=task_parameters[2]
      requirements=task_parameters[3]
      
      input_file=input("Give pgsc_calc input data file: ")
      pseudofolder=("jobs/"+workdir+"/input")
      upload_large_file(bucket+"/"+pseudofolder, input_file)
      input_definition={
          "name": input_file,
          "description": "input provided by requestor",
          "bucket": bucket,
          "object": (pseudofolder+input_file),
          "url": (bucket+"/"+pseudofolder+"/"+input_file),
          "path": input_file,
          "type": "FILE"
      }
      inputs.append(input_definition)
      
   if tasktype == "other":
       pseudofolder=("jobs/"+workdir+"/input")
       #Define input files
       inputs = []
       input_file = "x"
       print("\n")
       print("Give the names of input files including the sorfware containers.")
       print("Give empty file name to stop importing new input files.")
       while input_file != "":
           input_file=input("Give new input data file: ")
           print("\n")
           if input_file != "":
               upload_large_file(bucket+"/"+pseudofolder, input_file)
               input_definition={
               "name": input_file,
               "description": "input provided by requestor",
               "bucket": bucket,
               "object": pseudofolder+"/"+input_file,
               "url": (bucket+"/"+pseudofolder+"/"+input_file),
               "path": input_file,
               "type": "FILE"
               }
               inputs.append(input_definition)
         
       #Define output files        
       output_names = []
       output_file = "x"
       print("\n")       
       print("Give the names of output files to be returned.")
       print("Give empty file name to stop importing new output files.")
       while output_file != "":
           print("\n")
           output_file=input("Give new output data file: ")
           if output_file != "":
               output_names.append(output_file)
               
       #Define requirements
       requirements = []
       requirement = "x"
       print("\n")
       print("Give the techincal requirements of the task.")
       print("Give empty name to stop importing new requirements.")
       while requirement != "":
           if requirement != "":
              print("\n")
              requirement=input("Give requirement: ")
              requirements.append(requirement)
 
        #Define instructions
       print("\n")
       print("Give the file that contains instructios to run the task.")
       instructions_file=input("Give instructions data file: ")
       upload_large_file(bucket+"/"+pseudofolder, instructions_file)
       instructions=[
       {
        "bucket": bucket,
        "object": (pseudofolder+"/"+instructions_file)
       }   
       ]
       
       
       

   #create task directories and files for each biobank
   for biobank in available_biobanks:

     job_file_name="task.json"

     outputs= []
     for ofile in output_names: 
        o_item= {
         "url" :  (bucket+"/jobs/"+workdir+"/"+biobank+"/results/"+ofile),
         "path" : ("./"+ofile)
         }
        outputs.append(o_item)

     task_json = {"ID": jobid,
       "name": taskname,
       "descripotion": "none",
       "csc-user": _user,
       "requestor": email,
       "date": str(datetime.datetime.now()),
       "diobanks": available_biobanks,
       "requirements": requirements,
       "storageserver": storage,
       "inputs": inputs,        
       "outputs": outputs,
       "instructions": instructions    
       }     
    
     print("Uploading job "+jobid+" to "+bucket+" for "+biobank)

     #move object to allas
     object_dir=("jobs/"+workdir+"/"+biobank)
     object_name=(object_dir+"/"+job_file_name)
     conn.put_object(bucket, object_name, contents=json.dumps(task_json, indent=2), content_type='text/plain')
   
     #   copy_large_file(object_dir, job_file_name)
     full_allas_path=(object_dir+"/"+job_file_name)

     #create status object
     status_object=("jobs/"+workdir+"/"+biobank+"/status/submitted")
     conn.put_object(bucket, status_object ,
                    contents=(task_json["date"]),
                    content_type='text/plain')
   
     #make a note file for biobak.
     note_object=(biobank+"/requests/"+jobid)
     conn.put_object(bucket, note_object ,
                    contents=(full_allas_path),
                    content_type='text/plain')

####################################################################
# Main program starts here



#parameters for allas connection

_authurl = 'https://pouta.csc.fi:5001/v3'
_auth_version = '3'
_user = input('CSC username: \n')
_key = getpass.getpass('CSC password: \n')



_os_options = {
    'user_domain_name': 'Default',
    'project_domain_name': 'Default',
    'project_name': 'project_2004504'
}
_project = _os_options["project_name"]

conn = swiftclient.Connection(
    authurl=_authurl,
    user=_user,
    key=_key,
    os_options=_os_options,
    auth_version=_auth_version
)
bucket=(_project+"-intervene-tasks")

dothis = "start"



job_operations = {}
job_operations['list'] = 'list'
job_operations['submit'] = 'submit'
job_operations['download'] = 'download'
job_operations['delete'] = 'delete'
job_operations['quit'] = 'quit'


while dothis != "quit":

  dothis=selectFromDict(job_operations, "Select task")


  if dothis == "list":
      ##List my tasks
      print("Tasks submitted by user "+_user)
      for j in conn.get_container(bucket)[1]:
         if re.search("jobs/"+_user, j["name"] ):
           if re.search("/status/", j["name"] ):
              print(j["name"])
              
  if dothis == "submit":
      upload_new_task(_user, conn)

  if dothis == "delete":
      previous_jobid="x"
      job_dict = {}
      print("Tasks submitted by user "+_user)
      for j in conn.get_container(bucket)[1]:
        if re.search("jobs/"+_user, j["name"] ):
          if re.search("/status/", j["name"] ):
              job_status_objects=(j["name"])
              new_jobid=job_status_objects.split("/")[3]            
              if new_jobid != previous_jobid:
                 job_dict[new_jobid] = new_jobid
                 
              previous_jobid = new_jobid
              
      job_dict["None"] = "None"
      jobid=selectFromDict(job_dict, "Select task")          
              #print(job_status_objects.split("/")[3])
      if jobid != "None" :
         for j in conn.get_container(bucket)[1]:
           print(j["name"]) 
           if re.search(jobid, j["name"]):      
               print("Deleting object "+j["name"])
               conn.delete_object(bucket,j["name"])
   ##with open(job_file_name, "w") as outfile:
   #     json.dump(task_json, outfile, indent=2)
   #     outfile.close()
   #
   #
   ##make a note file for biobak.
   #job_file_name=(jobid)
   #with open(job_file_name, "w") as outfile:
       # outfile.write(task_json["Date"])
       # outfile.write(" "+os.uname()[1])
       # outfile.write("\n")
      #    outfile.write(full_allas_path)
      #  outfile.write("\n")
       # outfile.close

   #object_dir=(bucket+"/"+biobank+"/requests")
   #copy_large_file(object_dir, job_file_name)

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
import os
import swiftclient
from swiftclient.multithreading import OutputManager
from swiftclient.service import SwiftError, SwiftService, SwiftUploadObject
import getpass

############################################################################
##upload a large file


def get_task_json(target_task):
    task_json_link=("jobs/"+_user+"/"+target_task+"/task.json")
    task_json=conn.get_object(bucket, task_json_link)[1]
    task_description=json.loads(task_json)
    return task_description

def get_task_json_biobank(target_task, biobank):
    task_json_link=("jobs/"+_user+"/"+target_task+"/"+biobank+"/task.json")
    task_json=conn.get_object(bucket, task_json_link)[1]
    task_description=json.loads(task_json)
    return task_description


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
   taskname=input("Give a name for your task:")
   print("\n")
   email=input("contact email")
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
           "bucket": bucket,  
           "object": ("jobs/"+workdir+"/"+biobank+"/results/"+ofile),
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
         "biobanks": available_biobanks,
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
   
   #add common json file
   object_name=("jobs/"+workdir+"/task.json") 
   conn.put_object(bucket, object_name, contents=json.dumps(task_json, indent=2), content_type='text/plain')

def update_task_lists(conn):
   #this function returns updated my_tasks list and task_status_info that 
   # is a list of dictionaries
   #list all objects in the bucket
   all_objs=conn.get_container(bucket)[1] 
   #print("Debug1:")
   #print(all_objs)
   my_tasks=[]
   task_status_info=[]
   task_status_dict={}
   if len(all_objs) == 0 :
     return(my_tasks,task_status_info)
   
   print("Collecting task information.")

   for j in all_objs:
      if re.search("jobs/"+_user+"/", j["name"] ):
         #if re.search("/status/", j["name"] ):
         task_row=(j["name"])
         #print("Task row: "+task_row)
         task=task_row.split("/")[2]
         if task not in my_tasks:
            my_tasks.append(task)  
            #print("Adding:"+task)  
        
         
   #print("Debug 3 ")
   #print(my_tasks)      
   for task in my_tasks:
      task_desc=get_task_json(task)
      biobanks=task_desc["biobanks"]
      #print(task+" : ")
      #print(biobanks)
      task_status_dict={"id": task}
      #print("Evaluating "+task+" in ")
      #print(biobanks)
      for biobank in biobanks:
          
         task_stat_num=0
         # check all object for given biobank and task combibation
         for jj in all_objs:
            if re.search(task+"/"+biobank+"/status/ready", jj["name"]):
               task_stat_num=task_stat_num+4
               #print(jj["name"]+" ready"+task_stat_num)
            if re.search(task+"/"+biobank+"/status/processing", jj["name"]):
               task_stat_num=task_stat_num+2
               #print(jj["name"]+" proc")
            if re.search(task+"/"+biobank+"/status/submitted", jj["name"]):
               task_stat_num=task_stat_num+1
               #print(jj["name"]+" submitted")

         # infer the status of tasks based on checks
         if task_stat_num==7 or task_stat_num==5 or task_stat_num==4:
            task_status_dict[biobank]="ready"
            #print("Debug:"+task+" "+biobank+"ready")
         if task_stat_num==3:
            task_status_dict[biobank]="processing"
            
         if task_stat_num==1:
            task_status_dict[biobank]="submitted" 
         
         
            

      task_status_info.append(task_status_dict)      
      
   return(my_tasks,task_status_info)



####################################################################
# Main program starts here
#


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

#####################


##List my tasks
my_tasks=[]
task_selection_dict = {}
task_download_dict= {}
task_upload_dict={}

################
my_tasks, task_status_info = update_task_lists(conn)

job_operations = {}
job_operations['list'] = 'list'
job_operations['submit'] = 'submit'
job_operations['download'] = 'download'
job_operations['delete'] = 'delete'
job_operations['update task list'] = 'update task list'
job_operations['quit'] = 'quit'
#biobanks=["HUS","Estonia_Biobank"]

while dothis != "quit":

  dothis=selectFromDict(job_operations, "task")

  if dothis == "update task list":
       my_tasks, task_status_info = update_task_lists(conn)


  if dothis == "list":
      ##List my tasks
      print("Tasks submitted by user "+_user)
      for task_status_dict in task_status_info:
          print("Task:"+task_status_dict["id"])
          task_desc=get_task_json(task_status_dict["id"])
          biobanks=task_desc["biobanks"]
          for biobank in biobanks:
              #print out status of each biobank
              print("           "+biobank+": "+task_status_dict[biobank])
          print(" ")    
              
  if dothis == "submit":
      upload_new_task(_user, conn)
      my_tasks, task_status_info = update_task_lists(conn)
 
    
  if dothis == "delete":
      #previous_jobid="x"
      job_dict = {}
      for jobid in my_tasks:
          job_dict[jobid] = jobid
              
      job_dict["None"] = "None"
      jobid=selectFromDict(job_dict, "Select task")          
              #print(job_status_objects.split("/")[3])
      if jobid != "None" :
         for j in conn.get_container(bucket)[1]:
           print(j["name"]) 
           if re.search(jobid, j["name"]):      
               print("Deleting object "+j["name"])
               conn.delete_object(bucket,j["name"])
 
      # update task list after deleting
      my_tasks, task_status_info = update_task_lists(conn)
        
      
  if dothis == "download":
     print("Listing ready tasks") 
     tasks_ready=[]
     # check taks that are ready  for download
     my_tasks, task_status_info = update_task_lists(conn)
     for task_status_dict in task_status_info:
         task=task_status_dict["id"]
         #print("Task:"+task)
         task_desc=get_task_json(task)
         biobanks=task_desc["biobanks"]
         non_ready_biobanks=0
         ready_biobanks=0
         for biobank in biobanks:
             #print out status of each biobank
             if task_status_dict[biobank] != "ready":
                non_ready_biobanks=(non_ready_biobanks + 1)
             if task_status_dict[biobank] == "ready":
                ready_biobanks=(ready_biobanks +1 )
         if non_ready_biobanks == 0 :
            tasks_ready.append(task)
            print(task+" ready") 
         else:
            print(task+" "+str(ready_biobanks)+" bioanks ready. " +str(non_ready_biobanks)+" biobanks missing." )
     
        
     task_download_dict={}
     for task in tasks_ready:
          task_download_dict[task]=task        
    
     task_download_dict["none"]="none"
     download_task=selectFromDict(task_download_dict, "task")

     # Download the output files or selected task
     if download_task != "none":         
        task_desc=get_task_json(download_task)       
        downloaddir=download_task
        isExist = os.path.exists(downloaddir)
        #print("Dir test")
        if not isExist:
           # Create a new directory because it does not exist
           os.makedirs(downloaddir)
        for biobank in task_desc["biobanks"]:
          #create biobank specific subdeirectories
          isExist = os.path.exists(downloaddir+"/"+biobank)
          if not isExist:
             os.makedirs(downloaddir+"/"+biobank)
             task_biobank_desc=get_task_json_biobank(download_task, biobank)
          for output_file in task_biobank_desc["outputs"]:
             print(" ")
             print("Downloading "+output_file["url"]+" to "+download_task+"/"+biobank+"/"+output_file["path"])
             download_large_file(bucket, output_file["object"], download_task+"/"+biobank+"/"+output_file["path"])
     


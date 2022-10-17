#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  4 17:40:27 2022

@author: kkmattil

"""

import json
import datetime
import re
from keystoneauth1 import session
from keystoneauth1.identity import v3
import os
import swiftclient
from swiftclient.multithreading import OutputManager
from swiftclient.service import SwiftError, SwiftService, SwiftUploadObject
import getpass


def get_task_json(task):
    object_contents=conn.get_object(bucket, biobank+"/requests/"+task, headers=None)[1]
    oc_string=str(object_contents)
    task_json_link=oc_string.split(sep="'")[1]
    task_json_link.strip
    #print(task_json_link)

    task_json=conn.get_object(bucket, task_json_link)[1]
    task_description=json.loads(task_json)
    return task_description
    
    #task_desc_txt=json.dumps(task_description, indent=2)


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
                #print(opts)
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
                            
                            
#Subroutine to manage selection from several options
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


def update_biobank_tasks(conn,biobank):
    
   #list all objects in the bucket
   all_objs=conn.get_container(bucket)[1] 
   my_tasks=[]
   print("Collecting task infomration.\n")
   for j in all_objs:
      if re.search(biobank+"/requests/", j["name"] ):
         #if re.search("/status/", j["name"] ):
         task_row=(j["name"])
         task=task_row.split("/")[-1]
         my_tasks.append(task)  
         # task_selection_dict[task] = task


   ready_tasks=[]
   tasks_processing=[]
   waiting_tasks=[]
   for task in my_tasks:
      task_stat_num=0
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
      if task_stat_num==7 or task_stat_num==5:
          ready_tasks.append(task)
          task_selection_dict[task+" (ready)"] = task
      if task_stat_num==3:
          tasks_processing.append(task)
          task_selection_dict[task+" (active)"] = task
          task_upload_dict[task] = task
      if task_stat_num==1:
           waiting_tasks.append(task)
           task_selection_dict[task+" (waiting)"] = task
           task_download_dict[task]=task
           
   return(my_tasks, ready_tasks, tasks_processing, waiting_tasks, task_selection_dict, task_upload_dict, task_download_dict)
#print(my_tasks)

#target_task=selectFromDict(task_selection_dict, "task to be processed")


#
# Main program starts here
#


#parameters for allas connection

_authurl = 'https://pouta.csc.fi:5001/v3'
_auth_version = '3'
_user = input('CSC username: \n')
_key = getpass.getpass('CSC password: \n')
storage="allas"

_os_options = {
    'user_domain_name': 'Default',
    'project_domain_name': 'Default',
    'project_name': 'project_2004504'
}
_project = _os_options["project_name"]
bucket=(_project+"-intervene-tasks")


conn = swiftclient.Connection(
    authurl=_authurl,
    user=_user,
    key=_key,
    os_options=_os_options,
    auth_version=_auth_version
)


# Currently availalble biobanks
available_biobanks = {}
available_biobanks['Estonia Biobank'] = 'Estonia_Biobank'
available_biobanks['HUS'] = 'HUS'

#select right biobank
biobank=selectFromDict(available_biobanks, "Select your Biobank.")

##List my tasks
my_tasks=[]
task_selection_dict = {}
task_download_dict= {}
task_upload_dict={}

#print("Tasks submitted to be processed in  "+biobank)
my_tasks, ready_tasks, tasks_processing, waiting_tasks, task_selection_dict, task_upload_dict, task_download_dict = update_biobank_tasks(conn,biobank)


#select operation
operation=""
operations = {}
operations["Show task list"] = "list"
operations["Show the job request file"] = "info"
operations["Download task for processing"] = "download"
operations["Upload results"] = "upload"
operations["Quit"] = "quit"

# keep running untill quit
while operation != "quit":
   operation=selectFromDict(operations, "operation")
   
   #
   # List all tasks
   #
   if operation=="list":
       my_tasks, ready_tasks, tasks_processing, waiting_tasks, task_selection_dict, task_upload_dict, task_download_dict = update_biobank_tasks(conn,biobank)
       
       print("-----------------------------------------------------------")
       print("Ready tasks:")
       for task in ready_tasks:
           task_name=get_task_json(task)["name"]
           print(task+" "+task_name)
       print("-----------------------------------------------------------")
       print("Tasks that have already been downloaded for processing:")
       for task in tasks_processing:
           task_name=get_task_json(task)["name"]
           print(task+" "+task_name)
       print("-----------------------------------------------------------")    
       print("Tasks that are waiting to be downloaded for processing:")
       for task in waiting_tasks:
           task_name=get_task_json(task)["name"]
           print(task+" "+task_name+" Waiting.")
       print("-----------------------------------------------------------")
   #
   #show info for one task
   #
   if operation=="info":
      target_task=selectFromDict(task_selection_dict, "task to be processed")
      task_description=get_task_json(task)
    
      task_desc_txt=json.dumps(task_description, indent=2)
      print(task_desc_txt)


   #
   # download a task
   #
   if operation == "download":
      my_tasks, ready_tasks, tasks_processing, waiting_tasks, task_selection_dict, task_upload_dict, task_download_dict = update_biobank_tasks(conn,biobank)
      target_task=selectFromDict(task_download_dict, "Task to be downloaded for processing.")
      task_description=get_task_json(target_task)
      downloaddir=target_task
      isExist = os.path.exists(downloaddir)
      print("Dir test")
      if not isExist:
         # Create a new directory because it does not exist
         os.makedirs(downloaddir)
   
      print("Get inputs")
      # Inputfiles to download
      for input_file in task_description["inputs"]:
          print(" ")
          print("Downloading "+input_file["url"]+" to "+target_task+"/"+input_file["path"])
          print(" ")
          download_large_file(input_file["bucket"], input_file["object"], target_task+"/"+input_file["path"])
 
      print("get instuctions")
      # instuctions file    
      for input_file in task_description["instructions"]:
          download_large_file(input_file["bucket"], input_file["object"], target_task+"/README_instructions")
   
          #    print(input_file["url"]+"->"+target_task+"/"+input_file["path"])
          #   print("\n")
      print("write json")
     #task_json
      with open(target_task+"/task.json", "w") as outfile:
          outfile.write(json.dumps(task_description, indent=2))
          outfile.close 
   
      #create status object
      workdir=(task_description["csc-user"]+"/"+target_task)
   
      status_object=("jobs/"+workdir+"/"+biobank+"/status/processing")
      conn.put_object(bucket, status_object ,
                  contents=(_user+": "+str(datetime.datetime.now())),
                  content_type='text/plain')
   
   
      my_tasks, ready_tasks, tasks_processing, waiting_tasks, task_selection_dict, task_upload_dict, task_download_dict = update_biobank_tasks(conn,biobank)
      print("Job downloaded to local directory "+target_task)
      print("Task specific instaructions are available in: "+target_task+"/README_instructions")
        
   #
   # uoload a ready task
   #
   if operation == "upload":
       
       target_task=input('Task to upload: \n')
       task_description=get_task_json(task)
       #target_task=selectFromDict(task_download_dict, "Task to be uploaded for processing.")
       
       print("Cheking result files.")     
       for output_file in task_description["outputs"]:
           if os.path.isfile(output_file["path"]):
              print("Result file "+output_file["path"]+" found.")                   
           else:   
              print("ERROR: Result file "+output_file["path"]+"not found!")
              quit()  
    
       print("All result files found. startng upload")
       for output_file in task_description["outputs"]:       
          print("uploadig: "+output_file["url"]+"->"+output_file["path"])
          upload_large_file(bucket+"/jobs/"+task_description["csc-user"]+"/"+target_task+"/"+biobank+"/results", output_file["path"])
          #upload_large_file(output_file["url"], output_file["path"]) 
       
       
       #create status object
       workdir=(task_description["csc-user"]+"/"+target_task)

       status_object=("jobs/"+workdir+"/"+biobank+"/status/ready")
       conn.put_object(bucket, status_object ,
                  contents=(_user+": "+str(datetime.datetime.now())),
                  content_type='text/plain')
       my_tasks, ready_tasks, tasks_processing, waiting_tasks, task_selection_dict, task_upload_dict, task_download_dict = update_biobank_tasks(conn,biobank)

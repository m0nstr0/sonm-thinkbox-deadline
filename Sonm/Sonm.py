from Deadline.Cloud import *
from Deadline.Scripting import *
from subprocess import check_output
import json
import time
import os
import tempfile
import traceback
from collections import OrderedDict

######################################################################
## This is the function that Deadline calls to get an instance of the
## main CloudPluginWrapper class.
######################################################################
def GetCloudPluginWrapper():
    return SonmCloud()

######################################################################
## This is the function that Deadline calls when the cloud plugin is
## no longer in use so that it can get cleaned up.
######################################################################
def CleanupCloudPlugin( deadlinePlugin ):
    deadlinePlugin.Cleanup()

######################################################################
## This is the main DeadlineCloudListener class for MyCloud.
######################################################################
class SonmCloud (CloudPluginWrapper):

    DEAL_ANY        = 0
    DEAL_PENDING    = 1
    DEAL_ACCEPTED   = 2
    DEAL_CLOSED     = 3

    TASK_IS_NOT_DEADLINE = -1
    TASK_UNKNOWN         = 0
    TASK_SPOOLING        = 1
    TASK_SPAWNING        = 2
    TASK_RUNNING         = 3
    TASK_FINISHED        = 4
    TASK_BROKEN          = 5

    DEADLINE_IMG_NAME = "m0nstr0/deadline"

    def __init__( self ):
        ClientUtils.LogText("SonmCloud init.")
        #Set up our callbacks for cloud control
        self.VerifyAccessCallback += self.VerifyAccess
        self.AvailableHardwareTypesCallback += self.GetAvailableHardwareTypes
        self.AvailableOSImagesCallback += self.GetAvailableOSImages
        self.CreateInstancesCallback += self.CreateInstances
        self.TerminateInstancesCallback += self.TerminateInstances
        self.CloneInstanceCallback += self.CloneInstance
        self.GetActiveInstancesCallback += self.GetActiveInstances
        self.StopInstancesCallback += self.StopInstances
        self.StartInstancesCallback += self.StartInstances
        self.RebootInstancesCallback += self.RebootInstances

    def Cleanup( self ):
        #Clean up our callbacks for cloud control
        del self.VerifyAccessCallback
        del self.AvailableHardwareTypesCallback
        del self.AvailableOSImagesCallback
        del self.CreateInstancesCallback
        del self.TerminateInstancesCallback
        del self.CloneInstanceCallback
        del self.GetActiveInstancesCallback
        del self.StopInstancesCallback
        del self.StartInstancesCallback
        del self.RebootInstancesCallback

    def Yaml(self, v, level):
        result = ""
        for key, value in v.iteritems():
            for i in range(level):
                result = result + " "
            result = result +  str(key) + ":"
            if isinstance(value, dict):
                result = result + "\n"
                result = result + self.Yaml(value, level+1)    
            else:
                result = result +  " " + str(value) + "\n"
        return result

    def GenerateBidYaml( self ):
        BID = OrderedDict()
        BID['duration'] = self.GetConfigEntryWithDefault("Duration", "")
        BID['resources'] = OrderedDict()
        BID['resources']['cpu_cores'] = self.GetConfigEntryWithDefault("CpuCores", "")
        BID['resources']['ram_bytes'] = self.GetConfigEntryWithDefault("RamBytes", "")
        BID['resources']['gpu_count'] = self.GetConfigEntryWithDefault("GpuCount", "")
        BID['resources']['storage'] = self.GetConfigEntryWithDefault("Storage", "")
        BID['resources']['network'] = OrderedDict()
        BID['resources']['network']['in'] = self.GetConfigEntryWithDefault("NetworkIn", "")
        BID['resources']['network']['out'] = self.GetConfigEntryWithDefault("NetworkOut", "")
        BID['resources']['network']['type'] = self.GetConfigEntryWithDefault("NetworkType", "")
        BID['resources']['properties'] = OrderedDict()
        BID['resources']['properties']['sonm_deadline'] = '1'
        
        return self.Yaml(BID, 0)

    def GenerateTaskYaml( self ):
        TASK = OrderedDict()
        TASK["task"] = OrderedDict()
        TASK["task"]["miners"] = []
        TASK["task"]["container"] = OrderedDict()
        TASK["task"]["container"]["commit_on_stop"] = "true"
        TASK["task"]["container"]["name"] = self.DEADLINE_IMG_NAME
        TASK["task"]["container"]["volumes"] = OrderedDict()
        TASK["task"]["container"]["volumes"]["cifs"] = OrderedDict()
        TASK["task"]["container"]["volumes"]["cifs"]["type"] = "cifs"
        TASK["task"]["container"]["volumes"]["cifs"]["options"] = OrderedDict()
        TASK["task"]["container"]["volumes"]["cifs"]["options"]["share"] = self.GetConfigEntryWithDefault("CifsShare", "")
        TASK["task"]["container"]["volumes"]["cifs"]["options"]["username"] = self.GetConfigEntryWithDefault("CifsUsername", "")
        TASK["task"]["container"]["volumes"]["cifs"]["options"]["password"] = self.GetConfigEntryWithDefault("CifsPassword", "")
        TASK["task"]["container"]["volumes"]["cifs"]["options"]["vers"] = "3.0"
        TASK["task"]["container"]["mounts"] = ["cifs:/mnt/deadlinerepository10:rw"]
        TASK["task"]["resources"] = OrderedDict()
        TASK["task"]["resources"]["CPU"] = self.GetConfigEntryWithDefault("CpuCores", "")
        TASK["task"]["resources"]["RAM"] = self.GetConfigEntryWithDefault("RamBytes", "")
        TASK["task"]["resources"]["GPU"] = "false" if self.GetConfigEntryWithDefault("GpuCount", "") == "NO_GPU" else "true"
        return self.Yaml(TASK, 0)

    def GenerateFileWithYaml(self, data):
        temp_dir = os.path.abspath(tempfile.gettempdir())
        fname = 'tmp_' + str(time.time())
        postfix = 0
        while os.path.isfile(os.path.join(temp_dir, fname + str(postfix) + ".yml")):
            postfix = postfix + 1
        out_yaml = os.path.join(temp_dir, fname + str(postfix) + ".yml")
        f = open(out_yaml, 'w')
        f.write(data)
        f.close()

        return out_yaml

    def VerifyAccess( self ):
        return True

    def GetAvailableHardwareTypes( self ):
        ht_list = []
        ht = HardwareType()
        ht.ID = "SONM"
        ht.Name = "SONM"
        ht_list.append(ht)

        return ht_list

    def GetAvailableOSImages( self ):
        osi_list = []
        osi = OSImage()
        osi.ID = "SONM"
        osi.Description = "SONM"
        osi_list.append(osi)
        
        return osi_list

    def StartTask(self, dealID):
        ClientUtils.LogText( "[SONM] StartTask" )

        node = self.GetConfigEntryWithDefault("NodeConfig", "")

        cli = self.GetConfigEntryWithDefault("CliConfig", "")

        file_path = self.GenerateFileWithYaml(self.GenerateTaskYaml())

        try:
            response = check_output([cli, "tasks", "start", dealID, file_path, "--node", node, "--out", "json"])
            task = json.loads(response)

            if not isinstance(task, dict):
                return self.TASK_UNKNOWN

            if "error" in task:
                ClientUtils.LogText(task["error"])
                return self.TASK_UNKNOWN

            if "id" not in task:
                return self.TASK_UNKNOWN

        except Exception as e:
            ClientUtils.LogText( traceback.format_exc() )
        
        return self.TASK_SPAWNING

    def isDealDeadline(self, task):
        if not isinstance(task, dict):
            return False

        if "info" not in task:
            return False

        if "order" not in task["info"]:
            return False

        if "slot" not in task["info"]["order"]:
            return False

        if "resources" not in task["info"]["order"]["slot"]:
            return False

        if "properties" not in task["info"]["order"]["slot"]["resources"]:
            return False

        if "sonm_deadline" not in task["info"]["order"]["slot"]["resources"]["properties"]:
            return False

        return True

    def ParseTask (self, dealID):
        ClientUtils.LogText( "[SONM] ParseTask" )

        node = self.GetConfigEntryWithDefault("NodeConfig", "")

        cli = self.GetConfigEntryWithDefault("CliConfig", "")

        result = []

        try:
            response = check_output([cli, "deals", "status", dealID, "--node", node, "--out", "json"])
            task = json.loads(response)

            if not self.isDealDeadline(task):
                return self.TASK_IS_NOT_DEADLINE

            if "running" not in task["info"]:
                return self.TASK_UNKNOWN

            #if this is true than deal is just spawned
            if "statuses" not in task["info"]["running"]:
                if "statuses" not in task["info"]["completed"]:
                    #start task
                    return self.StartTask(dealID)

            #find image in running
            if "statuses" in task["info"]["running"]:
                for v in task["info"]["running"]["statuses"]:
                    if task["info"]["running"]["statuses"][v]["imageName"] == self.DEADLINE_IMG_NAME:
                        return task["info"]["running"]["statuses"][v]["status"]

            #find image in completed
            if "statuses" in task["info"]["completed"]:
                for v in task["info"]["completed"]["statuses"]:
                    ClientUtils.LogText(v)
                    if task["info"]["completed"]["statuses"][v]["imageName"] == self.DEADLINE_IMG_NAME:
                        return task["info"]["completed"]["statuses"][v]["status"]

            #deadline image has not been found, start task
            return self.StartTask(dealID)

        except Exception as e:
            ClientUtils.LogText( traceback.format_exc() )
        
        return self.TASK_IS_NOT_DEADLINE

    def ParseDeals (self):
        ClientUtils.LogText( "[SONM] ParseDeals" )

        activeInstances = []
        
        #check node config
        node = self.GetConfigEntryWithDefault("NodeConfig", "")
        if len(node.strip()) <= 0:                
            ClientUtils.LogText("[SONM] CreateInstances - Please enter path to node.")
            raise Exception("[SONM] Please enter path to node.")


        #check cli config
        cli = self.GetConfigEntryWithDefault("CliConfig", "")
        if len(cli.strip()) <= 0:                
            ClientUtils.LogText("[SONM] CreateInstances - Please enter path to cli.")
            raise Exception("[SONM] Please enter path to cli.")

        try:
            response = check_output([cli, "deals", "list", "--node", node, "--out", "json"])
            
            ClientUtils.LogText(response)
            
            deals = json.loads(response)

            if "deals" not in deals :
                ClientUtils.LogText("[SONM] Deals not found")
                return activeInstances

            if deals["deals"] is None:
                return activeInstances

            for v in deals["deals"]:
                #parse deals with only accepted status
                if v["status"] != self.DEAL_ACCEPTED:
                    continue

                task_status = self.ParseTask(v["id"])

                if task_status == self.TASK_IS_NOT_DEADLINE:
                    continue

                ci = CloudInstance()
                ci.ID = v["id"]
                ci.Name = "SONM " + v["id"]
                ci.Hostname = "SONM"
                
                if task_status == self.TASK_UNKNOWN:
                    ci.Status = InstanceStatus.Unknown

                if task_status == self.TASK_SPOOLING:
                    ci.Status = InstanceStatus.Pending

                if task_status == self.TASK_SPAWNING:
                    ci.Status = InstanceStatus.Pending

                if task_status == self.TASK_RUNNING:
                    ci.Status = InstanceStatus.Running

                if task_status == self.TASK_FINISHED:
                    ci.Status = InstanceStatus.Stopped

                if task_status == self.TASK_BROKEN:
                    ci.Status = InstanceStatus.Stopped 

                activeInstances.append(ci)
                
        except Exception as e:
            ClientUtils.LogText( traceback.format_exc() )
        
        return activeInstances

    def GetActiveInstances( self ):
        return self.ParseDeals()

    def CreateInstances( self, hardwareID, imageID, count ):
        
        ClientUtils.LogText( "[SONM] CreateInstances" )

        startedInstances = []
        
        node = self.GetConfigEntryWithDefault("NodeConfig", "")
        if len(node.strip()) <= 0:                
            ClientUtils.LogText("[SONM] CreateInstances - Please enter path to node.")
            raise Exception("[SONM] Please enter path to node.")


        cli = self.GetConfigEntryWithDefault("CliConfig", "")
        if len(cli.strip()) <= 0:                
            ClientUtils.LogText("[SONM] CreateInstances - Please enter path to cli.")
            raise Exception("[SONM] Please enter path to cli.")

        price = self.GetConfigEntryWithDefault("Price", "")
        if len(cli.strip()) <= 0:                
            ClientUtils.LogText("[SONM] CreateInstances - Please enter price.")
            raise Exception("[SONM] Please enter price.")

        supplier = self.GetConfigEntryWithDefault("Supplier", "")

        #generate temp file for order.yaml
        bid_yaml = self.GenerateFileWithYaml(self.GenerateBidYaml())

        for i in range(count):

            try:
                #if supplier is'n set
                if len(cli.strip()) <= 0:
                    response = check_output([cli, "market",  "create", price, str(bid_yaml), "--node", node, "--out", "json"])
                else:
                    response = check_output([cli, "market",  "create", price, str(bid_yaml), supplier, "--node", node, "--out", "json"])

                ClientUtils.LogText(response)

                response = json.loads(response)

                if "error" in response :
                    ClientUtils.LogText("[SONM] " + response['message'])
                    continue
                elif "id" not in response:
                    ClientUtils.LogText("[SONM] Task ID not found")
                    continue

                ClientUtils.LogText("[SONM] Task ID is " + response["id"])

            except Exception as e:
                ClientUtils.LogText( traceback.format_exc() )

        ClientUtils.LogText("[SONM] " + str(len(startedInstances)) + " from " + str(count) + " instances has been created")
        return []

    #deals finish <deal-id>
    def TerminateInstances( self, instanceIDs ):
        node = self.GetConfigEntryWithDefault("NodeConfig", "")
        if len(node.strip()) <= 0:                
            ClientUtils.LogText("[SONM] CreateInstances - Please enter path to node.")
            raise Exception("[SONM] Please enter path to node.")

        #check cli config
        cli = self.GetConfigEntryWithDefault("CliConfig", "")
        if len(cli.strip()) <= 0:                
            ClientUtils.LogText("[SONM] CreateInstances - Please enter path to cli.")
            raise Exception("[SONM] Please enter path to cli.")

        try:
            for v in instanceIDs:
                response = check_output([cli, "deals", "finish", v, "--node", node, "--out", "json"])

        except Exception as e:
            ClientUtils.LogText( traceback.format_exc() )

    #tasks stop <hub-address> <task-id>
    def StopInstances( self, instanceIDs ):
        node = self.GetConfigEntryWithDefault("NodeConfig", "")
        if len(node.strip()) <= 0:                
            ClientUtils.LogText("[SONM] CreateInstances - Please enter path to node.")
            raise Exception("[SONM] Please enter path to node.")

        #check cli config
        cli = self.GetConfigEntryWithDefault("CliConfig", "")
        if len(cli.strip()) <= 0:                
            ClientUtils.LogText("[SONM] CreateInstances - Please enter path to cli.")
            raise Exception("[SONM] Please enter path to cli.")

        try:
            for v in instanceIDs:
                response = check_output([cli, "deals", "status", v, "--node", node, "--out", "json"])
                task = json.loads(response)

                if not self.isDealDeadline(task):
                    continue

                if "running" not in task["info"]:
                    continue

                if "statuses" not in task["info"]["running"]:
                    continue

                #find image in running
                if "statuses" in task["info"]["running"]:
                    for task_id in task["info"]["running"]["statuses"]:
                        if task["info"]["running"]["statuses"][task_id]["imageName"] == self.DEADLINE_IMG_NAME:
                            response = check_output([cli, "tasks", "stop", task["deal"]["SupplierID"], task_id, "--node", node, "--out", "json"])

        except Exception as e:
            ClientUtils.LogText( traceback.format_exc() )

    #task start <deal-id> <task-yaml>
    def StartInstances( self, instanceIDs ):
        node = self.GetConfigEntryWithDefault("NodeConfig", "")
        if len(node.strip()) <= 0:                
            ClientUtils.LogText("[SONM] CreateInstances - Please enter path to node.")
            raise Exception("[SONM] Please enter path to node.")

        #check cli config
        cli = self.GetConfigEntryWithDefault("CliConfig", "")
        if len(cli.strip()) <= 0:                
            ClientUtils.LogText("[SONM] CreateInstances - Please enter path to cli.")
            raise Exception("[SONM] Please enter path to cli.")

        try:
            for v in instanceIDs:
                response = check_output([cli, "deals", "status", v, "--node", node, "--out", "json"])
                task = json.loads(response)

                if not self.isDealDeadline(task):
                    ClientUtils.LogText("[SONM] StartInstances - Is not deadline task.")
                    continue

                if "running" not in task["info"]:
                    ClientUtils.LogText("[SONM] StartInstances - Cant find running sections.")
                    continue

                if "statuses" not in task["info"]["running"]:
                    #start task
                    ClientUtils.LogText("[SONM] StartInstances - Starting task.")
                    self.StartTask(v)
                    continue

                #find image in running
                if "statuses" in task["info"]["running"]:
                    for task_id in task["info"]["running"]["statuses"]:
                        if task["info"]["running"]["statuses"][task_id]["imageName"] == self.DEADLINE_IMG_NAME:
                            continue

                #deadline image has not been found, start task
                ClientUtils.LogText("[SONM] StartInstances - Starting task.")
                self.StartTask(v)

        except Exception as e:
            ClientUtils.LogText( traceback.format_exc() )

    #StopInstance -> StartInstance
    def RebootInstances( self, instanceIDs ):
        #TODO: Return list of boolean values indicating which instances
        #rebooted successfully.
        pass
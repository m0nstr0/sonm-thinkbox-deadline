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
        BID['price'] = self.GetConfigEntryWithDefault("Price", "")
        BID['order_type'] = 'BID'
        BID['slot'] = OrderedDict()
        BID['slot']['duration'] = self.GetConfigEntryWithDefault("Duration", "")
        BID['slot']['resources'] = OrderedDict()
        BID['slot']['resources']['cpu_cores'] = self.GetConfigEntryWithDefault("CpuCores", "")
        BID['slot']['resources']['ram_bytes'] = self.GetConfigEntryWithDefault("RamBytes", "")
        BID['slot']['resources']['gpu_count'] = self.GetConfigEntryWithDefault("GpuCount", "")
        BID['slot']['resources']['storage'] = self.GetConfigEntryWithDefault("Storage", "")
        BID['slot']['resources']['network'] = OrderedDict()
        BID['slot']['resources']['network']['in'] = self.GetConfigEntryWithDefault("NetworkIn", "")
        BID['slot']['resources']['network']['out'] = self.GetConfigEntryWithDefault("NetworkOut", "")
        BID['slot']['resources']['network']['type'] = self.GetConfigEntryWithDefault("NetworkType", "")
        BID['slot']['resources']['properties'] = OrderedDict()
        BID['slot']['resources']['properties']['sonm_deadline'] = '1'
        
        return self.Yaml(BID, 0)

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

    def GetActiveInstances( self ):

        ClientUtils.LogText( "SONM -- GetActiveInstances" )

        activeInstances = []
        
        node = self.GetConfigEntryWithDefault("NodeConfig", "127.0.0.1:9999")
        if len(node.strip()) <= 0:                
            ClientUtils.LogText("Please enter path to node.")
            return []

        cli = self.GetConfigEntryWithDefault("CliConfig", "")
        if len(cli.strip()) <= 0:                
            ClientUtils.LogText("Please enter path to cli.")
            return []

        try:
            response = check_output([cli, "market",  "processing", "--node", node, "--out", "json"])
            
            ClientUtils.LogText(response)
            
            all_tasks = json.loads(response)

            if "orders" not in all_tasks :
                ClientUtils.LogText("Tasks not found")
                return activeInstances

            task_list = []
            for k,v in all_tasks["orders"].iteritems():
                task_list.append(k);

            ClientUtils.LogText("Found " + str(len(task_list)) + " tasks")

            for v in task_list:
                response = check_output([cli, "market",  "show", v, "--node", node, "--out", "json"])
                ClientUtils.LogText(response)

                response = json.loads(response)

                if "error" in response :
                    ClientUtils.LogText(response['message'])
                    continue
                elif "id" not in response:
                    ClientUtils.LogText("Task ID not found")
                    continue
                elif "slot" not in response:
                    ClientUtils.LogText("Slot not found")
                    continue
                elif "properties" not in response["slot"]["resources"]:
                    ClientUtils.LogText("Properties not found")
                    continue
                elif "sonm_deadline" not in response["slot"]["resources"]["properties"]:
                    ClientUtils.LogText("Sonm_deadline not found")
                    continue


                ci = CloudInstance()
                ci.ID = v
                ci.Name = "SONM " + v
                ci.Hostname = "SONM"
                ci.Provider = "SONM"

                # statusNew
                if all_tasks["orders"][v]["status"] == 0 :
                    ci.Status = InstanceStatus.Pending

                # statusSearching
                if all_tasks["orders"][v]["status"] == 1 :
                    ci.Status = InstanceStatus.Pending

                # statusProposing
                if all_tasks["orders"][v]["status"] == 2 :
                    ci.Status = InstanceStatus.Pending

                # statusDealing
                if all_tasks["orders"][v]["status"] == 3 :
                    ci.Status = InstanceStatus.Pending

                # statusWaitForApprove
                if all_tasks["orders"][v]["status"] == 4 :
                    ci.Status = InstanceStatus.Pending

                # statusDone
                if all_tasks["orders"][v]["status"] == 5 :
                    ci.Status = InstanceStatus.Stopped

                # statusFailed
                if all_tasks["orders"][v]["status"] == 6 :
                    ci.Status = InstanceStatus.Terminated

                activeInstances.append(ci)

            return activeInstances

        except Exception as e:
            ClientUtils.LogText( traceback.format_exc() )
            return activeInstances

    def CreateInstances( self, hardwareID, imageID, count ):
        
        ClientUtils.LogText( "[SONM] CreateInstances" )

        startedInstances = []
        
        node = self.GetConfigEntryWithDefault("NodeConfig", "127.0.0.1:9999")
        if len(node.strip()) <= 0:                
            ClientUtils.LogText("[SONM] CreateInstances - Please enter path to node.")
            return []

        cli = self.GetConfigEntryWithDefault("CliConfig", "")
        if len(cli.strip()) <= 0:                
            ClientUtils.LogText("[SONM] CreateInstances - lease enter path to cli.")
            return []

        supplier = self.GetConfigEntryWithDefault("Supplier", "")

        #generate temp file for order.yaml
        temp_dir = os.path.abspath(tempfile.gettempdir())
        fname = 'tmp_' + str(time.time())
        postfix = 0
        while os.path.isfile(os.path.join(temp_dir, fname + str(postfix) + ".yml")):
            postfix = postfix + 1
        bid_yaml = os.path.join(temp_dir, fname + str(postfix) + ".yml")
        f = open(bid_yaml, 'w')
        f.write(self.GenerateBidYaml())
        f.close()

        for i in range(count):

            try:
                #if supplier is'n set
                if len(cli.strip()) <= 0:
                    response = check_output([cli, "market",  "create", str(bid_yaml), "--node", node, "--out", "json"])
                else:
                    response = check_output([cli, "market",  "create", str(bid_yaml), supplier, "--node", node, "--out", "json"])

                ClientUtils.LogText(response)

                response = json.loads(response)

                if "error" in response :
                    ClientUtils.LogText("[SONM] " + response['message'])
                    continue
                elif "id" not in response:
                    ClientUtils.LogText("[SONM] Task ID not found")
                    continue

                ClientUtils.LogText("[SONM] Task ID is " + response["id"])

                ci = CloudInstance()
                ci.ID = response["id"]
                ci.Name = "SONM " + response["id"]
                ci.Hostname = "SONM"
                ci.Provider = "SONM"
                ci.HardwareID = hardwareID
                ci.ImageID = imageID

                startedInstances.append(ci)
            except Exception as e:
                ClientUtils.LogText( traceback.format_exc() )

        ClientUtils.LogText("[SONM] " + len(startedInstances) " from " + str(count) + " instances has been created")
        return startedInstances

    def TerminateInstances( self, instanceIDs ):
        #TODO: Return list of boolean values indicating which instances
        #terminated successfully.
        #Must be implemented for the Balancer to work.
        pass

    def StopInstances( self, instanceIDs ):
        #TODO: Return list of boolean values indicating which instances
        #stopped successfully.
        pass

    def StartInstances( self, instanceIDs ):
        #TODO: Return list of boolean values indicating which instances
        #started successfully.
        pass

    def RebootInstances( self, instanceIDs ):
        #TODO: Return list of boolean values indicating which instances
        #rebooted successfully.
        pass
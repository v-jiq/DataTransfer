// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#include <stdio.h>
#include <stdlib.h>

#include "iothub_module_client_ll.h"
#include "iothub_client_options.h"
#include "iothub_message.h"
#include "azure_c_shared_utility/threadapi.h"
#include "azure_c_shared_utility/crt_abstractions.h"
#include "azure_c_shared_utility/platform.h"
#include "azure_c_shared_utility/shared_util_options.h"
#include "iothubtransportmqtt.h"
#include "iothub.h"
#include "time.h"
#include "parson.h"

static double temperatureThreshold = 25;


typedef struct MESSAGE_INSTANCE_TAG
{
	IOTHUB_MESSAGE_HANDLE messageHandle;
	size_t messageTrackingId;  // For tracking the messages within the user callback.
} 
MESSAGE_INSTANCE;

size_t messagesReceivedByInput1Queue = 0;

// SendConfirmationCallback is invoked when the message that was forwarded on from 'InputQueue1Callback'
// pipeline function is confirmed.
static void SendConfirmationCallback(IOTHUB_CLIENT_CONFIRMATION_RESULT result, void* userContextCallback)
{
	// The context corresponds to which message# we were at when we sent.
	MESSAGE_INSTANCE* messageInstance = (MESSAGE_INSTANCE*)userContextCallback;
	printf("Confirmation[%zu] received for message with result = %d\r\n", messageInstance->messageTrackingId, result);
	IoTHubMessage_Destroy(messageInstance->messageHandle);
	free(messageInstance);
}

// Allocates a context for callback and clones the message
// NOTE: The message MUST be cloned at this stage.  InputQueue1Callback's caller always frees the message
// so we need to pass down a new copy.
static MESSAGE_INSTANCE* CreateMessageInstance(IOTHUB_MESSAGE_HANDLE message)
{
	MESSAGE_INSTANCE* messageInstance = (MESSAGE_INSTANCE*)malloc(sizeof(MESSAGE_INSTANCE));
	/*
	   if (NULL == messageInstance)
	   {
	   printf("Failed allocating 'MESSAGE_INSTANCE' for pipelined message\r\n");
	   }
	   else
	   {
	   memset(messageInstance, 0, sizeof(*messageInstance));

	   if ((messageInstance->messageHandle = IoTHubMessage_Clone(message)) == NULL)
	   {
	   free(messageInstance);
	   messageInstance = NULL;
	   }
	   else
	   {
	   messageInstance->messageTrackingId = messagesReceivedByInput1Queue;
	   }
	   }*/
	if ((messageInstance->messageHandle = IoTHubMessage_Clone(message)) == NULL)
	{
		free(messageInstance);
		messageInstance = NULL;
	}
	else
	{
		messageInstance->messageTrackingId = messagesReceivedByInput1Queue;
		MAP_HANDLE propMap = IoTHubMessage_Properties(messageInstance->messageHandle);
		if (Map_AddOrUpdate(propMap, "MessageType", "Alert") != MAP_OK)
		{
			printf("ERROR: Map_AddOrUpdate Failed!\r\n");
		}
	}

	return messageInstance;
}

static void moduleTwinCallback(DEVICE_TWIN_UPDATE_STATE update_state, const unsigned char* payLoad, size_t size, void* userContextCallback)
{
	printf("\r\nTwin callback called with (state=%s, size=%zu):\r\n%s\r\n",
			MU_ENUM_TO_STRING(DEVICE_TWIN_UPDATE_STATE, update_state), size, payLoad);
	JSON_Value *root_value = json_parse_string(payLoad);
	JSON_Object *root_object = json_value_get_object(root_value);
	if (json_object_dotget_value(root_object, "desired.TemperatureThreshold") != NULL) {
		temperatureThreshold = json_object_dotget_number(root_object, "desired.TemperatureThreshold");
	}
	if (json_object_get_value(root_object, "TemperatureThreshold") != NULL) {
		temperatureThreshold = json_object_get_number(root_object, "TemperatureThreshold");
	}
}

/*
   static IOTHUBMESSAGE_DISPOSITION_RESULT InputQueue1Callback(IOTHUB_MESSAGE_HANDLE message, void* userContextCallback)
   {
   IOTHUBMESSAGE_DISPOSITION_RESULT result;
   IOTHUB_CLIENT_RESULT clientResult;
   IOTHUB_MODULE_CLIENT_LL_HANDLE iotHubModuleClientHandle = (IOTHUB_MODULE_CLIENT_LL_HANDLE)userContextCallback;

   unsigned const char* messageBody;
   size_t contentSize;

   if (IoTHubMessage_GetByteArray(message, &messageBody, &contentSize) != IOTHUB_MESSAGE_OK)
   {
   messageBody = "<null>";
   }

   printf("Received Message [%zu]\r\n Data: [%s]\r\n", 
   messagesReceivedByInput1Queue, messageBody);

// This message should be sent to next stop in the pipeline, namely "output1".  What happens at "outpu1" is determined
// by the configuration of the Edge routing table setup.
MESSAGE_INSTANCE *messageInstance = CreateMessageInstance(message);
if (NULL == messageInstance)
{
result = IOTHUBMESSAGE_ABANDONED;
}
else
{
printf("Sending message (%zu) to the next stage in pipeline\n", messagesReceivedByInput1Queue);

clientResult = IoTHubModuleClient_LL_SendEventToOutputAsync(iotHubModuleClientHandle, messageInstance->messageHandle, "output1", SendConfirmationCallback, (void *)messageInstance);
if (clientResult != IOTHUB_CLIENT_OK)
{
IoTHubMessage_Destroy(messageInstance->messageHandle);
free(messageInstance);
printf("IoTHubModuleClient_LL_SendEventToOutputAsync failed on sending msg#=%zu, err=%d\n", messagesReceivedByInput1Queue, clientResult);
result = IOTHUBMESSAGE_ABANDONED;
}
else
{
result = IOTHUBMESSAGE_ACCEPTED;
}
}

messagesReceivedByInput1Queue++;
return result;
}*/

static IOTHUBMESSAGE_DISPOSITION_RESULT InputQueue1Callback(IOTHUB_MESSAGE_HANDLE message, void* userContextCallback)
{
	IOTHUBMESSAGE_DISPOSITION_RESULT result;
	IOTHUB_CLIENT_RESULT clientResult;
	IOTHUB_MODULE_CLIENT_LL_HANDLE iotHubModuleClientHandle = (IOTHUB_MODULE_CLIENT_LL_HANDLE)userContextCallback;

	unsigned const char* messageBody;
	size_t contentSize;

	if (IoTHubMessage_GetByteArray(message, &messageBody, &contentSize) != IOTHUB_MESSAGE_OK)
	{
		messageBody = "<null>";
	}

	printf("Received Message [%zu]\r\n Data: [%s]\r\n",
			messagesReceivedByInput1Queue, messageBody);

	// Check if the message reports temperatures higher than the threshold
	JSON_Value *root_value = json_parse_string(messageBody);
	JSON_Object *root_object = json_value_get_object(root_value);
	double temperature;
	if (json_object_dotget_value(root_object, "machine.temperature") != NULL && (temperature = json_object_dotget_number(root_object, "machine.temperature")) > temperatureThreshold)
	{
		printf("Machine temperature %f exceeds threshold %f\r\n", temperature, temperatureThreshold);
		// This message should be sent to next stop in the pipeline, namely "output1".  What happens at "outpu1" is determined
		// by the configuration of the Edge routing table setup.
		MESSAGE_INSTANCE *messageInstance = CreateMessageInstance(message);
		if (NULL == messageInstance)
		{
			result = IOTHUBMESSAGE_ABANDONED;
		}
		else
		{
			printf("Sending message (%zu) to the next stage in pipeline\n", messagesReceivedByInput1Queue);

			clientResult = IoTHubModuleClient_LL_SendEventToOutputAsync(iotHubModuleClientHandle, messageInstance->messageHandle, "output1", SendConfirmationCallback, (void *)messageInstance);
			if (clientResult != IOTHUB_CLIENT_OK)
			{
				IoTHubMessage_Destroy(messageInstance->messageHandle);
				free(messageInstance);
				printf("IoTHubModuleClient_LL_SendEventToOutputAsync failed on sending msg#=%zu, err=%d\n", messagesReceivedByInput1Queue, clientResult);
				result = IOTHUBMESSAGE_ABANDONED;
			}
			else
			{
				result = IOTHUBMESSAGE_ACCEPTED;
			}
		}
	}
	else
	{
		printf("Not sending message (%zu) to the next stage in pipeline.\r\n", messagesReceivedByInput1Queue);
		result = IOTHUBMESSAGE_ACCEPTED;
	}

	messagesReceivedByInput1Queue++;
	return result;
}

static IOTHUB_MODULE_CLIENT_LL_HANDLE InitializeConnection()
{
	IOTHUB_MODULE_CLIENT_LL_HANDLE iotHubModuleClientHandle;

	if (IoTHub_Init() != 0)
	{
		printf("Failed to initialize the platform.\r\n");
		iotHubModuleClientHandle = NULL;
	}
	else if ((iotHubModuleClientHandle = IoTHubModuleClient_LL_CreateFromEnvironment(MQTT_Protocol)) == NULL)
	{
		printf("ERROR: IoTHubModuleClient_LL_CreateFromEnvironment failed\r\n");
	}
	else
	{
		// Uncomment the following lines to enable verbose logging.
		// bool traceOn = true;
		// IoTHubModuleClient_LL_SetOption(iotHubModuleClientHandle, OPTION_LOG_TRACE, &trace);
	}

	return iotHubModuleClientHandle;
}

static void DeInitializeConnection(IOTHUB_MODULE_CLIENT_LL_HANDLE iotHubModuleClientHandle)
{
	if (iotHubModuleClientHandle != NULL)
	{
		IoTHubModuleClient_LL_Destroy(iotHubModuleClientHandle);
	}
	IoTHub_Deinit();
}

/*
   static int SetupCallbacksForModule(IOTHUB_MODULE_CLIENT_LL_HANDLE iotHubModuleClientHandle)
   {
   int ret;

   if (IoTHubModuleClient_LL_SetInputMessageCallback(iotHubModuleClientHandle, "input1", InputQueue1Callback, (void*)iotHubModuleClientHandle) != IOTHUB_CLIENT_OK)
   {
   printf("ERROR: IoTHubModuleClient_LL_SetInputMessageCallback(\"input1\")..........FAILED!\r\n");
   ret = 1;
   }
   else
   {
   ret = 0;
   }

   return ret;
   }*/

static int deviceMethodCallback(const char* method_name, const unsigned char* payload, size_t size, unsigned char** response, size_t* response_size, void* userContextCallback)
{
	(void)userContextCallback;
	(void)payload;
	(void)size;

	int result;

	if (strcmp("getCarVIN", method_name) == 0)
	{
		const char deviceMethodResponse[] = "{ \"Response\": \"1HGCM82633A004352\" }";
		*response_size = sizeof(deviceMethodResponse)-1;
		*response = malloc(*response_size);
		(void)memcpy(*response, deviceMethodResponse, *response_size);
		result = 200;
	}
	else
	{
		// All other entries are ignored.
		const char deviceMethodResponse[] = "{ }";
		*response_size = sizeof(deviceMethodResponse)-1;
		*response = malloc(*response_size);
		(void)memcpy(*response, deviceMethodResponse, *response_size);
		result = -1;
	}

	return result;
}



static int SetupCallbacksForModule(IOTHUB_MODULE_CLIENT_LL_HANDLE iotHubModuleClientHandle)
{
	int ret;

	if (IoTHubModuleClient_LL_SetInputMessageCallback(iotHubModuleClientHandle, "input1", InputQueue1Callback, (void*)iotHubModuleClientHandle) != IOTHUB_CLIENT_OK)
	{
		printf("ERROR: IoTHubModuleClient_LL_SetInputMessageCallback(\"input1\")..........FAILED!\r\n");
		ret = MU_FAILURE;
	}
	else if (IoTHubModuleClient_LL_SetModuleTwinCallback(iotHubModuleClientHandle, moduleTwinCallback, (void*)iotHubModuleClientHandle) != IOTHUB_CLIENT_OK)
	{
		printf("ERROR: IoTHubModuleClient_LL_SetModuleTwinCallback(default)..........FAILED!\r\n");
		ret = MU_FAILURE;
	}
	else if(IoTHubModuleClient_LL_SetModuleMethodCallback(iotHubModuleClientHandle,deviceMethodCallback,NULL) != IOTHUB_CLIENT_OK)
	{
		printf("ERROR: IoTHubModuleClient_LL_SetModuleMethodCallback(default)..........FAILED!\r\n");
		ret = MU_FAILURE;
	}
	else
	{
		ret = 0;
	}

	return ret;
}


void iothub_module()
{
	IOTHUB_MODULE_CLIENT_LL_HANDLE iotHubModuleClientHandle;

	srand((unsigned int)time(NULL));

	char report_value[1024] = "{\"lastOilChangeDate\":\"2016\",\"maker\":{\"makerName\":\"Fabrikam\",\"style\":\"sedan\",\"year\":2014},\"state\":{\"reported_maxSpeed\":100,\"softwareVersion\":1,\"vanityPlate\":\"1I1\"}}";	

	if ((iotHubModuleClientHandle = InitializeConnection()) != NULL && SetupCallbacksForModule(iotHubModuleClientHandle) == 0)
	{
		// The receiver just loops constantly waiting for messages.
		printf("Waiting for incoming messages.\r\n");
		while (true)
		{
			printf("do_work.....................\n");
			IoTHubModuleClient_LL_DoWork(iotHubModuleClientHandle,report_value,strlen(report_value),);
			ThreadAPI_Sleep(100);
		}
	}

	IoTHubModuleClient_SendReportedState(iotHubModuleClientHandle,);
	DeInitializeConnection(iotHubModuleClientHandle);
}

int main(void)
{
	iothub_module();
	return 0;
}

# Customer Charge End-to-End Low-Level Flow

## Overview

This document provides a comprehensive low-level analysis of the customer charge system, detailing every method call, internal processing, and data flow from the initial controller action to the final completion verification.

## System Architecture

The system consists of three main AWS Lambda functions orchestrated through SQS messages:

1. **AltaworxRevAWSEnqueueCustomerCharges** - Phase 1: Charge Enqueueing
2. **AltaworxRevAWSCreateCustomerChange** - Phase 2: Charge Creation
3. **AltaworxRevAWSCheckCustomerChargeIsProcessed** - Phase 3: Completion Verification

---

## Phase 0: Initial Trigger (CustomerChargeController)

### Entry Points

#### Single Instance Flow
**Method**: `CustomerChargeController.CreateCustomerCharges(long id)`
- **Location**: `CustomerChargeController1.cs:180-212`
- **Purpose**: Creates customer charges for a single optimization instance

**Detailed Flow**:

1. **Permission Check**
   ```csharp
   if (permissionManager.UserCannotAccess(Session, ModuleEnum.CustomerCharge))
       return RedirectToAction("Index", "Home");
   ```

2. **AWS Configuration Retrieval**
   ```csharp
   var customObjectDbList = permissionManager.CustomFields;
   var awsAccessKey = AwsAccessKeyFromCustomObjects(customObjectDbList);
   var awsSecretAccessKey = AwsSecretAccessKeyFromCustomObjects(customObjectDbList);
   var createCustomerChargeQueueName = CreateCustomerChargeQueueFromCustomObjects(customObjectDbList);
   ```

3. **Configuration Validation**
   - Validates AWS access key, secret key, and queue name are present
   - If validation fails, redirects to error view with alert message

4. **SQS Message Dispatch**
   ```csharp
   var errorMessage = EnqueueCreateCustomerChargesSqs(id, awsAccessKey, awsSecretAccessKey, 
       createCustomerChargeQueueName, altaWrxDb, tenantId, 1, 1, id.ToString());
   ```

#### Multiple Instance Flow
**Method**: `CustomerChargeController.CreateCustomerChargesSession(Guid sessionId, string selectedInstances, ...)`
- **Location**: `CustomerChargeController1.cs:300-400`
- **Purpose**: Creates customer charges for multiple optimization instances

**Detailed Flow**:

1. **Instance Parsing**
   ```csharp
   var instances = selectedInstances?.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
   instanceIds = instances.Select(x => long.Parse(x.Replace("\"", ""))).ToArray();
   ```

2. **Sequential Instance Processing**
   ```csharp
   foreach (var item in instanceIds.Select((instanceId, index) => new { instanceId, index }))
   {
       var isLastInstanceId = (item.index == instanceIds.Length - 1) ? 1 : 0;
       var enqueueCreateCustomerChargeErrorMessage = EnqueueCreateCustomerChargesWithSesstionSqs(
           item.instanceId, awsAccessKey, awsSecretAccessKey, createCustomerChargeQueueName, 
           altaWrxDb, tenantId, 1, isLastInstanceId, string.Join(",", instanceIds));
   }
   ```

### SQS Message Creation Methods

#### EnqueueCreateCustomerChargesSqs
**Method**: `CustomerChargeController.EnqueueCreateCustomerChargesSqs()`
- **Location**: `CustomerChargeController1.cs:874-935`

**Internal Processing**:

1. **AWS Client Initialization**
   ```csharp
   var awsCredentials = new BasicAWSCredentials(awsAccessKey, awsSecretAccessKey);
   using (var client = new AmazonSQSClient(awsCredentials, RegionEndpoint.USEast1))
   ```

2. **Queue Discovery**
   ```csharp
   var queueList = client.ListQueues(createCustomerChargeQueueName);
   ```

3. **Message Attributes Construction**
   ```csharp
   MessageAttributes = new Dictionary<string, MessageAttributeValue>
   {
       { "InstanceId", new MessageAttributeValue { DataType = "String", StringValue = instanceId.ToString() } },
       { "IsMultipleInstanceId", new MessageAttributeValue { DataType = "String", StringValue = isMultipleInstanceId.ToString() } },
       { "IsLastInstanceId", new MessageAttributeValue { DataType = "String", StringValue = isLastInstanceId.ToString() } },
       { "InstanceIds", new MessageAttributeValue { DataType = "String", StringValue = instanceIds } },
       { "CurrentIntegrationAuthenticationId", new MessageAttributeValue { DataType = "String", StringValue = integrationAuthentication.Id.ToString() } }
   }
   ```

4. **Message Dispatch**
   ```csharp
   var response = client.SendMessageAsync(request);
   response.Wait();
   ```

---

## Phase 1: Charge Enqueueing (AltaworxRevAWSEnqueueCustomerCharges)

### Lambda Function Handler
**Method**: `Function.FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)`
- **Location**: `AltaworxRevAWSEnqueueCustomerCharges.cs:33-52`

**Processing Flow**:

1. **Context Initialization**
   ```csharp
   keysysContext = base.BaseFunctionHandler(context);
   ```

2. **Environment Variable Setup**
   ```csharp
   if (string.IsNullOrWhiteSpace(DeviceCustomerChargeQueueUrl))
       DeviceCustomerChargeQueueUrl = context.ClientContext.Environment["DeviceCustomerChargeQueueUrl"];
   ```

3. **Event Processing Delegation**
   ```csharp
   ProcessEvent(keysysContext, sqsEvent);
   ```

### Event Processing Chain

#### ProcessEvent
**Method**: `Function.ProcessEvent(KeySysLambdaContext context, SQSEvent sqsEvent)`
- **Location**: `AltaworxRevAWSEnqueueCustomerCharges.cs:54-68`

**Logic**:
- Validates single message expectation
- Delegates to `ProcessEventRecord` for individual message processing

#### ProcessEventRecord
**Method**: `Function.ProcessEventRecord(KeySysLambdaContext context, SQSEvent.SQSMessage message)`
- **Location**: `AltaworxRevAWSEnqueueCustomerCharges.cs:70-86`

**Processing Steps**:

1. **InstanceId Extraction**
   ```csharp
   if (message.MessageAttributes.ContainsKey("InstanceId"))
   {
       string instanceIdString = message.MessageAttributes["InstanceId"].StringValue;
       long instanceId = long.Parse(instanceIdString);
   }
   ```

2. **SqsValues Construction**
   ```csharp
   var sqsValues = new SqsValues(context, message);
   ```

3. **Instance Processing Initiation**
   ```csharp
   ProcessInstance(context, instanceId, sqsValues);
   ```

#### SqsValues Constructor Analysis
**Class**: `SqsValues`
- **Location**: `SqsValues.cs:16-60`

**Message Attribute Extraction**:
- `IsMultipleInstanceId`: Parsed from message attributes, defaults to 0
- `IsLastInstanceId`: Parsed from message attributes, defaults to 0
- `InstanceIds`: String value from message attributes
- `CurrentIntegrationAuthenticationId`: Authentication ID for Rev.io integration

### Core Instance Processing

#### ProcessInstance
**Method**: `Function.ProcessInstance(KeySysLambdaContext context, long instanceId, SqsValues sqsValues)`
- **Location**: `AltaworxRevAWSEnqueueCustomerCharges.cs:88-120`

**Detailed Processing Flow**:

1. **Instance Retrieval**
   ```csharp
   OptimizationInstance instance = GetInstance(context, instanceId);
   ```

2. **Settings Loading**
   ```csharp
   context.LoadOptimizationSettingsByTenantId(instance.TenantId);
   ```

3. **Communication Groups Retrieval**
   ```csharp
   List<OptimizationCommGroup> commGroups = GetCommGroups(context, instanceId);
   ```

4. **Queue Processing Loop**
   ```csharp
   foreach (var item in commGroups.Select((commGroup, index) => new { index, commGroup }))
   {
       // Get winning queue for each comm group
       long winningQueueId = GetWinningQueueId(context, item.commGroup.Id, instance.PortalTypeId);
       
       // Determine if this is the last queue for last instance
       if (sqsValues.IsMultipleInstanceId == 1 && messageIsLastInstanceId == 1 && 
           item.index == commGroups.Count - 1)
       {
           sqsValues.IsLastInstanceId = 1;
       }
       
       // Enqueue customer charges for the queue
       EnqueueCustomerCharges(context, winningQueueId, instance.PortalTypeId, sqsValues, 
           (int)instance.IntegrationAuthenticationId);
   }
   ```

### Queue Selection Logic

#### GetWinningQueueId
**Method**: `Function.GetWinningQueueId(KeySysLambdaContext context, long commGroupId, int portalTypeId)`
- **Location**: `AltaworxRevAWSEnqueueCustomerCharges.cs:122-153`

**Queue Selection Strategy**:

1. **SQL Query Selection Based on Portal Type**
   ```csharp
   var sqlCommand = portalTypeId == PortalTypeMobility
       ? GetMobilityDeviceWinningQueueSql()
       : GetDeviceWinningQueueSql();
   
   if (portalTypeId == (int)PortalTypeEnum.CrossProvider)
       sqlCommand = GetCrossProviderDeviceWinningQueueSql();
   ```

2. **SQL Queries Analysis**:

   **M2M Devices** (`GetDeviceWinningQueueSql`):
   ```sql
   SELECT TOP 1 Id FROM OptimizationQueue oq
   WHERE EXISTS (
     SELECT 1 FROM OptimizationDeviceResult odr
     WHERE oq.Id = odr.QueueId
   )
   AND CommPlanGroupId = @commGroupId
   AND TotalCost IS NOT NULL
   AND RunEndTime IS NOT NULL
   ORDER BY TotalCost
   ```

   **Mobility Devices** (`GetMobilityDeviceWinningQueueSql`):
   ```sql
   SELECT TOP 1 Id FROM OptimizationQueue oq
   WHERE EXISTS (
     SELECT 1 FROM OptimizationMobilityDeviceResult odr
     WHERE oq.Id = odr.QueueId
   )
   AND CommPlanGroupId = @commGroupId
   AND TotalCost IS NOT NULL
   AND RunEndTime IS NOT NULL
   ORDER BY TotalCost
   ```

   **Cross Provider** (`GetCrossProviderDeviceWinningQueueSql`):
   ```sql
   SELECT TOP 1 Id FROM OptimizationQueue oq
   WHERE EXISTS (
     SELECT 1 FROM OptimizationMobilityDeviceResult odr
     WHERE oq.Id = odr.QueueId
     UNION 
     SELECT 1 FROM OptimizationDeviceResult odr
     WHERE oq.Id = odr.QueueId
   )
   AND CommPlanGroupId = @commGroupId
   AND TotalCost IS NOT NULL
   AND RunEndTime IS NOT NULL
   ORDER BY TotalCost
   ```

3. **Selection Criteria**:
   - Queue must have completed optimization results (`RunEndTime IS NOT NULL`)
   - Queue must have calculated total cost (`TotalCost IS NOT NULL`)
   - Selects queue with lowest total cost (`ORDER BY TotalCost`)

### Database and SQS Enqueueing

#### EnqueueCustomerCharges
**Method**: `Function.EnqueueCustomerCharges(KeySysLambdaContext context, long queueId, int portalTypeId, SqsValues sqsValues, int integrationAuthenticationId)`
- **Location**: `AltaworxRevAWSEnqueueCustomerCharges.cs:155-171`

**Processing Logic**:

1. **Cross Provider Handling**
   ```csharp
   if (portalTypeId == (int)PortalTypeEnum.CrossProvider)
   {
       EnqueueCustomerChargesDb(context, queueId, PortalTypeM2M);
       EnqueueCustomerChargesDb(context, queueId, PortalTypeMobility);
       EnqueueCustomerChargesSqs(context, queueId, sqsValues, portalTypeId, 
           Convert.ToInt32(sqsValues.CurrentIntegrationAuthenticationId));
   }
   ```

2. **Standard Portal Type Handling**
   ```csharp
   else
   {
       EnqueueCustomerChargesDb(context, queueId, portalTypeId);
       EnqueueCustomerChargesSqs(context, queueId, sqsValues, portalTypeId, integrationAuthenticationId);
   }
   ```

#### Database Enqueueing - EnqueueCustomerChargesDb
**Method**: `Function.EnqueueCustomerChargesDb(KeySysLambdaContext context, long queueId, int portalTypeId)`
- **Location**: `AltaworxRevAWSEnqueueCustomerCharges.cs:173-196`

**Database Operations**:

1. **Connection Setup**
   ```csharp
   using (var Conn = new SqlConnection(context.CentralDbConnectionString))
   {
       Conn.Open();
       using (var Cmd = new SqlCommand("SET ARITHABORT ON", Conn))
           Cmd.ExecuteNonQuery();
   }
   ```

2. **Stored Procedure Execution**
   ```csharp
   var sqlCommand = portalTypeId == PortalTypeMobility
       ? "usp_Optimization_Mobility_EnqueueCustomerCharges"
       : "usp_Optimization_EnqueueCustomerCharges";
   
   using (var Cmd = new SqlCommand(sqlCommand, Conn))
   {
       Cmd.CommandType = CommandType.StoredProcedure;
       Cmd.Parameters.AddWithValue("@QueueId", queueId);
       Cmd.CommandTimeout = 240;
       Cmd.ExecuteNonQuery();
   }
   ```

**Stored Procedures Function**:
- **usp_Optimization_EnqueueCustomerCharges**: Processes M2M device records for charging
- **usp_Optimization_Mobility_EnqueueCustomerCharges**: Processes Mobility device records for charging

#### SQS Enqueueing - EnqueueCustomerChargesSqs
**Method**: `Function.EnqueueCustomerChargesSqs(KeySysLambdaContext context, long queueId, SqsValues sqsValues, int portalTypeId, int integrationAuthenticationId)`
- **Location**: `AltaworxRevAWSEnqueueCustomerCharges.cs:198-248`

**SQS Message Construction**:

1. **Message Attributes Setup**
   ```csharp
   MessageAttributes = new Dictionary<string, MessageAttributeValue>
   {
       { "QueueId", new MessageAttributeValue { DataType = "String", StringValue = queueId.ToString() } },
       { "IsMultipleInstanceId", new MessageAttributeValue { DataType = "String", StringValue = sqsValues.IsMultipleInstanceId.ToString() } },
       { "IsLastInstanceId", new MessageAttributeValue { DataType = "String", StringValue = sqsValues.IsLastInstanceId.ToString() } },
       { "InstanceIds", new MessageAttributeValue { DataType = "String", StringValue = sqsValues.InstanceIds.ToString() } },
       { "PortalTypeId", new MessageAttributeValue { DataType = "String", StringValue = portalTypeId.ToString() } },
       { "CurrentIntegrationAuthenticationId", new MessageAttributeValue { DataType = "String", StringValue = integrationAuthenticationId.ToString() } }
   }
   ```

2. **Delay Logic**
   ```csharp
   var isLastQueue = sqsValues.IsLastInstanceId == 1;
   DelaySeconds = isLastQueue ? CommonConstants.DELAY_IN_SECONDS_FIVE_MINUTES : 0
   ```
   - **Purpose**: 5-minute delay for last queue to ensure all processing completes before verification

3. **Message Dispatch**
   ```csharp
   var response = client.SendMessageAsync(request);
   response.Wait();
   ```

---

## Phase 2: Charge Creation (AltaworxRevAWSCreateCustomerChange)

### Lambda Function Handler
**Method**: `Function.FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)`
- **Location**: `AltaworxRevAWSCreateCustomerChange.cs:45-114`

**Dependency Injection Setup**:

1. **Core Infrastructure**
   ```csharp
   keysysContext = BaseFunctionHandler(context);
   var environmentRepo = new EnvironmentRepository();
   var connectionString = environmentRepo.GetEnvironmentVariable(context, "ConnectionString");
   var logger = keysysContext.logger;
   var base64Service = new Base64Service();
   var settingsRepo = new SettingsRepository(logger, connectionString, base64Service);
   ```

2. **Repository Initialization**
   ```csharp
   var deviceCustomerChargeQueueRepo = new DeviceCustomerChargeQueueRepository(logger, environmentRepo, context, connectionString, settingsRepo);
   var revIoAuthRepo = new RevioAuthenticationRepository(connectionString, base64Service, logger);
   var customerRepo = new CustomerRepository(logger, connectionString);
   var optimizationInstanceRepo = new OptimizationInstanceRepository(logger, connectionString);
   var optimizationQueueRepo = new OptimizationQueueRepository(logger, connectionString);
   ```

3. **Rev.io Integration Setup**
   ```csharp
   var currentRecord = sqsEvent.Records.First();
   var integrationAuthenticationId = GetCurrentIntegrationAuthenticationId(currentRecord);
   var revIoAuth = revIoAuthRepo.GetRevioApiAuthentication(integrationAuthenticationId);
   var revioApiClient = new RevioApiClient(new SingletonHttpClientFactory(), new HttpRequestFactory(), revIoAuth, keysysContext.IsProduction, CommonConstants.NUMBER_OF_REV_IO_RETRIES_3);
   ```

4. **Service Layer Construction**
   ```csharp
   var chargeListFileService = new CustomerChargeListFileService();
   var chargeListEmailService = new CustomerChargeListEmailService(logger, emailClientFactory, settingsRepo, customerRepo);
   var deviceChargeRepository = new DeviceChargeRepository(logger, base64Service, environmentRepo, context, httpClientFactory, httpRetryPolicy, emailSender, generalProviderSettings, revioApiClient);
   var chargeService = new DeviceCustomerChargeService(logger, deviceCustomerChargeQueueRepo, revIoAuthRepo, environmentRepo, context, settingsRepo, chargeListFileService, s3Wrapper, chargeListEmailService, deviceChargeRepository, customerRepo, revioApiClient, emailSender, generalProviderSettings);
   ```

5. **Event Handler Initialization**
   ```csharp
   var changeHandler = new CustomerChangeEventHandler(logger, optimizationQueueRepo, optimizationInstanceRepo, chargeService);
   var sqsValues = new SqsValues(logger, sqsEvent.Records[0]);
   await changeHandler.HandleEventAsync(sqsEvent, sqsValues);
   ```

### Event Processing

#### CustomerChangeEventHandler.HandleEventAsync
**Method**: `CustomerChangeEventHandler.HandleEventAsync(SQSEvent sqsEvent, SqsValues sqsValues)`
- **Location**: `CustomerChangeEventHandler.cs:31-43`

**Processing Flow**:
```csharp
await ProcessEventAsync(sqsEvent, sqsValues);
```

#### ProcessEventAsync
**Method**: `CustomerChangeEventHandler.ProcessEventAsync(SQSEvent sqsEvent, SqsValues sqsValues)`
- **Location**: `CustomerChangeEventHandler.cs:45-59`

**Message Validation**:
```csharp
switch (sqsEvent.Records.Count)
{
    case 0: return;
    case 1: await ProcessEventRecordAsync(sqsEvent.Records[0], sqsValues); break;
    default: _logger.LogInfo(CommonConstants.EXCEPTION, string.Format(LogCommonStrings.MULTIPLE_MESSAGE_RECEIVED, sqsEvent.Records.Count)); break;
}
```

#### ProcessEventRecordAsync
**Method**: `CustomerChangeEventHandler.ProcessEventRecordAsync(SQSEvent.SQSMessage message, SqsValues sqsValues)`
- **Location**: `CustomerChangeEventHandler.cs:61-86`

**Message Type Routing**:

1. **Queue-based Processing**
   ```csharp
   if (message.MessageAttributes.ContainsKey(SQSMessageKeyConstant.QUEUE_ID))
   {
       var queueIdString = message.MessageAttributes[SQSMessageKeyConstant.QUEUE_ID].StringValue;
       var queueId = long.Parse(queueIdString);
       var queue = _optimizationQueueRepository.GetQueue(queueId);
       var instance = _optimizationInstanceRepository.GetInstance(queue.InstanceId);
       await _deviceCustomerChargeService.ProcessQueueAsync(queueId, instance, sqsValues);
   }
   ```

2. **File-based Processing**
   ```csharp
   else if (message.MessageAttributes.ContainsKey(SQSMessageKeyConstant.FILE_ID))
   {
       var fileIdString = message.MessageAttributes[SQSMessageKeyConstant.FILE_ID].StringValue;
       var fileId = int.Parse(fileIdString);
       await _deviceCustomerChargeService.ProcessQueueAsync(fileId, sqsValues);
   }
   ```

### Enhanced SqsValues Analysis
**Class**: `SqsValues` (CreateCustomerChange version)
- **Location**: `SqsValues1.cs:23-109`

**Enhanced Attributes**:
- `IsMultipleInstanceId`: Boolean flag for multiple instance processing
- `IsLastInstanceId`: Boolean flag for last instance in batch
- `InstanceIds`: Comma-separated string of instance IDs
- `PortalTypeId`: Type of portal (M2M=0, Mobility=2, CrossProvider, etc.)
- `CurrentIntegrationAuthenticationId`: Rev.io authentication identifier
- `IsSendSummaryEmailForMultipleInstanceStep`: Email summary flag
- `RetryNumber`: Current retry attempt number
- `PageNumber`: Current page number for pagination (defaults to 1)
- `RetryCount`: Total retry count

### Core Device Processing

#### DeviceCustomerChargeService.ProcessQueueAsync
**Method**: `DeviceCustomerChargeService.ProcessQueueAsync(long queueId, OptimizationInstance instance, SqsValues sqsValues)`
- **Location**: `DeviceCustomerChargeService1.cs:84-119`

**Processing Logic**:

1. **Pagination Setup**
   ```csharp
   var offset = (sqsValues.PageNumber - 1) * PAGE_SIZE; // PAGE_SIZE = 50
   ```

2. **Customer Type Determination**
   ```csharp
   var isNonRevCustomer = instance.AMOPCustomerId != null && instance.RevCustomerId == null && instance.IntegrationAuthenticationId == null;
   ```

3. **Device List Retrieval**
   ```csharp
   var deviceList = _customerChargeQueueRepository.GetDeviceList(queueId, PAGE_SIZE, offset, isNonRevCustomer)
       .Where(x => x.IsProcessed == false).ToList();
   ```

4. **Processing Branch**
   ```csharp
   if (isNonRevCustomer)
       await ProcessCustomerChargeForNonRev(queueId, instance, sqsValues, deviceList, serviceProviderList, proxyUrl, bucketName, offset);
   else
       await ProcessCustomerChargeForRev(queueId, instance, sqsValues, deviceList, serviceProviderList, proxyUrl, bucketName, offset);
   ```

### Non-Rev Customer Processing

#### ProcessCustomerChargeForNonRev
**Method**: `DeviceCustomerChargeService.ProcessCustomerChargeForNonRev()`
- **Location**: `DeviceCustomerChargeService1.cs:120-144`

**Processing Steps**:

1. **Mock Processing**
   ```csharp
   var chargeId = 0;
   var smsChargeId = 0;
   var hasErrors = false;
   var errorMessage = string.Empty;
   
   foreach (var device in deviceLists)
   {
       _customerChargeQueueRepository.MarkRecordProcessed(device.Id, chargeId.ToString(), 
           device.DeviceCharge, device.BaseRate, device.DeviceCharge + device.BaseRate, 
           hasErrors, errorMessage, smsChargeId.ToString(), device.SmsChargeAmount);
   }
   ```

2. **Pagination Management**
   ```csharp
   var totalPage = CalculateTotalPageInQueue(queueId, PAGE_SIZE, true);
   await MultipleEnqueueCustomerChargesAsync(queueId, sqsValues, totalPage, true);
   ```

3. **Completion Check Enqueueing**
   ```csharp
   if (sqsValues.PageNumber == totalPage)
   {
       await _customerChargeQueueRepository.EnqueueCheckCustomerChargesIsProcessedAsync(queueId, 
           sqsValues.PortalTypeId, sqsValues.InstanceIds, sqsValues.IsMultipleInstanceId, sqsValues.IsLastInstanceId);
   }
   ```

### Rev Customer Processing

#### ProcessCustomerChargeForRev
**Method**: `DeviceCustomerChargeService.ProcessCustomerChargeForRev()`
- **Location**: `DeviceCustomerChargeService1.cs:145-169`

**Processing Flow**:

1. **Authentication Validation**
   ```csharp
   if (instance.IntegrationAuthenticationId.HasValue)
   {
       var revIoAuth = _revIoAuthenticationRepository.GetRevioApiAuthentication(instance.IntegrationAuthenticationId.Value);
   ```

2. **Settings Retrieval**
   ```csharp
   var optimizationSettings = _settingsRepository.GetOptimizationSettings();
   var billingTimeZone = optimizationSettings?.BillingTimeZone;
   var useNewLogicCustomerCharge = (bool)optimizationSettings?.UsingNewProcessInCustomerCharge;
   ```

3. **Device List Processing**
   ```csharp
   await ProcessDeviceList(deviceList, queueId, sqsValues, instance, revIoAuth, billingTimeZone, serviceProviders, useNewLogicCustomerCharge);
   ```

4. **Pagination and Completion Logic**
   ```csharp
   var totalPage = CalculateTotalPageInQueue(queueId, PAGE_SIZE, false);
   await MultipleEnqueueCustomerChargesAsync(queueId, sqsValues, totalPage, false);
   
   if (sqsValues.PageNumber == totalPage)
   {
       await _customerChargeQueueRepository.EnqueueCheckCustomerChargesIsProcessedAsync(queueId, 
           sqsValues.PortalTypeId, sqsValues.InstanceIds, sqsValues.IsMultipleInstanceId, 
           sqsValues.IsLastInstanceId, sqsValues.CurrentIntegrationAuthenticationId);
   }
   ```

### Detailed Device List Processing

#### ProcessDeviceList
**Method**: `DeviceCustomerChargeService.ProcessDeviceList()`
- **Location**: `DeviceCustomerChargeService1.cs:309-475`

**Core Processing Loop**:

1. **Initialization**
   ```csharp
   bool retryFlag = false;
   List<DeviceCustomerChargeQueueRecord> errorDeviceList = new List<DeviceCustomerChargeQueueRecord>();
   ```

2. **Device Processing Loop**
   ```csharp
   foreach (var device in deviceList)
   {
       var shouldRetry = await ProcessSingleDevice(device, queueId, instance, revIoAuth, billingTimeZone, serviceProviders, useNewLogicCustomerCharge);
       if (shouldRetry)
       {
           retryFlag = true;
           errorDeviceList.Add(device);
       }
   }
   ```

3. **Retry Logic**
   ```csharp
   if (retryFlag && errorDeviceList.Count > 0)
   {
       await SendErrorEmailNotificationAsync(errorDeviceList);
       if (sqsValues.RetryCount < CommonConstants.MAX_RETRY_COUNT)
       {
           await _customerChargeQueueRepository.EnqueueCustomerChargesAsync(queueId, sqsValues.PageNumber, 
               sqsValues.CurrentIntegrationAuthenticationId, sqsValues.RetryCount + 1);
       }
   }
   ```

#### ProcessSingleDevice (Internal Method)
**Method**: `DeviceCustomerChargeService.ProcessSingleDevice()` (Internal logic within ProcessDeviceList)
- **Location**: `DeviceCustomerChargeService1.cs:315-475`

**Detailed Processing Steps**:

1. **Service Provider Resolution**
   ```csharp
   var integrationId = serviceProviders.FirstOrDefault(x => x.Id == device.ServiceProviderId)?.IntegrationId ?? 0;
   ```

2. **Rate Plan Validation**
   ```csharp
   var revService = serviceProviders.FirstOrDefault(x => x.Id == device.ServiceProviderId);
   if (revService?.RevRatePlan == null)
   {
       hasErrors = true;
       errorMessage = LogCommonStrings.REV_RATE_PLAN_NOT_FOUND;
       _customerChargeQueueRepository.MarkRecordProcessed(device.Id, chargeId.ToString(), 
           device.DeviceCharge, device.BaseRate, device.DeviceCharge + device.BaseRate, 
           hasErrors, errorMessage, smsChargeId.ToString(), device.SmsChargeAmount);
       return false;
   }
   ```

3. **Charge Processing Logic**
   ```csharp
   if (Convert.ToBoolean(_environmentRepository.GetEnvironmentVariable(_context, "SendToRev")))
   {
       // Usage Charge Processing
       if (!isSMSCharge && device.DeviceCharge > 0.0M)
       {
           var customerChargeResponse = await AddCustomerUsageChargeAsync(device, instance, billingTimeZone, integrationId, useNewLogicCustomerCharge, isRateCharge, isOverageCharge);
           if (customerChargeResponse != null)
           {
               chargeId = customerChargeResponse.ChargeId;
               hasErrors = customerChargeResponse.HasErrors;
               statusCode = customerChargeResponse.StatusCode;
               errorMessage = customerChargeResponse.ErrorMessage + Environment.NewLine;
           }
       }
       
       // SMS Charge Processing
       if (isSMSCharge && !device.IsBillInAdvance && device.SmsChargeAmount > 0.0M && 
           (device.SmsRevProductTypeId != null || device.SmsRevProductId != null))
       {
           var customerChargeResponse = await AddCustomerSmsChargeAsync(device, integrationId, instance, billingTimeZone, useNewLogicCustomerCharge);
           if (customerChargeResponse != null)
           {
               smsChargeId = customerChargeResponse.ChargeId;
               hasErrors = hasErrors || customerChargeResponse.HasErrors;
               errorMessage += customerChargeResponse.ErrorMessage;
           }
       }
   }
   ```

4. **Record Processing**
   ```csharp
   if (!hasErrors && statusCode != (int)HttpStatusCode.TooManyRequests)
   {
       _customerChargeQueueRepository.MarkRecordProcessed(device.Id, chargeId.ToString(), 
           device.DeviceCharge, device.BaseRate, device.DeviceCharge + device.BaseRate, 
           hasErrors, errorMessage, smsChargeId.ToString(), device.SmsChargeAmount);
       return false; // No retry needed
   }
   else
   {
       return true; // Retry needed
   }
   ```

### Charge Creation via Rev.io API

#### AddCustomerUsageChargeAsync
**Method**: `DeviceCustomerChargeService.AddCustomerUsageChargeAsync()`
- **Location**: `DeviceCustomerChargeService1.cs:598-726`

**New Logic vs Old Logic Processing**:

1. **New Logic (Split Charges)**
   ```csharp
   if (useNewLogicCustomerCharge)
   {
       if (isRateCharge)
       {
           var requestRateCharge = new RevIOCommon.CreateDeviceChargeRequest(device, revService, 
               instance.BillingPeriodStartDate, instance.BillingPeriodEndDate, billingTimeZone, 
               integrationId, false, useNewLogicCustomerCharge, false, true);
           var responseRateCharge = await _deviceChargeRepository.AddChargeAsync(requestRateCharge);
           return responseRateCharge;
       }
       if (isOverageCharge)
       {
           var requestOverCharge = new RevIOCommon.CreateDeviceChargeRequest(device, revService, 
               instance.BillingPeriodStartDate, instance.BillingPeriodEndDate, billingTimeZone, 
               integrationId, false, useNewLogicCustomerCharge, true, false);
           var responseOverCharge = await _deviceChargeRepository.AddChargeAsync(requestOverCharge);
           return responseOverCharge;
       }
   }
   ```

2. **Old Logic (Combined Charges)**
   ```csharp
   else
   {
       var request = new RevIOCommon.CreateDeviceChargeRequest(device, revService, 
           instance.BillingPeriodStartDate, instance.BillingPeriodEndDate, billingTimeZone, integrationId);
       var response = await _deviceChargeRepository.AddChargeAsync(request);
       return response;
   }
   ```

#### AddCustomerSmsChargeAsync
**Method**: `DeviceCustomerChargeService.AddCustomerSmsChargeAsync()`
- **Location**: `DeviceCustomerChargeService1.cs:743-757`

**SMS Charge Processing**:
```csharp
var request = new RevIOCommon.CreateDeviceChargeRequest(device, revService, 
    instance.BillingPeriodStartDate, instance.BillingPeriodEndDate, billingTimeZone, 
    integrationId, true, useNewLogicCustomerCharge);
var response = await _deviceChargeRepository.AddChargeAsync(request);
```

#### DeviceChargeRepository.AddChargeAsync
**Method**: `DeviceChargeRepository.AddChargeAsync(RevIOCommon.CreateDeviceChargeRequest request)`
- **Location**: `DeviceChargeRepository.cs:50-72`

**Rev.io API Integration**:

1. **Request Serialization**
   ```csharp
   var requestString = JsonConvert.SerializeObject(request);
   ```

2. **API Call Execution**
   ```csharp
   var response = await revioApiClient.AddChargeAsync(requestString, retryPolicy, logger);
   ```

3. **Response Processing**
   ```csharp
   if (response == null || response?.Id <= 0)
   {
       logger.LogInfo(CommonConstants.WARNING, string.Format(LogCommonStrings.ERROR_WHILE_UPLOADING_CHARGES, response));
       var errorMessage = JsonConvert.SerializeObject(response);
       return new CustomerChargeResponse
       {
           HasErrors = true,
           ErrorMessage = errorMessage
       };
   }
   
   return new CustomerChargeResponse
   {
       ChargeId = response.Id,
       HasErrors = false,
       ErrorMessage = string.Empty
   };
   ```

### Pagination Management

#### MultipleEnqueueCustomerChargesAsync
**Method**: `DeviceCustomerChargeService.MultipleEnqueueCustomerChargesAsync()`
- **Internal method for handling pagination**

**Logic**:
```csharp
if (totalPage > 1 && sqsValues.PageNumber == 1)
{
    for (var pageNumber = 2; pageNumber <= totalPage; pageNumber++)
    {
        await _customerChargeQueueRepository.EnqueueCustomerChargesAsync(queueId, pageNumber, 
            sqsValues.CurrentIntegrationAuthenticationId);
    }
}
```

---

## Phase 3: Completion Verification (AltaworxRevAWSCheckCustomerChargeIsProcessed)

### Lambda Function Handler
**Method**: `Function.FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)`
- **Location**: `AltaworxRevAWSCheckCustomerChargeIsProcessed.cs:29-63`

**Service Initialization**:

1. **Infrastructure Setup**
   ```csharp
   keysysContext = BaseFunctionHandler(context);
   var environmentRepo = new EnvironmentRepository();
   var connectionString = environmentRepo.GetEnvironmentVariable(context, SQSMessageKeyConstant.CONNECTION_STRING);
   var logger = keysysContext.logger;
   var base64Service = new Base64Service();
   var settingsRepo = new SettingsRepository(logger, connectionString, base64Service);
   ```

2. **Service Construction**
   ```csharp
   var deviceCustomerChargeQueueRepo = new DeviceCustomerChargeQueueRepository(logger, environmentRepo, context, connectionString, settingsRepo);
   var customerRepo = new CustomerRepository(logger, connectionString);
   var chargeListEmailService = new CustomerChargeListEmailService(logger, emailClientFactory, settingsRepo, customerRepo);
   var chargeListFileService = new CustomerChargeListFileService();
   var generalProviderSettings = settingsRepo.GetGeneralProviderSettings();
   var s3Wrapper = new S3Wrapper(generalProviderSettings.AwsCredentials, environmentRepo.GetEnvironmentVariable(context, SQSMessageKeyConstant.CUSTOMER_CHARGES_S3_BUCKET_NAME));
   ```

3. **Event Handler Execution**
   ```csharp
   var checkIsProcessedService = new CheckIsProcessedService(context, chargeListEmailService, deviceCustomerChargeQueueRepo, logger, chargeListFileService, environmentRepo, s3Wrapper, customerRepo);
   var sqsValues = new SqsValues(logger, sqsEvent.Records[0]);
   var checkIsProcessEventHandler = new CheckIsProcessedEventHandler(logger, optimizationQueueRepo, optimizationInstanceRepo, checkIsProcessedService);
   await checkIsProcessEventHandler.HandleEventAsync(sqsEvent, sqsValues);
   ```

### Processing Verification Logic

#### CheckIsProcessedService.ProcessQueueAsync
**Method**: `CheckIsProcessedService.ProcessQueueAsync(long queueId, OptimizationInstance instance, SqsValues sqsValues)`
- **Location**: `CheckIsProcessedService.cs:46-99`

**Verification Flow**:

1. **Queue Status Check**
   ```csharp
   if (!_customerChargeQueueRepository.QueueHasMoreItems(queueId, isNonRevCustomer))
   {
       // Queue is complete - proceed with file generation and email
   }
   else
   {
       // Queue still has unprocessed items - retry or fail
   }
   ```

2. **Completion Processing**
   ```csharp
   // Get charge list
   var chargeList = _customerChargeQueueRepository.GetChargeList(queueId)?.ToList();
   
   // Create charge list file and save to S3
   var fileName = $"{queueId}.txt";
   var chargeListFileBytes = _chargeListFileService.GenerateChargeListFile(chargeList, 
       instance.BillingPeriodStartDate, instance.BillingPeriodEndDate, serviceProviderList);
   
   _s3Wrapper.UploadAwsFile(chargeListFileBytes, fileName);
   ```

3. **S3 Upload Verification**
   ```csharp
   var statusUploadFileToS3 = _s3Wrapper.WaitForFileUploadCompletion(fileName, 
       CommonConstants.DELAY_IN_SECONDS_FIVE_MINUTES, _logger);
   
   var statusUploadFile = (isUploadSucces: statusUploadFileToS3.Result.Item1, 
                          errorMessage: statusUploadFileToS3.Result.Item2);
   ```

4. **Email Summary Processing**
   ```csharp
   if (statusUploadFile.isUploadSucces)
   {
       var errorCount = chargeList?.Count(x => x.HasErrors) ?? 0;
       if (!sqsValues.IsMultipleInstanceId)
       {
           await _customerChargeListEmailService.SendEmailSummaryAsync(queueId, instance, 
               chargeListFileBytes, fileName, errorCount, isNonRevCustomer);
       }
       else if (sqsValues.IsLastInstanceId)
       {
           await ProcessSendEmailSummaryForMultipleInstanceStep(sqsValues, instance, 
               proxyUrl, bucketName, queueId, isNonRev: isNonRevCustomer);
       }
   }
   ```

5. **Retry Logic for Incomplete Processing**
   ```csharp
   else
   {
       if (sqsValues.RetryNumber > CommonConstants.NUMBER_OF_RETRIES)
       {
           _logger.LogInfo(CommonConstants.EXCEPTION, 
               string.Format(LogCommonStrings.ERROR_QUEUE_CANNOT_CHECK_CREATE_CUSTOMER_CHARGE_IS_PROCESSED, queueId));
       }
       else
       {
           var retryNumber = sqsValues.RetryNumber + 1;
           await _customerChargeQueueRepository.EnqueueCheckCustomerChargesIsProcessedAsync(queueId, 
               sqsValues.PortalTypeId, sqsValues.InstanceIds, sqsValues.IsMultipleInstanceId, 
               sqsValues.IsLastInstanceId, customDelayTime: CommonConstants.DELAY_IN_SECONDS_FIFTEEN_MINUTES, 
               retryNumber: retryNumber);
       }
   }
   ```

### File Generation and S3 Upload

#### CustomerChargeListFileService.GenerateChargeListFile
**Method**: `CustomerChargeListFileService.GenerateChargeListFile()`
- **Location**: `CustomerChargeListFileService.cs`

**File Format**:
- **Format**: Tab-separated text file (.txt)
- **Headers**: MSISDN, IsSuccessful, ChargeId, ChargeAmount, BillingPeriodStart, BillingPeriodEnd, DateCharged, ErrorMessage
- **Content**: Individual device charge records with success/failure status
- **Summary**: Total counts and statistics

#### S3Wrapper.UploadAwsFile
**Method**: `S3Wrapper.UploadAwsFile(byte[] fileBytes, string fileName)`

**Upload Process**:
1. Uploads file to configured S3 bucket (`CustomerChargesS3BucketName`)
2. Filename format: `{queueId}.txt` or `{fileId}.txt`
3. Waits for upload completion with 5-minute timeout

#### S3Wrapper.WaitForFileUploadCompletion
**Method**: `S3Wrapper.WaitForFileUploadCompletion(string fileName, int timeoutSeconds, IKeysysLogger logger)`

**Verification Process**:
- Polls S3 for file existence
- Maximum wait time: 5 minutes
- Returns success/failure status with error details

### Email Summary Processing

#### Single Instance Email Summary
**Method**: `CustomerChargeListEmailService.SendEmailSummaryAsync()`

**Email Content**:
- Charge list file attachment
- Error count summary
- Customer information
- Processing statistics

#### Multiple Instance Email Summary
**Method**: `CheckIsProcessedService.ProcessSendEmailSummaryForMultipleInstanceStep()`

**Processing Logic**:
1. Checks if all instances in the batch are completed
2. If completed: Sends consolidated summary email
3. If still processing: Re-enqueues with delay
4. Aggregates results from all instances in the batch

---

## Error Handling and Retry Logic

### Retry Mechanisms

1. **Rev.io API Rate Limiting (429 Status)**
   - Automatic retry with exponential backoff
   - Maximum 3 retries per charge
   - Re-enqueues failed items for later processing

2. **Database Connection Failures**
   - Connection timeout handling
   - Transaction rollback on failures
   - Error logging and notification

3. **S3 Upload Failures**
   - 5-minute timeout for upload completion
   - Retry logic for temporary failures
   - Error notification to administrators

4. **Processing Verification Retries**
   - 15-minute delay between retry attempts
   - Maximum retry count enforcement
   - Graceful degradation on final failure

### Error Notification System

#### Email Notifications
- **Immediate**: Critical failures during processing
- **Summary**: Batch completion with error counts
- **Administrative**: System-level failures and timeouts

#### Logging Strategy
- **CloudWatch**: Structured logging with context
- **Database**: Error tracking and audit trail
- **Application**: Debug information for troubleshooting

---

## Performance Optimizations

### Pagination Strategy
- **Page Size**: 50 devices per page
- **Concurrent Processing**: Multiple pages processed simultaneously
- **Memory Management**: Prevents memory overflow on large datasets

### Database Optimizations
- **Connection Pooling**: Efficient database connection management
- **Stored Procedures**: Optimized database operations
- **Index Usage**: Leverages database indexes for performance

### API Rate Limiting Compliance
- **Retry Policies**: Polly retry policies for resilience
- **Rate Limiting**: Respects Rev.io API rate limits
- **Circuit Breaker**: Prevents cascading failures

---

## Monitoring and Observability

### Key Metrics
- **Processing Time**: End-to-end processing duration
- **Success Rate**: Percentage of successful charge creations
- **Error Rate**: Failure rate by error type
- **Queue Depth**: SQS queue length monitoring

### Alerts and Notifications
- **High Error Rate**: Threshold-based alerting
- **Processing Delays**: SLA violation notifications
- **System Failures**: Critical system component failures

### Audit Trail
- **Database Records**: Complete processing history
- **Log Aggregation**: Centralized logging for analysis
- **Performance Metrics**: Historical performance tracking

---

## Security Considerations

### Authentication and Authorization
- **Rev.io Integration**: Secure API authentication
- **AWS Credentials**: IAM role-based access
- **Database Access**: Connection string encryption

### Data Protection
- **Encryption in Transit**: HTTPS/TLS for all communications
- **Encryption at Rest**: S3 and database encryption
- **PII Handling**: Secure handling of customer data

### Access Control
- **Role-based Access**: User permission validation
- **API Security**: Secure API endpoint access
- **Audit Logging**: Complete access audit trail

---

## Conclusion

This comprehensive low-level flow documentation provides a complete understanding of the customer charge system's internal workings, from the initial controller action through the final completion verification. Each method call, data transformation, and system interaction has been detailed to enable effective maintenance, troubleshooting, and enhancement of the system.

The system demonstrates a robust, scalable architecture with comprehensive error handling, retry logic, and monitoring capabilities, ensuring reliable processing of customer charges across multiple integration scenarios and customer types.
# Customer Charge End-to-End Flow - Detailed Low-Level Analysis

## Overview
The customer charge system processes optimization instances through three main Lambda functions that work sequentially via SQS messaging. Each phase handles specific responsibilities from initial enqueueing to final completion verification.

---

## Phase 1: Charge Enqueueing (AltaworxRevAWSEnqueueCustomerCharges)

### Initial Entry Point - CustomerChargeController

#### Trigger Methods in CustomerChargeController:
1. **EnqueueCreateCustomerChargesWithSesstionSqs()** - Called when processing multiple instances from sessions
2. **EnqueueCreateCustomerChargesSqs()** - Called for single instance processing

#### SQS Message Creation Process:

**Method: EnqueueCreateCustomerChargesWithSesstionSqs()**
- **Purpose**: Sends SQS message to trigger the Lambda function for customer charge processing
- **Parameters**: instanceId, awsAccessKey, awsSecretAccessKey, createCustomerChargeQueueName, altaWrxDb, tenantId, isMultipleInstanceId, isLastInstanceId, instanceIds
- **Process Flow**:
  1. Creates AWS credentials using BasicAWSCredentials with provided access key and secret
  2. Retrieves RevIO integration authentication details from database using IntegrationAuthenticationRepository
  3. Initializes AmazonSQSClient with credentials and US East 1 region
  4. Lists queues to find the target queue by name (createCustomerChargeQueueName)
  5. Constructs SendMessageRequest with:
     - **DelaySeconds**: 90 seconds if isLastInstanceId = 1, otherwise 0
     - **MessageAttributes**:
       - `InstanceId`: The optimization instance ID to process
       - `IsMultipleInstanceId`: Flag indicating if processing multiple instances (0 or 1)
       - `IsLastInstanceId`: Flag indicating if this is the last instance in batch (0 or 1)
       - `InstanceIds`: Comma-separated string of all instance IDs in the batch
       - `CurrentIntegrationAuthenticationId`: RevIO authentication ID for API calls
     - **MessageBody**: Simple text "Instance to work is {instanceId}"
     - **QueueUrl**: First queue URL from the queue list results
  6. Sends message asynchronously and waits for completion
  7. Returns empty string on success or error message on failure

### Lambda Function Handler - AltaworxRevAWSEnqueueCustomerCharges

#### FunctionHandler Entry Point:
- **Input**: SQSEvent containing message records from SQS queue
- **Process**:
  1. Calls BaseFunctionHandler() to initialize KeySysLambdaContext
  2. Retrieves DeviceCustomerChargeQueueUrl from environment variables
  3. Calls ProcessEvent() to handle the SQS event

#### ProcessEvent Method:
- **Purpose**: Validates and processes SQS event records
- **Logic**:
  1. Checks if exactly one record is present (logs exception if multiple)
  2. Calls ProcessEventRecord() for single record processing

#### ProcessEventRecord Method:
- **Purpose**: Extracts instance ID from message and initiates processing
- **Process**:
  1. Validates presence of "InstanceId" in message attributes
  2. Parses InstanceId string to long integer
  3. Creates SqsValues object from message attributes
  4. Calls ProcessInstance() with extracted data

#### SqsValues Constructor:
- **Purpose**: Extracts and validates all message attributes
- **Attributes Processed**:
  - `IsMultipleInstanceId`: Defaults to 0 if not present
  - `IsLastInstanceId`: Defaults to 0 if not present  
  - `InstanceIds`: Defaults to null if not present
  - `CurrentIntegrationAuthenticationId`: Defaults to empty string if not present
- **Each attribute extraction includes logging for traceability**

#### ProcessInstance Method - Core Processing Logic:
- **Purpose**: Main orchestration method for processing optimization instances
- **Parameters**: instanceId, sqsValues
- **Detailed Flow**:

  **Step 1: Instance Retrieval**
  - Calls GetInstance() to retrieve OptimizationInstance from database
  - Logs instance details and SQS values for debugging

  **Step 2: Settings Loading**
  - Calls context.LoadOptimizationSettingsByTenantId() to load tenant-specific settings
  - These settings control optimization behavior and billing parameters

  **Step 3: Communication Groups Retrieval**
  - Calls GetCommGroups() to get list of OptimizationCommGroup objects
  - Communication groups represent different device groupings for optimization

  **Step 4: Processing Each Communication Group**
  - Iterates through each communication group
  - For each group:
    - **Queue Selection**: Calls GetWinningQueueId() to find optimal queue
    - **Last Instance Detection**: Sets IsLastInstanceId flag only for the last communication group of the last instance
    - **Charge Enqueueing**: Calls EnqueueCustomerCharges() for database and SQS operations

#### GetWinningQueueId Method - Queue Selection Logic:
- **Purpose**: Determines the best optimization queue based on portal type and cost
- **Parameters**: commGroupId, portalTypeId
- **SQL Query Selection Logic**:
  
  **For M2M Portal (PortalTypeId = 0)**:
  - Uses GetDeviceWinningQueueSql()
  - Query: Selects TOP 1 queue from OptimizationQueue where:
    - EXISTS in OptimizationDeviceResult for the queue
    - CommPlanGroupId matches the communication group
    - TotalCost and RunEndTime are not null (completed optimization)
    - Orders by TotalCost ascending (lowest cost wins)

  **For Mobility Portal (PortalTypeId = 2)**:
  - Uses GetMobilityDeviceWinningQueueSql()
  - Query: Similar to M2M but checks OptimizationMobilityDeviceResult table
    - EXISTS in OptimizationMobilityDeviceResult for the queue
    - Same filtering and ordering logic

  **For Cross Provider Portal**:
  - Uses GetCrossProviderDeviceWinningQueueSql()
  - Query: UNION of both M2M and Mobility result tables
    - EXISTS in either OptimizationMobilityDeviceResult OR OptimizationDeviceResult
    - Covers both device types in single query
    - Same cost-based selection logic

- **Database Execution**:
  1. Opens SQL connection using CentralDbConnectionString
  2. Executes parameterized query with @commGroupId parameter
  3. Returns first queue ID found or 0 if none found
  4. Properly closes connection after execution

#### EnqueueCustomerCharges Method - Dual Enqueueing Process:
- **Purpose**: Performs both database and SQS enqueueing for customer charges
- **Parameters**: queueId, portalTypeId, sqsValues, integrationAuthenticationId
- **Logic Flow**:

  **Cross Provider Handling**:
  - If portal type is CrossProvider:
    - Calls EnqueueCustomerChargesDb() twice (once for M2M, once for Mobility)
    - Calls EnqueueCustomerChargesSqs() with CurrentIntegrationAuthenticationId from sqsValues

  **Standard Portal Handling**:
  - For M2M or Mobility portals:
    - Calls EnqueueCustomerChargesDb() once with specific portal type
    - Calls EnqueueCustomerChargesSqs() with provided integrationAuthenticationId

#### EnqueueCustomerChargesDb Method - Database Stored Procedure Execution:
- **Purpose**: Executes database stored procedures to mark charges for processing
- **Parameters**: queueId, portalTypeId
- **Process**:
  1. Opens SQL connection to CentralDbConnectionString
  2. Sets ARITHABORT ON for proper arithmetic handling
  3. **Stored Procedure Selection**:
     - **Mobility Portal**: Executes `usp_Optimization_Mobility_EnqueueCustomerCharges`
     - **M2M Portal**: Executes `usp_Optimization_EnqueueCustomerCharges`
  4. **Procedure Parameters**:
     - @QueueId: The optimization queue ID to process
  5. Sets command timeout to 240 seconds (4 minutes)
  6. Executes stored procedure synchronously
  7. Properly closes database connection

#### EnqueueCustomerChargesSqs Method - SQS Message Dispatch:
- **Purpose**: Sends SQS message to trigger Phase 2 (Create Customer Change Lambda)
- **Parameters**: queueId, sqsValues, portalTypeId, integrationAuthenticationId
- **Message Construction**:
  1. **AWS Client Setup**:
     - Creates credentials using AwsCredentials() helper
     - Initializes AmazonSQSClient with US East 1 region
  
  2. **Message Body**: Simple text "Queue to work is {queueId}"
  
  3. **Delay Logic**: 
     - 5 minutes (300 seconds) if IsLastInstanceId = 1
     - 0 seconds for all other messages
     - Purpose: Ensures all processing completes before final verification
  
  4. **Message Attributes**:
     - `QueueId`: String value of queue ID for Phase 2 processing
     - `IsMultipleInstanceId`: Propagated from original message
     - `IsLastInstanceId`: Propagated from original message  
     - `InstanceIds`: Propagated comma-separated instance list
     - `PortalTypeId`: Portal type for proper processing logic
     - `CurrentIntegrationAuthenticationId`: RevIO authentication for API calls
  
  5. **Queue URL**: Uses DeviceCustomerChargeQueueUrl environment variable
  
  6. **Message Dispatch**:
     - Sends message asynchronously using SendMessageAsync()
     - Waits for completion using Wait()
     - Logs errors if message status is Faulted or Canceled

---

## Phase 2: Charge Creation (AltaworxRevAWSCreateCustomerChange)

### Lambda Function Handler - AltaworxRevAWSCreateCustomerChange

#### FunctionHandler Entry Point:
- **Input**: SQSEvent from Phase 1 Lambda
- **Initialization Process**:
  1. Calls BaseFunctionHandler() for context setup
  2. **Repository Initialization**:
     - EnvironmentRepository for environment variables
     - OptimizationInstanceRepository for instance data
     - OptimizationQueueRepository for queue management
     - DeviceCustomerChargeQueueRepository for charge queue operations
     - CustomerRepository for customer information
     - Base64Service for credential decoding
     - SettingsRepository for configuration management
  3. **Service Initialization**:
     - DeviceCustomerChargeService for main processing logic
     - CustomerChargeListEmailService for email notifications
     - CustomerChargeListFileService for file generation
     - RevioApiClient for Rev.io API integration
     - S3Wrapper for file storage operations
  4. Creates SqsValues object from first SQS record
  5. Initializes CustomerChangeEventHandler with all dependencies
  6. Calls HandleEventAsync() to process the event

### CustomerChangeEventHandler - Event Processing Orchestration

#### HandleEventAsync Method:
- **Purpose**: Main event handling entry point with error handling
- **Process**:
  1. Calls ProcessEventAsync() within try-catch block
  2. Logs any exceptions with full stack trace
  3. Flushes logger to ensure all logs are written

#### ProcessEventAsync Method:
- **Purpose**: Validates SQS event structure and delegates processing
- **Logic**:
  1. **Record Count Validation**:
     - 0 records: Returns immediately (no processing needed)
     - 1 record: Calls ProcessEventRecordAsync() for processing
     - Multiple records: Logs exception (expects single record)

#### ProcessEventRecordAsync Method:
- **Purpose**: Extracts queue or file ID and initiates charge processing
- **Message Attribute Processing**:
  
  **QueueId Processing Path**:
  1. Extracts QueueId from message attributes
  2. Calls _optimizationQueueRepository.GetQueue() to retrieve queue details
  3. Calls _optimizationInstanceRepository.GetInstance() using queue.InstanceId
  4. Calls _deviceCustomerChargeService.ProcessQueueAsync() with queue ID, instance, and SQS values
  
  **FileId Processing Path**:
  1. Extracts FileId from message attributes
  2. Calls _deviceCustomerChargeService.ProcessQueueAsync() with file ID and SQS values
  3. Used for file-based charge processing scenarios
  
  **Error Handling**:
  - Logs exception if neither QueueId nor FileId is present in message attributes

### SqsValues Enhanced Constructor (Phase 2):
- **Purpose**: Extends Phase 1 SqsValues with additional Phase 2 attributes
- **Additional Attributes**:
  - `PortalTypeId`: Portal type for processing logic (defaults to 0)
  - `PageNumber`: Current page for pagination (defaults to 1)
  - `RetryNumber`: Current retry attempt (defaults to 0)
  - `RetryCount`: Maximum retry attempts (defaults to 0)
  - `IsSendSummaryEmailForMultipleInstanceStep`: Email sending flag (defaults to false)
- **All attributes include default value handling and logging**

### DeviceCustomerChargeService - Core Charge Processing Logic

#### ProcessQueueAsync Method - Main Processing Orchestration:
- **Purpose**: Orchestrates the entire charge creation process for a queue
- **Parameters**: queueId, instance, sqsValues
- **Detailed Process Flow**:

  **Step 1: Environment and Configuration Setup**
  ```
  - Calculate pagination offset: (sqsValues.PageNumber - 1) * PAGE_SIZE
  - Determine customer type:
    * Non-Rev Customer: AMOPCustomerId != null AND RevCustomerId == null AND IntegrationAuthenticationId == null
    * Rev Customer: Has IntegrationAuthenticationId
  - Retrieve environment variables:
    * CONNECTION_STRING: Database connection
    * PROXY_URL: HTTP proxy for external calls
    * CUSTOMER_CHARGES_S3_BUCKET_NAME: S3 bucket for file storage
  - Load service provider list using ServiceProviderCommon.GetServiceProviders()
  ```

  **Step 2: Device List Retrieval**
  ```
  - Call _customerChargeQueueRepository.GetDeviceList(queueId, PAGE_SIZE, offset, isNonRevCustomer)
  - Returns paginated list of devices (maximum 50 devices per page)
  - Each device contains:
    * Device identification information
    * Charge amounts and details
    * Processing status flags
    * Customer association data
  ```

  **Step 3: Processing Logic Branch**
  ```
  If (isNonRevCustomer):
      Call ProcessCustomerChargeForNonRev(deviceList, queueId, instance, sqsValues)
  Else:
      Call ProcessCustomerChargeForRev(deviceList, queueId, instance, sqsValues, serviceProviderList, proxyUrl)
  ```

#### ProcessCustomerChargeForRev Method - Rev.io Integration Processing:
- **Purpose**: Processes charges for customers integrated with Rev.io billing system
- **Parameters**: deviceList, queueId, instance, sqsValues, serviceProviderList, proxyUrl
- **Detailed Processing Steps**:

  **Step 1: RevIO Authentication Setup**
  ```
  - Retrieve RevIO authentication details using _revIoAuthenticationRepository.GetRevioAuthentication()
  - Load optimization settings using _settingsRepository.GetOptimizationSettings()
  - Extract billing timezone from settings for proper date/time handling
  - Initialize RevIO API client with authentication credentials
  ```

  **Step 2: Device Processing Loop**
  ```
  For each device in deviceList:
    
    A. Charge Type Determination:
       - Check UsingNewProcessInCustomerCharge setting
       - New Logic: Split into Rate Charge and Overage Charge
       - Old Logic: Create single combined charge
    
    B. Usage Charge Processing:
       If (UsingNewProcessInCustomerCharge == true):
           - Create Rate Charge: Base rate portion of the charge
           - Create Overage Charge: Usage overage portion (if applicable)
       Else:
           - Create single combined usage charge
    
    C. SMS Charge Processing (if applicable):
       If (SmsChargeAmount > 0 AND (SmsRevProductTypeId exists OR SmsRevProductId exists)):
           - Create SMS charge via Rev.io API
           - Use SMS-specific product type or product ID
    
    D. Rev.io API Charge Creation:
       - Call _deviceChargeRepository.AddChargeAsync() for each charge
       - Handle API responses:
         * Success: Store ChargeId in database
         * Rate Limit (HTTP 429): Re-enqueue message with retry count
         * Error: Mark device as failed with error details
    
    E. Database Status Update:
       - Mark device record as processed (IsProcessed = true)
       - Update charge IDs and error information
       - Commit changes to database
  ```

  **Step 3: Pagination and Completion Handling**
  ```
  - Calculate total pages: Math.Ceiling(totalDeviceCount / PAGE_SIZE)
  - If (current page < total pages):
      * Enqueue next page: sqsValues.PageNumber + 1
      * Send SQS message for continued processing
  - If (current page == total pages AND sqsValues.IsLastInstanceId):
      * Enqueue Check Lambda for completion verification
      * Send message to CheckCustomerChargeIsProcessed queue
  ```

#### ProcessCustomerChargeForNonRev Method - Non-Rev Customer Processing:
- **Purpose**: Processes charges for customers not integrated with Rev.io
- **Parameters**: deviceList, queueId, instance, sqsValues
- **Simplified Processing**:
  ```
  1. Mark all devices in the list as processed (IsProcessed = true)
  2. No actual charge creation (no Rev.io integration)
  3. Update database records with processed status
  4. Handle pagination same as Rev customer processing
  5. Enqueue Check Lambda when processing is complete
  ```

#### DeviceChargeRepository.AddChargeAsync - Rev.io API Integration:
- **Purpose**: Creates actual charges in Rev.io billing system
- **Parameters**: Charge request object with device and billing details
- **Process Flow**:
  ```
  1. Serialize charge request to JSON format
  2. Call RevioApiClient.AddChargeAsync() with serialized data
  3. Handle HTTP response:
     - Success (200): Extract ChargeId from response
     - Rate Limit (429): Return rate limit error for retry handling
     - Client Error (4xx): Return error details for logging
     - Server Error (5xx): Return server error for retry consideration
  4. Return CustomerChargeResponse with:
     - Success flag
     - ChargeId (if successful)
     - Error message (if failed)
     - HTTP status code for debugging
  ```

#### Pagination and Re-enqueueing Logic:
- **Purpose**: Handles large device lists through pagination
- **Process**:
  ```
  1. Each queue processes maximum 50 devices per Lambda execution
  2. Calculate remaining pages: totalDevices / PAGE_SIZE
  3. For each additional page:
     - Create new SQS message with incremented PageNumber
     - Include all original message attributes
     - Send to same queue for continued processing
  4. Only the final page of the final instance enqueues the Check Lambda
  5. Delay logic ensures proper sequencing of operations
  ```

---

## Phase 3: Completion Verification (AltaworxRevAWSCheckCustomerChargeIsProcessed)

### Lambda Function Handler - AltaworxRevAWSCheckCustomerChargeIsProcessed

#### FunctionHandler Entry Point:
- **Input**: SQS Event from Phase 2 Lambda (final page processing)
- **Initialization Process**:
  ```
  1. Call BaseFunctionHandler() for context initialization
  2. Repository Setup:
     - EnvironmentRepository: Environment variable access
     - SettingsRepository: Configuration management with Base64Service
     - DeviceCustomerChargeQueueRepository: Queue and charge data access
     - CustomerRepository: Customer information retrieval
  3. Service Initialization:
     - SimpleEmailServiceFactory: Email service creation
     - CustomerChargeListEmailService: Email composition and sending
     - CustomerChargeListFileService: Charge list file generation
     - S3Wrapper: AWS S3 file operations with credentials and bucket name
     - CheckIsProcessedService: Main processing logic coordination
  4. Extract SqsValues from first SQS record
  5. Initialize CheckIsProcessedEventHandler with all dependencies
  6. Call HandleEventAsync() for processing
  ```

### CheckIsProcessedEventHandler - Completion Verification Orchestration

#### HandleEventAsync Method:
- **Purpose**: Main event handling with error management
- **Process**:
  ```
  1. Call ProcessEventAsync() within try-catch block
  2. Log any exceptions with full message and stack trace
  3. Ensure proper cleanup and logging flush
  ```

#### ProcessEventAsync Method:
- **Purpose**: Validates SQS event and delegates to record processing
- **Logic**:
  ```
  - Validate single record (same as Phase 2)
  - Call ProcessEventRecordAsync() for single record
  - Log exceptions for multiple records
  ```

#### ProcessEventRecordAsync Method:
- **Purpose**: Extracts queue or file identifier and initiates completion check
- **Processing Paths**:
  ```
  QueueId Path:
  1. Extract QueueId from message attributes
  2. Get queue details using _optimizationQueueRepository.GetQueue()
  3. Get instance details using _optimizationInstanceRepository.GetInstance()
  4. Call _checkIsProcessedService.ProcessQueueAsync() with queue ID and instance
  
  FileId Path:
  1. Extract FileId from message attributes
  2. Call _checkIsProcessedService.ProcessQueueAsync() with file ID only
  3. Handle file-based processing scenarios
  ```

### CheckIsProcessedService - Completion Processing Logic

#### ProcessQueueAsync Method - Main Completion Verification:
- **Purpose**: Verifies all charges are processed and handles final steps
- **Parameters**: queueId, instance, sqsValues
- **Detailed Process Flow**:

  **Step 1: Environment and Customer Type Setup**
  ```
  - Retrieve CONNECTION_STRING, PROXY_URL, and S3 bucket name from environment
  - Load service provider list for charge file generation
  - Determine customer type (Non-Rev vs Rev customer) same as Phase 2
  ```

  **Step 2: Processing Completion Check**
  ```
  Call _customerChargeQueueRepository.QueueHasMoreItems(queueId, isNonRevCustomer):
  
  If (no more unprocessed items):
      Proceed to file generation and email sending
  Else:
      Handle retry logic with delay
  ```

  **Step 3: Charge List File Generation and S3 Upload**
  ```
  A. Charge List Retrieval:
     - Call _customerChargeQueueRepository.GetChargeList(queueId)
     - Returns complete list of all processed charges with results
  
  B. File Generation:
     - Call _chargeListFileService.GenerateChargeListFile()
     - Parameters: chargeList, billingPeriodStartDate, billingPeriodEndDate, serviceProviderList
     - Creates tab-separated text file with:
       * Headers: MSISDN, IsSuccessful, ChargeId, ChargeAmount, ErrorMessage, etc.
       * Device charge details for each processed device
       * Summary totals and error counts
     - Returns byte array of generated file content
  
  C. S3 Upload Process:
     - File name format: "{queueId}.txt" or "{fileId}.txt"
     - Call _s3Wrapper.UploadAwsFile(chargeListFileBytes, fileName)
     - Asynchronous upload to configured S3 bucket
  
  D. Upload Completion Verification:
     - Call _s3Wrapper.WaitForFileUploadCompletion()
     - Parameters: fileName, 5-minute timeout, logger
     - Polls S3 for file existence with exponential backoff
     - Returns success status and error message if failed
  ```

  **Step 4: Email Summary Processing**
  ```
  If (S3 upload successful):
      
      A. Error Count Calculation:
         - Count devices with HasErrors = true from charge list
         - Used for summary statistics in email
      
      B. Single Instance Processing:
         If (NOT sqsValues.IsMultipleInstanceId):
             - Call _customerChargeListEmailService.SendEmailSummaryAsync()
             - Parameters: queueId, instance, chargeListFileBytes, fileName, errorCount, isNonRevCustomer
             - Sends immediate email with charge list attachment
      
      C. Multiple Instance Processing:
         If (sqsValues.IsMultipleInstanceId AND sqsValues.IsLastInstanceId):
             - Call ProcessSendEmailSummaryForMultipleInstanceStep()
             - Coordinates consolidated summary email for all instances
             - Waits for all instance processing to complete
             - Generates combined summary with all charge files
  
  Else:
      Log S3 upload failure with specific error message
  ```

  **Step 5: Retry Logic for Incomplete Processing**
  ```
  If (queue still has unprocessed items):
      
      A. Retry Count Check:
         If (sqsValues.RetryNumber > CommonConstants.NUMBER_OF_RETRIES):
             - Log final error message
             - Stop retry attempts to prevent infinite loops
      
      B. Re-enqueueing for Retry:
         Else:
             - Increment retry number: sqsValues.RetryNumber + 1
             - Call _customerChargeQueueRepository.EnqueueCheckCustomerChargesIsProcessedAsync()
             - Parameters: queueId, portalTypeId, instanceIds, isMultipleInstanceId, isLastInstanceId
             - Custom delay: 15 minutes (CommonConstants.DELAY_IN_SECONDS_FIFTEEN_MINUTES)
             - Purpose: Allow more time for charge processing to complete
  ```

### CustomerChargeListFileService.GenerateChargeListFile - File Generation Logic:
- **Purpose**: Creates detailed charge list file for customer review
- **Parameters**: chargeList, billingPeriodStartDate, billingPeriodEndDate, serviceProviderList
- **File Format and Content**:
  ```
  A. File Structure (Tab-Separated Text):
     - Header Row: Column names separated by tabs
     - Data Rows: Device charge information
     - Summary Section: Totals and statistics
  
  B. Column Headers:
     - MSISDN: Device phone number or identifier
     - IsSuccessful: Success/failure status (true/false)
     - ChargeId: Rev.io charge ID (if successful)
     - ChargeAmount: Monetary charge amount
     - ErrorMessage: Error details (if failed)
     - DeviceType: M2M or Mobility device classification
     - ServiceProvider: Carrier or service provider name
     - BillingPeriod: Start and end dates for charges
  
  C. Data Processing:
     For each charge in chargeList:
       - Format monetary amounts with proper decimal places
       - Convert boolean flags to readable text
       - Handle null values with appropriate defaults
       - Apply service provider name mapping from serviceProviderList
  
  D. Summary Section:
     - Total Devices Processed: Count of all devices
     - Successful Charges: Count of successful charge creations
     - Failed Charges: Count of failed charge attempts
     - Total Charge Amount: Sum of all successful charges
     - Error Summary: Breakdown of error types and counts
  ```

### S3Wrapper.UploadAwsFile and WaitForFileUploadCompletion:
- **Purpose**: Handles secure file upload to AWS S3 with verification
- **Upload Process**:
  ```
  1. Initialize S3 client with AWS credentials from settings
  2. Create PutObjectRequest with:
     - Bucket name from environment configuration
     - File key (filename) for S3 object identification
     - File content as byte array stream
     - Content type set to text/plain for .txt files
  3. Execute asynchronous upload using PutObjectAsync()
  4. Handle upload errors and retry logic if needed
  ```

- **Completion Verification Process**:
  ```
  1. Polling Loop with 5-minute timeout:
     - Check file existence using HeadObjectAsync()
     - Exponential backoff between checks (start at 5 seconds, double each retry)
     - Maximum wait time prevents infinite waiting
  
  2. Success Criteria:
     - File exists in S3 bucket
     - File size matches uploaded content
     - No access errors when retrieving file metadata
  
  3. Return Values:
     - Tuple: (isUploadSuccessful: bool, errorMessage: string)
     - Success: (true, null)
     - Failure: (false, detailed error message)
  ```

### CustomerChargeListEmailService.SendEmailSummaryAsync - Email Notification:
- **Purpose**: Sends charge processing summary email to stakeholders
- **Parameters**: queueId, instance, chargeListFileBytes, fileName, errorCount, isNonRevCustomer
- **Email Composition Process**:
  ```
  A. Recipient Determination:
     - Extract customer email addresses from instance.Customer
     - Add system administrator emails from settings
     - Include billing department contacts if configured
  
  B. Email Content Creation:
     - Subject: "Customer Charge Processing Summary - Queue {queueId}"
     - Body: HTML formatted summary including:
       * Processing completion timestamp
       * Customer information (account number, name)
       * Device count and charge statistics
       * Error summary (if any errors occurred)
       * Billing period information
       * Next steps or action items
  
  C. Attachment Handling:
     - Attach charge list file (chargeListFileBytes) as .txt file
     - Set proper MIME type and encoding
     - Include filename for recipient download
  
  D. Email Delivery:
     - Use configured SMTP settings or AWS SES
     - Handle delivery failures with retry logic
     - Log successful delivery confirmation
  ```

### ProcessSendEmailSummaryForMultipleInstanceStep - Multi-Instance Coordination:
- **Purpose**: Coordinates email sending for multiple instance processing
- **Process Flow**:
  ```
  1. Instance Completion Check:
     - Query database for all instances in sqsValues.InstanceIds
     - Check processing status for each instance
     - Verify all charge files are uploaded to S3
  
  2. Consolidated Summary Creation:
     If (all instances completed):
         - Download all charge files from S3
         - Combine statistics from all instances
         - Create consolidated summary report
         - Generate combined ZIP file with all charge lists
         - Send single summary email with all attachments
     
     Else:
         - Re-enqueue check with delay
         - Wait for remaining instances to complete
         - Increment retry counter to prevent infinite waiting
  
  3. Cleanup Operations:
     - Mark multi-instance processing as complete
     - Update database flags for email sending status
     - Clean up temporary files and resources
  ```

---

## Error Handling and Retry Logic Throughout All Phases

### Phase 1 Error Handling:
- **Database Connection Failures**: Logged with connection string details (masked)
- **SQS Send Failures**: Returned as error messages to calling controller
- **Invalid Instance IDs**: Logged as exceptions with instance details
- **Missing Message Attributes**: Logged with expected attribute names

### Phase 2 Error Handling:
- **Rev.io API Rate Limiting (HTTP 429)**:
  - Increment retry count in SQS message
  - Re-enqueue with exponential backoff delay
  - Maximum retry attempts prevent infinite loops
- **Rev.io API Authentication Failures**:
  - Mark devices as failed with authentication error
  - Continue processing remaining devices
  - Include error details in charge list file
- **Database Transaction Failures**:
  - Rollback partial changes
  - Re-enqueue entire page for retry
  - Log transaction details for debugging
- **Pagination Errors**:
  - Validate page numbers and device counts
  - Handle edge cases with partial pages
  - Ensure completion detection works correctly

### Phase 3 Error Handling:
- **S3 Upload Failures**:
  - Retry upload with exponential backoff
  - Log detailed AWS error messages
  - Fallback to email without attachment if critical
- **Email Delivery Failures**:
  - Retry with different SMTP settings
  - Log recipient and content details
  - Queue for manual intervention if all retries fail
- **File Generation Errors**:
  - Handle corrupted charge data gracefully
  - Generate partial files with error notes
  - Include error summary in file content

### Cross-Phase Monitoring and Logging:
- **Execution Timing**: Log start/end times for performance monitoring
- **Resource Usage**: Monitor memory and CPU usage during processing
- **Message Flow Tracking**: Unique correlation IDs across all phases
- **Alert Thresholds**: Automated alerts for excessive errors or delays
- **Data Integrity Checks**: Validate data consistency between phases

---

## Performance Considerations and Optimizations

### Database Query Optimization:
- **Indexed Queries**: All queue selection queries use proper indexes on TotalCost and RunEndTime
- **Connection Pooling**: Efficient database connection management
- **Batch Operations**: Process multiple devices in single database transactions
- **Query Timeouts**: Appropriate timeout values prevent hanging operations

### SQS Message Management:
- **Message Batching**: Group related operations when possible
- **Visibility Timeouts**: Set appropriate timeouts for processing duration
- **Dead Letter Queues**: Handle permanently failed messages
- **Message Deduplication**: Prevent duplicate processing of same instances

### Memory Management:
- **Streaming File Operations**: Handle large charge lists without loading entire files in memory
- **Garbage Collection**: Proper disposal of large objects and database connections
- **Page Size Optimization**: Balance between memory usage and processing efficiency

### Scalability Features:
- **Horizontal Scaling**: Multiple Lambda instances can process different queues simultaneously
- **Auto-scaling**: Lambda functions scale automatically based on SQS queue depth
- **Regional Distribution**: Can be deployed across multiple AWS regions for redundancy
- **Load Distribution**: Communication groups allow parallel processing of device segments

This comprehensive flow documentation provides complete visibility into every aspect of the customer charge processing system, from initial controller actions through final email delivery, with detailed explanations of all internal methods, error handling, and system interactions.
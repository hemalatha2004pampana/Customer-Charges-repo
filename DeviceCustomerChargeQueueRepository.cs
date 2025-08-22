using Altaworx.AWS.Core;
using Altaworx.AWS.Core.Models;
using AltaworxRevAWSCreateCustomerChange.Models;
using Amazon;
using Amazon.Lambda.Core;
using Amazon.SQS;
using Amazon.SQS.Model;
using Amop.Core.Constants;
using Amop.Core.Helpers;
using Amop.Core.Logger;
using Amop.Core.Models;
using Amop.Core.Models.Revio;
using Amop.Core.Models.Settings;
using Amop.Core.Repositories.Environment;
using Amop.Core.Resilience;
using Microsoft.Data.SqlClient;
using Polly;
using System;
using System.Collections.Generic;
using System.Data;
using System.Net;
using System.Threading.Tasks;
using static Altaworx.AWS.Core.RevIOCommon;

namespace AltaworxRevAWSCreateCustomerChange.Repositories.DeviceCustomerCharge
{
    public class DeviceCustomerChargeQueueRepository : IDeviceCustomerChargeQueueRepository
    {
        private const int DelaySeconds = 10;
        private readonly string _connectionString;
        private readonly ILambdaContext _context;
        private readonly IEnvironmentRepository _environmentRepository;
        private readonly IKeysysLogger _logger;
        private readonly ISettingsRepository _settingsRepository;


        public DeviceCustomerChargeQueueRepository(IKeysysLogger logger, IEnvironmentRepository environmentRepository,
            ILambdaContext context, string connectionString, ISettingsRepository settingsRepository)
        {
            _logger = logger;
            _environmentRepository = environmentRepository;
            _context = context;
            _connectionString = connectionString;
            _settingsRepository = settingsRepository;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities", Justification = "Reviewed")]
        public IEnumerable<RevIOCommon.DeviceCustomerChargeQueueRecord> GetDeviceList(long queueId, int pageSize, int offset, bool isNonRev = false)
        {
            var records = new List<RevIOCommon.DeviceCustomerChargeQueueRecord>();

            var optimizationDeviceResult_CustomerChargeQueue = SQLConstant.StoredProcedureName.GET_OPTIMIZATION_DEVICE_RESULT_CUSTOMER_CHARGE_QUEUE;
            var optimizationMobilityDeviceResult_CustomerChargeQueue = SQLConstant.StoredProcedureName.GET_OPTIMIZATION_MOBILITY_DEVICE_RESULT_CUSTOMER_CHARGE_QUEUE;
            if (isNonRev)
            {
                optimizationDeviceResult_CustomerChargeQueue = SQLConstant.StoredProcedureName.GET_OPTIMIZATION_DEVICE_RESULT_CUSTOMER_CHARGE_QUEUE_NON_REV;
                optimizationMobilityDeviceResult_CustomerChargeQueue = SQLConstant.StoredProcedureName.GET_OPTIMIZATION_MOBILITY_DEVICE_RESULT_CUSTOMER_CHARGE_QUEUE_NON_REV;
            }

            var sqlRetryPolicy = GetSqlTransientRetryPolicy(_logger);

            var parameters = new List<SqlParameter>()
            {
                new SqlParameter(CommonSQLParameterNames.QUEUE_ID, queueId),
                new SqlParameter(CommonSQLParameterNames.PAGE_SIZE, pageSize),
                new SqlParameter(CommonSQLParameterNames.OFFSET, offset)
            };
            records.AddRange(sqlRetryPolicy.Execute(() =>
             SqlQueryHelper.ExecuteStoredProcedureWithListResult(ParameterizedLog(_logger), _connectionString,
                 optimizationDeviceResult_CustomerChargeQueue,
                 (dataReader) => DeviceRecordFromReader(dataReader, false),
                 parameters,
                 SQLConstant.ShortTimeoutSeconds)));

            records.AddRange(sqlRetryPolicy.Execute(() =>
                SqlQueryHelper.ExecuteStoredProcedureWithListResult(ParameterizedLog(_logger), _connectionString,
                    optimizationMobilityDeviceResult_CustomerChargeQueue,
                    (dataReader) => DeviceRecordFromReader(dataReader, false),
                    parameters,
                    SQLConstant.ShortTimeoutSeconds)));

            return records;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities", Justification = "Reviewed")]
        public IEnumerable<RevIOCommon.DeviceCustomerChargeQueueRecord> GetDeviceList(int fileId, int pageSize, int offset, bool isNonRev = false)
        {
            var records = new List<RevIOCommon.DeviceCustomerChargeQueueRecord>();

            var optimizationDeviceResult_CustomerChargeQueue = SQLConstant.StoredProcedureName.GET_OPTIMIZATION_DEVICE_RESULT_CUSTOMER_CHARGE_QUEUE;
            var optimizationMobilityDeviceResult_CustomerChargeQueue = SQLConstant.StoredProcedureName.GET_OPTIMIZATION_MOBILITY_DEVICE_RESULT_CUSTOMER_CHARGE_QUEUE;
            if (isNonRev)
            {
                optimizationDeviceResult_CustomerChargeQueue = SQLConstant.StoredProcedureName.GET_OPTIMIZATION_DEVICE_RESULT_CUSTOMER_CHARGE_QUEUE_NON_REV;
                optimizationMobilityDeviceResult_CustomerChargeQueue = SQLConstant.StoredProcedureName.GET_OPTIMIZATION_MOBILITY_DEVICE_RESULT_CUSTOMER_CHARGE_QUEUE_NON_REV;
            }
            var sqlRetryPolicy = GetSqlTransientRetryPolicy(_logger);

            var parameters = new List<SqlParameter>()
            {
                new SqlParameter(CommonSQLParameterNames.FILE_ID, fileId),
                new SqlParameter(CommonSQLParameterNames.PAGE_SIZE, pageSize),
                new SqlParameter(CommonSQLParameterNames.OFFSET, offset)
            };
            records.AddRange(sqlRetryPolicy.Execute(() =>
                SqlQueryHelper.ExecuteStoredProcedureWithListResult(ParameterizedLog(_logger), _connectionString,
                    optimizationDeviceResult_CustomerChargeQueue,
                    (dataReader) => DeviceRecordFromReader(dataReader, true),
                    parameters,
                    SQLConstant.ShortTimeoutSeconds)));

            records.AddRange(sqlRetryPolicy.Execute(() =>
                SqlQueryHelper.ExecuteStoredProcedureWithListResult(ParameterizedLog(_logger), _connectionString,
                    optimizationMobilityDeviceResult_CustomerChargeQueue,
                    (dataReader) => DeviceRecordFromReader(dataReader, true),
                    parameters,
                    SQLConstant.ShortTimeoutSeconds)));

            return records;
        }

        public CustomerChargeUploadedFile GetUploadedFile(int fileId)
        {
            CustomerChargeUploadedFile uploadedFile = null;
            using (var conn = new SqlConnection(_connectionString))
            {
                using (var cmd = new SqlCommand(
                    "SELECT id, [FileName], [Status], [Description], ProcessedBy, ProcessedDate, CreatedBy, CreatedDate, ModifiedBy, ModifiedDate, DeletedBy, DeletedDate, IsDeleted, IsActive, [IntegrationAuthenticationId] FROM CustomerCharge_UploadedFile WHERE IsActive = 1 and IsDeleted = 0 and id = @fileId",
                    conn))
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.Parameters.AddWithValue("@fileId", fileId);
                    conn.Open();

                    var rdr = cmd.ExecuteReader();
                    if (rdr.Read())
                    {
                        uploadedFile = UploadedFileFromReader(rdr);
                    }

                    conn.Close();
                }
            }

            return uploadedFile;
        }

        public bool QueueHasMoreItems(long queueId, bool isNonRev = false)
        {
            var vwOptimizationDeviceResult_CustomerChargeQueue = "vwOptimizationDeviceResult_CustomerChargeQueue";
            var vwOptimizationMobilityDeviceResult_CustomerChargeQueue = "vwOptimizationMobilityDeviceResult_CustomerChargeQueue";
            if (isNonRev)
            {
                vwOptimizationDeviceResult_CustomerChargeQueue = "vwOptimizationDeviceResult_CustomerChargeQueue_NonRev";
                vwOptimizationMobilityDeviceResult_CustomerChargeQueue = "vwOptimizationMobilityDeviceResult_CustomerChargeQueue_NonRev";
            }
            var hasMoreItems = false;
            using (var conn = new SqlConnection(_connectionString))
            {
                conn.Open();

                using (var cmd =
                    new SqlCommand(
                        $"SELECT COUNT(1) AS QueueLength FROM {vwOptimizationDeviceResult_CustomerChargeQueue} WHERE IsProcessed = 0 AND QueueId = @queueId",
                        conn))
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.Parameters.AddWithValue("@queueId", queueId);

                    var deviceCount = cmd.ExecuteScalar();
                    if ((int)deviceCount > 0)
                    {
                        hasMoreItems = true;
                    }
                }

                using (var cmd =
                    new SqlCommand(
                        $"SELECT COUNT(1) AS QueueLength FROM {vwOptimizationMobilityDeviceResult_CustomerChargeQueue} WHERE IsProcessed = 0 AND QueueId = @queueId",
                        conn))
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.Parameters.AddWithValue("@queueId", queueId);

                    var deviceCount = cmd.ExecuteScalar();
                    if ((int)deviceCount > 0)
                    {
                        hasMoreItems = true;
                    }
                }
            }

            return hasMoreItems;
        }

        public bool QueueHasMoreItems(int fileId, bool isNonRev = false)
        {
            var vwOptimizationDeviceResult_CustomerChargeQueue = "vwOptimizationDeviceResult_CustomerChargeQueue";
            var vwOptimizationMobilityDeviceResult_CustomerChargeQueue = "vwOptimizationMobilityDeviceResult_CustomerChargeQueue";
            if (isNonRev)
            {
                vwOptimizationDeviceResult_CustomerChargeQueue = "vwOptimizationDeviceResult_CustomerChargeQueue_NonRev";
                vwOptimizationMobilityDeviceResult_CustomerChargeQueue = "vwOptimizationMobilityDeviceResult_CustomerChargeQueue_NonRev";
            }
            var hasMoreItems = false;
            using (var conn = new SqlConnection(_connectionString))
            {
                conn.Open();

                using (var cmd =
                    new SqlCommand(
                        $"SELECT COUNT(1) AS QueueLength FROM {vwOptimizationDeviceResult_CustomerChargeQueue} WHERE IsProcessed = 0 AND UploadedFileId = @fileId",
                        conn))
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.Parameters.AddWithValue("@fileId", fileId);

                    var deviceCount = cmd.ExecuteScalar();
                    if ((int)deviceCount > 0)
                    {
                        hasMoreItems = true;
                    }
                }

                using (var cmd =
                    new SqlCommand(
                        $"SELECT COUNT(1) AS QueueLength FROM {vwOptimizationMobilityDeviceResult_CustomerChargeQueue} WHERE IsProcessed = 0 AND UploadedFileId = @fileId",
                        conn))
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.Parameters.AddWithValue("@fileId", fileId);

                    var deviceCount = cmd.ExecuteScalar();
                    if ((int)deviceCount > 0)
                    {
                        hasMoreItems = true;
                    }
                }
            }

            return hasMoreItems;
        }

        public async Task EnqueueCustomerChargesAsync(long queueId, int portalTypeId, string instanceIds, bool isMultipleInstance, bool isLastInstanceId, int pageNumber, int currentIntegrationAuthenticationId = 0, bool isSendSummaryEmailForMultipleInstanceStep = false, int retryNumber = 0, int retryCount = 0)
        {
            _logger.LogInfo(CommonConstants.SUB, $"{nameof(queueId)}: {queueId}");
            _logger.LogInfo(CommonConstants.SUB, $"{nameof(isSendSummaryEmailForMultipleInstanceStep)}: {isSendSummaryEmailForMultipleInstanceStep}");
            _logger.LogInfo(CommonConstants.SUB, $"{nameof(retryNumber)}: {retryNumber}");
            _logger.LogInfo(CommonConstants.SUB, $"{nameof(retryCount)}: {retryCount}");

            var awsCredentials = _settingsRepository.GetGeneralProviderSettings().AwsCredentials;
            using (var client = new AmazonSQSClient(awsCredentials, RegionEndpoint.USEast1))
            {
                var requestMsgBody = string.Format(LogCommonStrings.QUEUE_TO_WORK, queueId);
                var request = new SendMessageRequest
                {
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        {
                            SQSMessageKeyConstant.QUEUE_ID, new MessageAttributeValue
                                {DataType = CommonConstants.STRING, StringValue = queueId.ToString()}
                        },
                        {
                            SQSMessageKeyConstant.IS_MULTIPLE_INSTANCE_ID, new MessageAttributeValue
                            { DataType = CommonConstants.STRING, StringValue = Convert.ToInt32(isMultipleInstance).ToString()}
                        },
                        {
                            SQSMessageKeyConstant.IS_LAST_INSTANCE_ID, new MessageAttributeValue
                            { DataType = CommonConstants.STRING, StringValue =  Convert.ToInt32(isLastInstanceId).ToString()}
                        },
                        {
                            SQSMessageKeyConstant.INSTANCE_IDS, new MessageAttributeValue
                            { DataType = CommonConstants.STRING, StringValue = instanceIds}
                        },
                        {
                            SQSMessageKeyConstant.PORTAL_TYPE_ID, new MessageAttributeValue
                            { DataType = CommonConstants.STRING, StringValue = portalTypeId.ToString()}
                        },
                        {
                            SQSMessageKeyConstant.CURRENT_INTEGRATION_AUTHENTICATION_ID, new MessageAttributeValue
                            { DataType = CommonConstants.STRING, StringValue = currentIntegrationAuthenticationId.ToString()}
                        },
                        {
                            SQSMessageKeyConstant.IS_SEND_SUMMARY_EMAIL_FOR_MULTIPLE_INSTANCE_STEP, new MessageAttributeValue
                            { DataType = CommonConstants.STRING, StringValue =  Convert.ToInt32(isSendSummaryEmailForMultipleInstanceStep).ToString()}
                        },
                        {
                            SQSMessageKeyConstant.RETRY_NUMBER, new MessageAttributeValue
                            {DataType = CommonConstants.STRING, StringValue = retryNumber.ToString()}
                        },
                        {
                            SQSMessageKeyConstant.PAGE_NUMBER, new MessageAttributeValue
                            { DataType = CommonConstants.STRING, StringValue = pageNumber.ToString()}
                        },
                        {
                            SQSMessageKeyConstant.RETRY_COUNT, new MessageAttributeValue
                            { DataType = CommonConstants.STRING, StringValue = retryCount.ToString()}
                        }
                    },

                    MessageBody = requestMsgBody,
                    QueueUrl = _environmentRepository.GetEnvironmentVariable(_context, SQSMessageKeyConstant.DEVICE_CUSTOMER_CHARGE_QUEUE_URL)
                };

                var response = await client.SendMessageAsync(request);
                if ((response.HttpStatusCode < HttpStatusCode.OK) || (response.HttpStatusCode >= HttpStatusCode.Ambiguous))
                {
                    _logger.LogInfo(CommonConstants.EXCEPTION, string.Format(LogCommonStrings.ERROR_ENQUEUING, queueId, response.HttpStatusCode, response.HttpStatusCode));
                }
            }
        }

        public async Task EnqueueCustomerChargesAsync(int fileId, int pageNumber, int currentIntegrationAuthenticationId = 0, int retryCount = 0)
        {
            _logger.LogInfo(CommonConstants.SUB, $"{nameof(fileId)}: {fileId}");

            var awsCredentials = _settingsRepository.GetGeneralProviderSettings().AwsCredentials;
            using (var client = new AmazonSQSClient(awsCredentials, RegionEndpoint.USEast1))
            {
                var requestMsgBody = string.Format(LogCommonStrings.FILE_TO_WORK, fileId);
                var request = new SendMessageRequest
                {
                    DelaySeconds = DelaySeconds,
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        {
                            SQSMessageKeyConstant.FILE_ID, new MessageAttributeValue
                                {DataType = CommonConstants.STRING, StringValue = fileId.ToString()}
                        },
                        {
                            SQSMessageKeyConstant.CURRENT_INTEGRATION_AUTHENTICATION_ID, new MessageAttributeValue
                            { DataType = CommonConstants.STRING, StringValue = currentIntegrationAuthenticationId.ToString()}
                        },
                        {
                            SQSMessageKeyConstant.PAGE_NUMBER, new MessageAttributeValue
                            { DataType = CommonConstants.STRING, StringValue = pageNumber.ToString()}
                        },
                        {
                            SQSMessageKeyConstant.RETRY_COUNT, new MessageAttributeValue
                            { DataType = CommonConstants.STRING, StringValue = retryCount.ToString()}
                        }
                    },
                    MessageBody = requestMsgBody,
                    QueueUrl = _environmentRepository.GetEnvironmentVariable(_context, SQSMessageKeyConstant.DEVICE_CUSTOMER_CHARGE_QUEUE_URL)
                };

                var response = await client.SendMessageAsync(request);
                if ((response.HttpStatusCode < HttpStatusCode.OK) || (response.HttpStatusCode >= HttpStatusCode.Ambiguous))
                {
                    _logger.LogInfo(CommonConstants.EXCEPTION, string.Format(LogCommonStrings.ERROR_ENQUEUING, fileId, response.HttpStatusCode, response.HttpStatusCode));
                }
            }
        }

        public void MarkRecordProcessed(long id, string chargeId, decimal chargeAmount, decimal baseChargeAmount,
            decimal totalChargeAmount, bool hasErrors, string errorMessage, string smsChargeId, decimal smsChargeAmount)
        {
            _logger.LogInfo("SUB", $"MarkProcessed(,{id},{chargeId},{chargeAmount},{baseChargeAmount},{totalChargeAmount},{hasErrors},{errorMessage},{smsChargeId},{smsChargeAmount})");

            using (var conn = new SqlConnection(_connectionString))
            {
                conn.Open();

                using (var cmd = new SqlCommand(
                    "UPDATE OptimizationDeviceResult_CustomerChargeQueue SET IsProcessed = 1, ModifiedBy = 'System', ModifiedDate = GETDATE(), ChargeId = @chargeId, ChargeAmount = @chargeAmount, BaseChargeAmount = @baseChargeAmount, TotalChargeAmount = @totalChargeAmount, HasErrors = @hasErrors, ErrorMessage = @errorMessage, SmsChargeId = @smsChargeId, SmsChargeAmount = @smsChargeAmount WHERE Id = @id",
                    conn))
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.Parameters.AddWithValue("@id", id);
                    cmd.Parameters.AddWithValue("@chargeId", chargeId);
                    cmd.Parameters.AddWithValue("@chargeAmount", chargeAmount);
                    cmd.Parameters.AddWithValue("@baseChargeAmount", baseChargeAmount);
                    cmd.Parameters.AddWithValue("@totalChargeAmount", totalChargeAmount);
                    cmd.Parameters.AddWithValue("@hasErrors", hasErrors);
                    cmd.Parameters.AddWithValue("@errorMessage", errorMessage);
                    cmd.Parameters.AddWithValue("@smsChargeId", smsChargeId);
                    cmd.Parameters.AddWithValue("@smsChargeAmount", smsChargeAmount);

                    cmd.ExecuteNonQuery();
                }

                using (var cmd = new SqlCommand(
                    "UPDATE OptimizationMobilityDeviceResult_CustomerChargeQueue SET IsProcessed = 1, ModifiedBy = 'System', ModifiedDate = GETDATE(), ChargeId = @chargeId, ChargeAmount = @chargeAmount, BaseChargeAmount = @baseChargeAmount, TotalChargeAmount = @totalChargeAmount, HasErrors = @hasErrors, ErrorMessage = @errorMessage, SmsChargeId = @smsChargeId, SmsChargeAmount = @smsChargeAmount WHERE Id = @id",
                    conn))
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.Parameters.AddWithValue("@id", id);
                    cmd.Parameters.AddWithValue("@chargeId", chargeId);
                    cmd.Parameters.AddWithValue("@chargeAmount", chargeAmount);
                    cmd.Parameters.AddWithValue("@baseChargeAmount", baseChargeAmount);
                    cmd.Parameters.AddWithValue("@totalChargeAmount", totalChargeAmount);
                    cmd.Parameters.AddWithValue("@hasErrors", hasErrors);
                    cmd.Parameters.AddWithValue("@errorMessage", errorMessage);
                    cmd.Parameters.AddWithValue("@smsChargeId", smsChargeId);
                    cmd.Parameters.AddWithValue("@smsChargeAmount", smsChargeAmount);

                    cmd.ExecuteNonQuery();
                }
            }
        }

        public IEnumerable<RevIOCommon.DeviceCustomerChargeQueueRecord> GetChargeList(long queueId)
        {
            var records = new List<RevIOCommon.DeviceCustomerChargeQueueRecord>();

            var sqlRetryPolicy = GetSqlTransientRetryPolicy(_logger);

            var parameters = new List<SqlParameter>()
            {
                new SqlParameter(CommonSQLParameterNames.QUEUE_ID, queueId)
            };
            records.AddRange(sqlRetryPolicy.Execute(() =>
                SqlQueryHelper.ExecuteStoredProcedureWithListResult(ParameterizedLog(_logger), _connectionString,
                    SQLConstant.StoredProcedureName.DEVICE_CUSTOMER_CHARGE_QUEUE_GET_CHARGE_LIST,
                    (dataReader) => DeviceRecordFromReader(dataReader, false),
                    parameters,
                    SQLConstant.ShortTimeoutSeconds)));

            records.AddRange(sqlRetryPolicy.Execute(() =>
                SqlQueryHelper.ExecuteStoredProcedureWithListResult(ParameterizedLog(_logger), _connectionString,
                    SQLConstant.StoredProcedureName.MOBILITY_DEVICE_CUSTOMER_CHARGE_QUEUE_GET_CHARGE_LIST,
                    (dataReader) => DeviceRecordFromReader(dataReader, false),
                    parameters,
                    SQLConstant.ShortTimeoutSeconds)));

            return records;
        }

        public IEnumerable<RevIOCommon.DeviceCustomerChargeQueueRecord> GetChargeList(int fileId)
        {
            var records = new List<RevIOCommon.DeviceCustomerChargeQueueRecord>();

            var sqlRetryPolicy = GetSqlTransientRetryPolicy(_logger);

            var parameters = new List<SqlParameter>()
            {
                new SqlParameter(CommonSQLParameterNames.FILE_ID, fileId)
            };
            records.AddRange(sqlRetryPolicy.Execute(() =>
              SqlQueryHelper.ExecuteStoredProcedureWithListResult(ParameterizedLog(_logger), _connectionString,
                  SQLConstant.StoredProcedureName.DEVICE_CUSTOMER_CHARGE_QUEUE_GET_CHARGE_LIST,
                  (dataReader) => DeviceRecordFromReader(dataReader, false, true),
                  parameters,
                  SQLConstant.ShortTimeoutSeconds)));

            records.AddRange(sqlRetryPolicy.Execute(() =>
             SqlQueryHelper.ExecuteStoredProcedureWithListResult(ParameterizedLog(_logger), _connectionString,
                 SQLConstant.StoredProcedureName.MOBILITY_DEVICE_CUSTOMER_CHARGE_QUEUE_GET_CHARGE_LIST,
                 (dataReader) => DeviceRecordFromReader(dataReader, false, true),
                 parameters,
                 SQLConstant.ShortTimeoutSeconds)));

            return records;
        }

        private static CustomerChargeUploadedFile UploadedFileFromReader(IDataRecord rdr)
        {
            return new CustomerChargeUploadedFile
            {
                Id = int.Parse(rdr["Id"].ToString()),
                FileName = rdr["FileName"].ToString(),
                Status = rdr["Status"].ToString(),
                ProcessedDate = rdr["ProcessedDate"] == DBNull.Value
                    ? new DateTime?()
                    : Convert.ToDateTime(rdr["ProcessedDate"].ToString()),
                DeletedDate = rdr["DeletedDate"] == DBNull.Value
                    ? new DateTime?()
                    : Convert.ToDateTime(rdr["DeletedDate"].ToString()),
                CreatedDate = rdr["CreatedDate"] == DBNull.Value
                    ? new DateTime()
                    : Convert.ToDateTime(rdr["CreatedDate"].ToString()),
                ModifiedDate = rdr["ModifiedDate"] == DBNull.Value
                    ? new DateTime?()
                    : Convert.ToDateTime(rdr["ModifiedDate"].ToString()),
                Description = rdr["Description"].ToString(),
                CreatedBy = rdr["CreatedBy"].ToString(),
                DeletedBy = rdr["DeletedBy"].ToString(),
                ModifiedBy = rdr["ModifiedBy"].ToString(),
                ProcessedBy = rdr["ProcessedBy"].ToString(),
                IsActive = bool.Parse(rdr["IsActive"].ToString()),
                IsDeleted = bool.Parse(rdr["IsDeleted"].ToString()),
                IntegrationAuthenticationId = rdr["IntegrationAuthenticationId"] == DBNull.Value
                    ? new int?()
                    : Convert.ToInt32(rdr["IntegrationAuthenticationId"].ToString())
            };
        }

        private static RevIOCommon.DeviceCustomerChargeQueueRecord DeviceRecordFromReader(SqlDataReader dataReader, bool includeExtendedFields, bool isFile = false)
        {
            var columns = dataReader.GetColumnsFromReader();
            var record = new RevIOCommon.DeviceCustomerChargeQueueRecord
            {
                Id = dataReader.LongFromReader(columns, CommonColumnNames.Id),
                ICCID = dataReader.StringFromReader(columns, CommonColumnNames.ICCID),
                MSISDN = dataReader.StringFromReader(columns, CommonColumnNames.MSISDN),
                UsageMB = dataReader.DecimalFromReader(columns, CommonColumnNames.UsageMB),
                RatePlanCode = dataReader.StringFromReader(columns, CommonColumnNames.RatePlanCode),
                RatePlanName = dataReader.StringFromReader(columns, CommonColumnNames.RatePlanName),
                BaseRate = dataReader.DecimalFromReader(columns, CommonColumnNames.BaseRate),
                _3GSurcharge = dataReader.DecimalFromReader(columns, CommonColumnNames.Surcharge3G),
                PlanMB = dataReader.DecimalFromReader(columns, CommonColumnNames.PlanMB),
                IsProcessed = dataReader.BooleanFromReader(columns, CommonColumnNames.IsProcessed),
                ChargeId = dataReader.IntFromReader(columns, CommonColumnNames.ChargeId),
                ChargeAmount = dataReader.DecimalFromReader(columns, CommonColumnNames.ChargeAmount),
                ModifiedDate = dataReader.DateTimeFromReader(columns, CommonColumnNames.ModifiedDate, true),
                RateChargeAmount = dataReader.DecimalFromReader(columns, CommonColumnNames.RateChargeAmt),
                DisplayRate = dataReader.DecimalFromReader(columns, CommonColumnNames.DisplayRate),
                DataPerOverageCharge = dataReader.DecimalFromReader(columns, CommonColumnNames.DataPerOverageCharge),
                OverageRateCost = dataReader.DecimalFromReader(columns, CommonColumnNames.OverageRateCost),
                RevProductTypeId = dataReader.IntFromReader(columns, CommonColumnNames.RevProductTypeId),
                RevServiceNumber = dataReader.StringFromReader(columns, CommonColumnNames.RevServiceNumber),
                HasErrors = dataReader.BooleanFromReader(columns, CommonColumnNames.HasErrors),
                ErrorMessage = dataReader.StringFromReader(columns, CommonColumnNames.ErrorMessage),
                SmsRevProductTypeId = dataReader.IntFromReader(columns, CommonColumnNames.SmsRevProductTypeId),
                SmsChargeAmount = dataReader.DecimalFromReader(columns, CommonColumnNames.SmsChargeAmount),
                SmsChargeId = dataReader.IntFromReader(columns, CommonColumnNames.SmsChargeId),
                SmsRate = dataReader.DecimalFromReader(columns, CommonColumnNames.SmsRate),
                SmsUsage = dataReader.IntFromReader(columns, CommonColumnNames.SmsUsage),
                CalculatedBaseRate = dataReader.DecimalFromReader(columns, CommonColumnNames.BaseRateAmount),
                ServiceProviderId = dataReader.IntFromReader(columns, CommonColumnNames.ServiceProviderId),
                RevProductId = dataReader.IntFromReader(columns, CommonColumnNames.RevProductId),
                SmsRevProductId = dataReader.IntFromReader(columns, CommonColumnNames.SmsRevProductId)
            };
            if (!isFile)
            {
                record.IsBillInAdvance = dataReader.BooleanFromReader(columns, CommonColumnNames.IsBillInAdvance);
                record.CalculatedOverageCharge = dataReader.DecimalFromReader(columns, CommonColumnNames.OverageChargeAmount);
                record.CalculatedRateCharge = dataReader.DecimalFromReader(columns, CommonColumnNames.RateChargeAmount);
                record.OverageRevProductTypeId = dataReader.IntFromReader(columns, CommonColumnNames.OverageRevProductTypeId);
                record.OverageRevProductId = dataReader.IntFromReader(columns, CommonColumnNames.OverageRevProductId);
            }
            if (includeExtendedFields)
            {
                record.Description = dataReader.StringFromReader(columns, CommonColumnNames.Description);
                record.BillingStartDate = dataReader.DateTimeFromReader(columns, CommonColumnNames.BillingStartDate, true);
                record.BillingEndDate = dataReader.DateTimeFromReader(columns, CommonColumnNames.BillingEndDate, true);
            }

            return record;
        }

        public List<CustomerChargeQueueOfInstance> GetQueueIsNeedSendMailSumary(string InstanceIds, int portalTypeId)
        {
            _logger.LogInfo("SUB", $"GetQueueIsNeedSendMailSumary({InstanceIds})");

            List<CustomerChargeQueueOfInstance> result = new List<CustomerChargeQueueOfInstance>();
            using (var Conn = new SqlConnection(_connectionString))
            {
                // Need to be change
                using (var Cmd = new SqlCommand("dbo.usp_CustomerCharge_GetAllQueueId_ByInstanceIds", Conn))
                {
                    Cmd.CommandType = CommandType.StoredProcedure;
                    Cmd.Parameters.AddWithValue("@InstanceIds", InstanceIds);
                    Cmd.Parameters.AddWithValue("@portalTypeId", portalTypeId);
                    Cmd.CommandTimeout = 900;

                    Conn.Open();

                    SqlDataReader rdr = Cmd.ExecuteReader();
                    while (rdr.Read())
                    {
                        var customerChargeQueue = CustomerChargeQueueOfInstanceFromReader(rdr);
                        result.Add(customerChargeQueue);
                    }
                    Conn.Close();
                }
            }
            return result;
        }
        private static CustomerChargeQueueOfInstance CustomerChargeQueueOfInstanceFromReader(IDataRecord rdr)
        {
            var record = new CustomerChargeQueueOfInstance
            {
                OptimizationId = int.Parse(rdr["OptimizationId"].ToString()),
                QueueId = int.Parse(rdr["QueueId"].ToString()),
                BillingPeriodStartDate = rdr["BillingPeriodStartDate"] == DBNull.Value ? new DateTime() : Convert.ToDateTime(rdr["BillingPeriodStartDate"].ToString()),
                BillingPeriodEndDate = rdr["BillingPeriodEndDate"] == DBNull.Value ? new DateTime() : Convert.ToDateTime(rdr["BillingPeriodEndDate"].ToString()),
                RevCustomerId = rdr["RevCustomerId"] == DBNull.Value ? Guid.Empty : new Guid(rdr["RevCustomerId"].ToString()),
                AMOPCustomerId = rdr["AMOPCustomerId"] == DBNull.Value ? default(int?) : int.Parse(rdr["AMOPCustomerId"].ToString()),
            };

            return record;
        }

        public IEnumerable<RevIOCommon.RevProductType> GetProductTypeList()
        {
            var records = new List<RevProductType>();
            var sqlCommand = @"SELECT [ProductTypeId], [Description] FROM RevProductType WHERE IsActive = 1 AND IsDeleted = 0";
            using (var Conn = new SqlConnection(_connectionString))
            {
                using (var Cmd = new SqlCommand(sqlCommand, Conn))
                {
                    Cmd.CommandType = CommandType.Text;
                    Conn.Open();

                    SqlDataReader rdr = Cmd.ExecuteReader();
                    while (rdr.Read())
                    {
                        var deviceRecord = new RevProductType
                        {
                            product_type_id = rdr["ProductTypeId"] == DBNull.Value ? 0 : int.Parse(rdr["ProductTypeId"].ToString()),
                            description = rdr["Description"] == DBNull.Value ? string.Empty : rdr["Description"].ToString(),
                        };
                        records.Add(deviceRecord);
                    }
                    Conn.Close();
                }
            }
            return records;
        }
        public bool VerifyAnyInstanceStillInProgress(string sessionId, int portalTypeId, bool isNonRev)
        {
            var listInstanceInProgress = new List<int>();
            try
            {
                _logger.LogInfo(CommonConstants.SUB, $"");

                using (var connection = new SqlConnection(_connectionString))
                {
                    using (var command = new SqlCommand(SQLConstant.StoredProcedureName.CUSTOMER_CHARGE_GET_ALL_INSTANCES_IN_PROGRESS_BY_SESSION_ID, connection))
                    {
                        command.CommandType = CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("@sessionId", sessionId);
                        command.Parameters.AddWithValue("@portalTypeId", portalTypeId);
                        command.Parameters.AddWithValue("@isNonRev", isNonRev);
                        command.CommandTimeout = SQLConstant.ShortTimeoutSeconds;

                        connection.Open();

                        SqlDataReader dataReader = command.ExecuteReader();
                        while (dataReader.Read())
                        {
                            var instanceId = int.Parse(dataReader[CommonColumnNames.InstanceId].ToString());
                            listInstanceInProgress.Add(instanceId);
                        }
                    }
                }
            }
            catch (SqlException ex)
            {
                _logger.LogInfo(CommonConstants.EXCEPTION, string.Format(LogCommonStrings.EXCEPTION_WHEN_EXECUTING_SQL_COMMAND, ex.Message));
            }
            catch (InvalidOperationException ex)
            {
                _logger.LogInfo(CommonConstants.EXCEPTION, string.Format(LogCommonStrings.EXCEPTION_WHEN_CONNECTING_DATABASE, ex.Message));
            }
            catch (Exception ex)
            {
                _logger.LogInfo(CommonConstants.EXCEPTION, ex.Message);
            }
            _logger.LogInfo(CommonConstants.INFO, string.Format(LogCommonStrings.ITEMS_IN_PROGRESS, listInstanceInProgress.Count));
            return listInstanceInProgress.Count > 0;
        }

        public int CountAllM2MItemInQueue(long queueId, bool isNonRev = false)
        {
            int count = 0;
            var sqlRetryPolicy = GetSqlTransientRetryPolicy(_logger);
            var storedProcedureName = isNonRev ?
                SQLConstant.StoredProcedureName.COUNT_BY_QUEUE_ID_VW_OPTIMIZATION_DEVICE_RESULT_CUSTOMER_CHARGE_QUEUE_NON_REV
                :
                SQLConstant.StoredProcedureName.COUNT_BY_QUEUE_ID_VW_OPTIMIZATION_DEVICE_RESULT_CUSTOMER_CHARGE_QUEUE;
            var parameters = new List<SqlParameter>()
            {
                new SqlParameter(CommonSQLParameterNames.QUEUE_ID, queueId)
            };
            sqlRetryPolicy.Execute(() =>
            {
                count = SqlQueryHelper.ExecuteStoredProcedureWithIntResult(ParameterizedLog(_logger),
                _connectionString,
                storedProcedureName,
                parameters,
                SQLConstant.ShortTimeoutSeconds);
            });

            return count;
        }

        public int CountAllMobilityItemInQueue(long queueId, bool isNonRev = false)
        {
            int count = 0;
            var sqlRetryPolicy = GetSqlTransientRetryPolicy(_logger);
            var storedProcedureName = isNonRev ?
                SQLConstant.StoredProcedureName.COUNT_BY_QUEUE_ID_VW_OPTIMIZATION_MOBILITY_DEVICE_RESULT_CUSTOMER_CHARGE_QUEUE_NON_REV
                :
                SQLConstant.StoredProcedureName.COUNT_BY_QUEUE_ID_VW_OPTIMIZATION_MOBILITY_DEVICE_RESULT_CUSTOMER_CHARGE_QUEUE;
            var parameters = new List<SqlParameter>()
            {
                new SqlParameter(CommonSQLParameterNames.QUEUE_ID, queueId)
            };
            sqlRetryPolicy.Execute(() =>
            {
                count = SqlQueryHelper.ExecuteStoredProcedureWithIntResult(ParameterizedLog(_logger),
                _connectionString,
                storedProcedureName,
                parameters,
                SQLConstant.ShortTimeoutSeconds);
            });

            return count;
        }

        public int CountAllM2MItemInFile(int fileId, bool isNonRev = false)
        {
            int count = 0;
            var sqlRetryPolicy = GetSqlTransientRetryPolicy(_logger);
            var storedProcedureName = isNonRev ?
                SQLConstant.StoredProcedureName.COUNT_BY_QUEUE_ID_VW_OPTIMIZATION_DEVICE_RESULT_CUSTOMER_CHARGE_QUEUE_NON_REV
                :
                SQLConstant.StoredProcedureName.COUNT_BY_QUEUE_ID_VW_OPTIMIZATION_DEVICE_RESULT_CUSTOMER_CHARGE_QUEUE;
            var parameters = new List<SqlParameter>()
            {
                new SqlParameter(CommonSQLParameterNames.FILE_ID, fileId)
            };
            sqlRetryPolicy.Execute(() =>
            {
                count = SqlQueryHelper.ExecuteStoredProcedureWithIntResult(ParameterizedLog(_logger),
                _connectionString,
                storedProcedureName,
                parameters,
                SQLConstant.ShortTimeoutSeconds);
            });

            return count;
        }

        public int CountAllMobilityItemInFile(int fileId, bool isNonRev = false)
        {
            int count = 0;
            var sqlRetryPolicy = GetSqlTransientRetryPolicy(_logger);
            var storedProcedureName = isNonRev ?
                SQLConstant.StoredProcedureName.COUNT_BY_QUEUE_ID_VW_OPTIMIZATION_MOBILITY_DEVICE_RESULT_CUSTOMER_CHARGE_QUEUE_NON_REV
                :
                SQLConstant.StoredProcedureName.COUNT_BY_QUEUE_ID_VW_OPTIMIZATION_MOBILITY_DEVICE_RESULT_CUSTOMER_CHARGE_QUEUE;
            var parameters = new List<SqlParameter>()
            {
                new SqlParameter(CommonSQLParameterNames.FILE_ID, fileId)
            };
            sqlRetryPolicy.Execute(() =>
            {
                count = SqlQueryHelper.ExecuteStoredProcedureWithIntResult(ParameterizedLog(_logger),
                _connectionString,
                storedProcedureName,
                parameters,
                SQLConstant.ShortTimeoutSeconds);
            });

            return count;
        }

        public async Task EnqueueCheckCustomerChargesIsProcessedAsync(long queueId, int portalTypeId, string instanceIds, bool isMultipleInstance, bool isLastInstanceId, int currentIntegrationAuthenticationId = 0, bool IsSendSummaryEmailForMultipleInstanceStep = false, int customDelayTime = 0, int retryNumber = 0)
        {
            _logger.LogInfo(CommonConstants.SUB, $"{nameof(queueId)}: {queueId}");
            _logger.LogInfo(CommonConstants.SUB, $"{nameof(IsSendSummaryEmailForMultipleInstanceStep)}: {IsSendSummaryEmailForMultipleInstanceStep}");
            _logger.LogInfo(CommonConstants.SUB, $"{nameof(retryNumber)}: {retryNumber}");

            var awsCredentials = _settingsRepository.GetGeneralProviderSettings().AwsCredentials;
            using (var client = new AmazonSQSClient(awsCredentials, RegionEndpoint.USEast1))
            {
                var requestMsgBody = string.Format(LogCommonStrings.QUEUE_TO_WORK, queueId);
                var request = new SendMessageRequest
                {
                    DelaySeconds = customDelayTime,
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        {
                            SQSMessageKeyConstant.QUEUE_ID, new MessageAttributeValue
                                {DataType = CommonConstants.STRING, StringValue = queueId.ToString()}
                        },
                        {
                            SQSMessageKeyConstant.IS_MULTIPLE_INSTANCE_ID, new MessageAttributeValue
                            { DataType = CommonConstants.STRING, StringValue = Convert.ToInt32(isMultipleInstance).ToString()}
                        },
                        {
                            SQSMessageKeyConstant.IS_LAST_INSTANCE_ID, new MessageAttributeValue
                            { DataType = CommonConstants.STRING, StringValue =  Convert.ToInt32(isLastInstanceId).ToString()}
                        },
                        {
                            SQSMessageKeyConstant.INSTANCE_IDS, new MessageAttributeValue
                            { DataType = CommonConstants.STRING, StringValue = instanceIds}
                        },
                        {
                            SQSMessageKeyConstant.PORTAL_TYPE_ID, new MessageAttributeValue
                            { DataType = CommonConstants.STRING, StringValue = portalTypeId.ToString()}
                        },
                        {
                            SQSMessageKeyConstant.CURRENT_INTEGRATION_AUTHENTICATION_ID, new MessageAttributeValue
                            { DataType = CommonConstants.STRING, StringValue = currentIntegrationAuthenticationId.ToString()}
                        },
                        {
                            SQSMessageKeyConstant.IS_SEND_SUMMARY_EMAIL_FOR_MULTIPLE_INSTANCE_STEP, new MessageAttributeValue
                            { DataType = CommonConstants.STRING, StringValue =  Convert.ToInt32(IsSendSummaryEmailForMultipleInstanceStep).ToString()}
                        },
                        {
                            SQSMessageKeyConstant.RETRY_NUMBER, new MessageAttributeValue
                            {DataType = CommonConstants.STRING, StringValue = retryNumber.ToString()}
                        },
                    },

                    MessageBody = requestMsgBody,
                    QueueUrl = _environmentRepository.GetEnvironmentVariable(_context, SQSMessageKeyConstant.CUSTOMER_CHARGE_CHECK_IS_PROCESSED_URL)
                };

                var response = await client.SendMessageAsync(request);
                if ((response.HttpStatusCode < HttpStatusCode.OK) || (response.HttpStatusCode >= HttpStatusCode.Ambiguous))
                {
                    _logger.LogInfo(CommonConstants.EXCEPTION, string.Format(LogCommonStrings.ERROR_ENQUEUING, queueId, response.HttpStatusCode, response.HttpStatusCode));
                }
            }
        }

        public async Task EnqueueCheckCustomerChargesIsProcessedAsync(int fileId, int currentIntegrationAuthenticationId = 0, int retryNumber = 0)
        {
            _logger.LogInfo(CommonConstants.SUB, $"{nameof(fileId)}: {fileId}");

            var awsCredentials = _settingsRepository.GetGeneralProviderSettings().AwsCredentials;
            using (var client = new AmazonSQSClient(awsCredentials, RegionEndpoint.USEast1))
            {
                var requestMsgBody = string.Format(LogCommonStrings.FILE_TO_WORK, fileId);
                var request = new SendMessageRequest
                {
                    DelaySeconds = DelaySeconds,
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        {
                            SQSMessageKeyConstant.FILE_ID, new MessageAttributeValue
                                {DataType = CommonConstants.STRING, StringValue = fileId.ToString()}
                        },
                        {
                            SQSMessageKeyConstant.CURRENT_INTEGRATION_AUTHENTICATION_ID, new MessageAttributeValue
                                { DataType = CommonConstants.STRING, StringValue = currentIntegrationAuthenticationId.ToString()}
                        },
                        {
                            SQSMessageKeyConstant.RETRY_NUMBER, new MessageAttributeValue
                            {DataType = CommonConstants.STRING, StringValue = retryNumber.ToString()}
                        },
                    },
                    MessageBody = requestMsgBody,
                    QueueUrl = _environmentRepository.GetEnvironmentVariable(_context, SQSMessageKeyConstant.CUSTOMER_CHARGE_CHECK_IS_PROCESSED_URL)
                };

                var response = await client.SendMessageAsync(request);
                if ((response.HttpStatusCode < HttpStatusCode.OK) || (response.HttpStatusCode >= HttpStatusCode.Ambiguous))
                {
                    _logger.LogInfo(CommonConstants.EXCEPTION, string.Format(LogCommonStrings.ERROR_ENQUEUING, fileId, response.HttpStatusCode, response.HttpStatusCode));
                }
            }
        }

        public static Action<string, string> ParameterizedLog(IKeysysLogger logger)
        {
            return (type, message) => logger.LogInfo(type, message);
        }
        private static ISyncPolicy GetSqlTransientRetryPolicy(IKeysysLogger logger)
        {
            var policyFactory = new PolicyFactory(logger);
            return policyFactory.GetSqlRetryPolicy(CommonConstants.NUMBER_OF_RETRIES);
        }
    }
}

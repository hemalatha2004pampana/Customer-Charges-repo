using Altaworx.AWS.Core;
using Altaworx.AWS.Core.Helpers;
using Altaworx.AWS.Core.Models;
using Altaworx.AWS.Core.Repositories.Customer;
using AltaworxRevAWSCreateCustomerChange.Models;
using AltaworxRevAWSCreateCustomerChange.Repositories.DeviceCustomerCharge;
using AltaworxRevAWSCreateCustomerChange.Services.ChargeList;
using Amazon.Lambda.Core;
using Amop.Core.Constants;
using Amop.Core.Logger;
using Amop.Core.Models.Revio;
using Amop.Core.Repositories.Environment;
using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

namespace AltaworxRevAWSCheckCustomerChargeIsProcessed.Services
{
    public class CheckIsProcessedService : ICheckIsProcessedService
    {
        private readonly ILambdaContext _context;
        private readonly ICustomerChargeListEmailService _customerChargeListEmailService;
        private readonly IDeviceCustomerChargeQueueRepository _customerChargeQueueRepository;
        private readonly IKeysysLogger _logger;
        private readonly ICustomerChargeListFileService _chargeListFileService;
        private readonly IEnvironmentRepository _environmentRepository;
        private readonly IS3Wrapper _s3Wrapper;
        private readonly ICustomerRepository _customerRepository;

        public CheckIsProcessedService(ILambdaContext context, ICustomerChargeListEmailService customerChargeListEmailService, IDeviceCustomerChargeQueueRepository customerChargeQueueRepository, IKeysysLogger logger, ICustomerChargeListFileService chargeListFileService, IEnvironmentRepository environmentRepository, IS3Wrapper s3Wrapper, ICustomerRepository customerRepository)
        {
            _context = context;
            _customerChargeListEmailService = customerChargeListEmailService;
            _customerChargeQueueRepository = customerChargeQueueRepository;
            _logger = logger;
            _chargeListFileService = chargeListFileService;
            _environmentRepository = environmentRepository;
            _s3Wrapper = s3Wrapper;
            _customerRepository = customerRepository;
        }

        public async Task ProcessQueueAsync(long queueId, OptimizationInstance instance, SqsValues sqsValues)
        {
            var connectionString = _environmentRepository.GetEnvironmentVariable(_context, SQSMessageKeyConstant.CONNECTION_STRING);
            var serviceProviderList = ServiceProviderCommon.GetServiceProviders(connectionString);
            var proxyUrl = _environmentRepository.GetEnvironmentVariable(_context, SQSMessageKeyConstant.PROXY_URL);
            var bucketName = _environmentRepository.GetEnvironmentVariable(_context, SQSMessageKeyConstant.CUSTOMER_CHARGES_S3_BUCKET_NAME);
            var isNonRevCustomer = instance.AMOPCustomerId != null && instance.RevCustomerId == null && instance.IntegrationAuthenticationId == null;

            if (!_customerChargeQueueRepository.QueueHasMoreItems(queueId, isNonRevCustomer))
            {
                // get charge list
                var chargeList = _customerChargeQueueRepository.GetChargeList(queueId)?.ToList();

                // create charge list file and save to S3
                var fileName = $"{queueId}.txt";
                var chargeListFileBytes = _chargeListFileService.GenerateChargeListFile(chargeList, instance.BillingPeriodStartDate,
                    instance.BillingPeriodEndDate, serviceProviderList);

                _s3Wrapper.UploadAwsFile(chargeListFileBytes, fileName);

                var statusUploadFileToS3 = _s3Wrapper.WaitForFileUploadCompletion(fileName, CommonConstants.DELAY_IN_SECONDS_FIVE_MINUTES, _logger);

                var statusUploadFile = (isUploadSucces: statusUploadFileToS3.Result.Item1, errorMessage: statusUploadFileToS3.Result.Item2);
                if (statusUploadFile.isUploadSucces)
                {
                    // Send Summary Email with charge list file
                    var errorCount = chargeList?.Count(x => x.HasErrors) ?? 0;
                    if (!sqsValues.IsMultipleInstanceId)
                    {
                        await _customerChargeListEmailService.SendEmailSummaryAsync(queueId, instance, chargeListFileBytes, fileName, errorCount, isNonRevCustomer);
                    }
                    else if (sqsValues.IsLastInstanceId)
                    {
                        await ProcessSendEmailSummaryForMultipleInstanceStep(sqsValues, instance, proxyUrl, bucketName, queueId, isNonRev: isNonRevCustomer);
                    }
                }
                else
                {
                    _logger.LogInfo(CommonConstants.EXCEPTION, string.Format(LogCommonStrings.UPLOAD_FILE_TO_S3_NOT_SUCCESS, $"{queueId}.txt"));
                }
            }
            else
            {
                if (sqsValues.RetryNumber > CommonConstants.NUMBER_OF_RETRIES)
                {
                    _logger.LogInfo(CommonConstants.EXCEPTION, string.Format(LogCommonStrings.ERROR_QUEUE_CANNOT_CHECK_CREATE_CUSTOMER_CHARGE_IS_PROCESSED, queueId));
                }
                else
                {
                    var retryNumber = sqsValues.RetryNumber + 1;
                    await _customerChargeQueueRepository.EnqueueCheckCustomerChargesIsProcessedAsync(queueId, sqsValues.PortalTypeId, sqsValues.InstanceIds, sqsValues.IsMultipleInstanceId, sqsValues.IsLastInstanceId, customDelayTime: CommonConstants.DELAY_IN_SECONDS_FIFTEEN_MINUTES, retryNumber: retryNumber);
                }
            }
        }

        public async Task ProcessQueueAsync(int fileId, SqsValues sqsValues)
        {
            var connectionString = _environmentRepository.GetEnvironmentVariable(_context, SQSMessageKeyConstant.CONNECTION_STRING);
            var serviceProviderList = ServiceProviderCommon.GetServiceProviders(connectionString);
            var file = _customerChargeQueueRepository.GetUploadedFile(fileId);

            if (!_customerChargeQueueRepository.QueueHasMoreItems(fileId))
            {
                // get charge list
                var chargeList = _customerChargeQueueRepository.GetChargeList(fileId)?.ToList();

                // create charge list file and save to S3
                var fileName = $"{fileId}.txt";
                var chargeListFileBytes = _chargeListFileService.GenerateChargeListFile(chargeList, serviceProviderList);
                _s3Wrapper.UploadAwsFile(chargeListFileBytes, fileName);

                var statusUploadFileToS3 = _s3Wrapper.WaitForFileUploadCompletion(fileName, CommonConstants.DELAY_IN_SECONDS_FIVE_MINUTES, _logger);

                var statusUploadFile = (isUploadSucces: statusUploadFileToS3.Result.Item1, errorMessage: statusUploadFileToS3.Result.Item2);
                if (statusUploadFile.isUploadSucces)
                {
                    // Send Summary Email with charge list file
                    var errorCount = chargeList?.Count(x => x.HasErrors) ?? 0;
                    await _customerChargeListEmailService.SendEmailSummaryAsync(file, chargeListFileBytes, fileName,
                        errorCount);
                }
                else
                {
                    _logger.LogInfo(CommonConstants.EXCEPTION, string.Format(LogCommonStrings.UPLOAD_FILE_TO_S3_NOT_SUCCESS, $"{fileId}.txt"));
                }
            }
            else
            {
                if (sqsValues.RetryNumber > CommonConstants.NUMBER_OF_RETRIES)
                {
                    _logger.LogInfo(CommonConstants.EXCEPTION, string.Format(LogCommonStrings.ERROR_FILE_CANNOT_CHECK_CREATE_CUSTOMER_CHARGE_IS_PROCESSED, fileId));
                }
                else
                {
                    var retryNumber = sqsValues.RetryNumber + 1;
                    await _customerChargeQueueRepository.EnqueueCheckCustomerChargesIsProcessedAsync(fileId, sqsValues.CurrentIntegrationAuthenticationId, retryNumber);
                }
            }

        }

        private async Task ProcessSendEmailSummaryForMultipleInstanceStep(SqsValues sqsValues, OptimizationInstance instance, string proxyUrl, string bucketName, long queueId, bool isNonRev)
        {
            //check another instance process completed or not
            var isAnyInstanceInProgress = _customerChargeQueueRepository.VerifyAnyInstanceStillInProgress(instance.OptimizationSessionId.ToString(), sqsValues.PortalTypeId, isNonRev);
            if (!isAnyInstanceInProgress || sqsValues.RetryNumber > CommonConstants.NUMBER_OF_RETRIES)
            {
                //If all instances completed. send email summary
                SendMailSummaryCustomerChargeForMultipleInstance(sqsValues, instance, proxyUrl, bucketName, isNonRev);
            }
            else
            {
                if (sqsValues.RetryNumber > CommonConstants.NUMBER_OF_RETRIES)
                {
                    _logger.LogInfo(CommonConstants.EXCEPTION, string.Format(LogCommonStrings.ERROR_FILE_CANNOT_CHECK_CREATE_CUSTOMER_CHARGE_IS_PROCESSED, queueId));
                }
                else
                {
                    var retryNumber = sqsValues.RetryNumber + 1;
                    await _customerChargeQueueRepository.EnqueueCheckCustomerChargesIsProcessedAsync(queueId, sqsValues.PortalTypeId, sqsValues.InstanceIds, sqsValues.IsMultipleInstanceId, sqsValues.IsLastInstanceId, customDelayTime: CommonConstants.DELAY_IN_SECONDS_FIFTEEN_MINUTES, retryNumber: retryNumber);
                }
            }
        }

        private void SendMailSummaryCustomerChargeForMultipleInstance(SqsValues sqsValues, OptimizationInstance instance, string proxyUrl, string bucketName, bool isNonRev)
        {
            var customerChargeQueueIdList = _customerChargeQueueRepository.GetQueueIsNeedSendMailSumary(sqsValues.InstanceIds, sqsValues.PortalTypeId);
            _logger.LogInfo(CommonConstants.INFO, $"Last step has {customerChargeQueueIdList.Count} queue.");
            if (customerChargeQueueIdList.Count > 0)
            {
                var lstCustomer = new List<RevCustomerModel>();
                if (isNonRev)
                {
                    var customerIds = customerChargeQueueIdList.Select(item => item.AMOPCustomerId ?? 0).Distinct().ToList();
                    lstCustomer.AddRange(_customerRepository.GetNonRevCustomers(customerIds));
                }
                else
                {
                    var revCustomerGuidIds = customerChargeQueueIdList.Select(item => item.RevCustomerId ?? Guid.Empty).Distinct().ToList();
                    lstCustomer.AddRange(_customerRepository.GetCustomers(revCustomerGuidIds));
                }

                var jsonContent = new RevCustomerChargeEmailModel
                {
                    customerChargeQueueIdList = customerChargeQueueIdList,
                    lstCustomer = lstCustomer,
                    TenantId = instance.TenantId,
                    IsNonRev = isNonRev,
                    BucketName = bucketName

                };

                using (var client = new HttpClient())
                {
                    if (!string.IsNullOrWhiteSpace(proxyUrl))
                    {
                        var payload = new PayloadModel()
                        {
                            JsonContent = JsonConvert.SerializeObject(jsonContent),
                            Password = null,
                            Token = null,
                            Username = null,
                            IsOptCustomerSendEmail = false
                        };
                        var requestSendEmailResult = client.CustomerChargeSendEmailProxy(proxyUrl, payload, _logger);
                        _logger.LogInfo(CommonConstants.INFO, $"{requestSendEmailResult.ResponseMessage}");
                    }
                }
            }
        }
    }
}

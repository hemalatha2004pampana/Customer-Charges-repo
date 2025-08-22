using Altaworx.AWS.Core;
using Altaworx.AWS.Core.Helpers;
using Altaworx.AWS.Core.Models;
using Altaworx.AWS.Core.Repositories.Customer;
using AltaworxRevAWSCreateCustomerChange.Models;
using AltaworxRevAWSCreateCustomerChange.Repositories.DeviceCustomerCharge;
using AltaworxRevAWSCreateCustomerChange.Services.ChargeList;
using Amazon.Lambda.Core;
using Amazon.SQS.Model;
using Amazon.SQS;
using Amop.Core.Constants;
using Amop.Core.Logger;
using Amop.Core.Models.Revio;
using Amop.Core.Models.Settings;
using Amop.Core.Repositories.Environment;
using Amop.Core.Repositories.Revio;
using Amop.Core.Services.Revio;
using Newtonsoft.Json;
using OfficeOpenXml;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using static Altaworx.AWS.Core.RevIOCommon;
using Amazon;
using Polly;
using System.Net;
using System.Collections;
using Altaworx.AWS.Core.Services;
using MimeKit;
using System.Text.RegularExpressions;

namespace AltaworxRevAWSCreateCustomerChange.Services.DeviceCustomerCharge
{
    public class DeviceCustomerChargeService : IDeviceCustomerChargeService
    {
        private readonly ICustomerRepository _customerRepository;
        private readonly ICustomerChargeListFileService _chargeListFileService;
        private readonly ILambdaContext _context;
        private readonly ICustomerChargeListEmailService _customerChargeListEmailService;
        private readonly IDeviceCustomerChargeQueueRepository _customerChargeQueueRepository;
        private readonly IDeviceChargeRepository _deviceChargeRepository;
        private readonly IEnvironmentRepository _environmentRepository;
        private readonly IKeysysLogger _logger;
        private readonly RevioAuthenticationRepository _revIoAuthenticationRepository;
        private readonly IS3Wrapper _s3Wrapper;
        private readonly ISettingsRepository _settingsRepository;
        private readonly string SUMMARY_FILENAME = "CustomerChargeSummary.zip";
        private readonly string EXCEL_FORMAT = ".xlsx";
        private readonly string TXT_FORMAT = ".txt";
        private readonly RevioApiClient _revApiClient;
        private readonly int PAGE_SIZE = 50;
        private const int DELAY_SECONDS = 10;
        private readonly IEmailSender _emailSender;
        private readonly GeneralProviderSettings _settings;
        public DeviceCustomerChargeService(IKeysysLogger logger,
            IDeviceCustomerChargeQueueRepository customerChargeQueueRepository,
            RevioAuthenticationRepository revIoAuthenticationRepository, IEnvironmentRepository environmentRepository,
            ILambdaContext context, ISettingsRepository settingsRepository, ICustomerChargeListFileService chargeListFileService,
            IS3Wrapper s3Wrapper, ICustomerChargeListEmailService customerChargeListEmailService, IDeviceChargeRepository deviceChargeRepository,
            ICustomerRepository customerRepository, RevioApiClient revioApiClient, IEmailSender emailSender, GeneralProviderSettings settings)
        {
            _logger = logger;
            _customerChargeQueueRepository = customerChargeQueueRepository;
            _revIoAuthenticationRepository = revIoAuthenticationRepository;
            _environmentRepository = environmentRepository;
            _context = context;
            _settingsRepository = settingsRepository;
            _chargeListFileService = chargeListFileService;
            _s3Wrapper = s3Wrapper;
            _customerChargeListEmailService = customerChargeListEmailService;
            _deviceChargeRepository = deviceChargeRepository;
            _customerRepository = customerRepository;
            _revApiClient = revioApiClient;
            _emailSender = emailSender;
            _settings = settings;
        }

        public async Task ProcessQueueAsync(long queueId, OptimizationInstance instance, SqsValues sqsValues)
        {
            _logger.LogInfo(CommonConstants.SUB, $"{nameof(queueId)}: {queueId}");
            _logger.LogInfo(CommonConstants.SUB, $"{nameof(instance)}: {instance.Id}");
            _logger.LogInfo(CommonConstants.SUB, $"{nameof(sqsValues.IsMultipleInstanceId)}: {sqsValues.IsMultipleInstanceId}");
            _logger.LogInfo(CommonConstants.SUB, $"{nameof(sqsValues.IsLastInstanceId)}: {sqsValues.IsLastInstanceId}");
            _logger.LogInfo(CommonConstants.SUB, $"{nameof(sqsValues.InstanceIds)}: {sqsValues.InstanceIds}");
            _logger.LogInfo(CommonConstants.SUB, $"{nameof(sqsValues.IsSendSummaryEmailForMultipleInstanceStep)}: {sqsValues.IsSendSummaryEmailForMultipleInstanceStep}");
            _logger.LogInfo(CommonConstants.SUB, $"{nameof(sqsValues.PageNumber)}: {sqsValues.PageNumber}");

            var offset = (sqsValues.PageNumber - 1) * PAGE_SIZE;
            // instance is non-rev customer
            var isNonRevCustomer = instance.AMOPCustomerId != null && instance.RevCustomerId == null && instance.IntegrationAuthenticationId == null;
            var connectionString = _environmentRepository.GetEnvironmentVariable(_context, SQSMessageKeyConstant.CONNECTION_STRING);
            var proxyUrl = _environmentRepository.GetEnvironmentVariable(_context, SQSMessageKeyConstant.PROXY_URL);
            var bucketName = _environmentRepository.GetEnvironmentVariable(_context, SQSMessageKeyConstant.CUSTOMER_CHARGES_S3_BUCKET_NAME);
            var serviceProviderList = ServiceProviderCommon.GetServiceProviders(connectionString);

            // get next devices to update
            var deviceList = _customerChargeQueueRepository.GetDeviceList(queueId, PAGE_SIZE, offset, isNonRevCustomer).Where(x => x.IsProcessed == false).ToList();
            if (deviceList.Count() == 0)
            {
                _logger.LogInfo(CommonConstants.INFO, string.Format(LogCommonStrings.ALL_ITEMS_OF_QUEUE_HAS_BEEN_PROCESSED, queueId));
                return;
            }
            if (isNonRevCustomer)
            {
                // Process Customer Charge for non rev
                await ProcessCustomerChargeForNonRev(queueId, instance, sqsValues, deviceList, serviceProviderList, proxyUrl, bucketName, offset);
            }
            else
            {
                // Process Customer Charge for rev 
                await ProcessCustomerChargeForRev(queueId, instance, sqsValues, deviceList, serviceProviderList, proxyUrl, bucketName, offset);
            }
        }
        private async Task ProcessCustomerChargeForNonRev(long queueId, OptimizationInstance instance, SqsValues sqsValues, IEnumerable<RevIOCommon.DeviceCustomerChargeQueueRecord> deviceLists, List<ServiceProvider> serviceProviders, string proxyUrl, string bucketName, int offset)
        {
            var optimizationSettings = _settingsRepository.GetOptimizationSettings();
            var billingTimeZone = optimizationSettings?.BillingTimeZone;
            var chargeId = 0;
            var smsChargeId = 0;
            var hasErrors = false;
            var errorMessage = string.Empty;
            foreach (var device in deviceLists)
            {
                _customerChargeQueueRepository.MarkRecordProcessed(device.Id, chargeId.ToString(), device.DeviceCharge,
                    device.BaseRate,
                    device.DeviceCharge + device.BaseRate, hasErrors, errorMessage,
                    smsChargeId.ToString(), device.SmsChargeAmount);
            }

            var totalPage = CalculateTotalPageInQueue(queueId, PAGE_SIZE, true);
            await MultipleEnqueueCustomerChargesAsync(queueId, sqsValues, totalPage, true);

            if (sqsValues.PageNumber == totalPage)
            {
                // Enqueue run check is processed
                await _customerChargeQueueRepository.EnqueueCheckCustomerChargesIsProcessedAsync(queueId, sqsValues.PortalTypeId, sqsValues.InstanceIds, sqsValues.IsMultipleInstanceId, sqsValues.IsLastInstanceId);
            }
        }
        private async Task ProcessCustomerChargeForRev(long queueId, OptimizationInstance instance, SqsValues sqsValues, IEnumerable<RevIOCommon.DeviceCustomerChargeQueueRecord> deviceList, List<ServiceProvider> serviceProviders, string proxyUrl, string bucketName, int offset)
        {
            if (instance.IntegrationAuthenticationId.HasValue)
            {
                var revIoAuth = _revIoAuthenticationRepository.GetRevioApiAuthentication(instance.IntegrationAuthenticationId.Value);
                var optimizationSettings = _settingsRepository.GetOptimizationSettings();
                var billingTimeZone = optimizationSettings?.BillingTimeZone;
                var useNewLogicCustomerCharge = (bool)optimizationSettings?.UsingNewProcessInCustomerCharge;
                await ProcessDeviceList(deviceList, queueId, sqsValues, instance, revIoAuth, billingTimeZone, serviceProviders, useNewLogicCustomerCharge);
            }
            else
            {
                _logger.LogInfo(CommonConstants.EXCEPTION, LogCommonStrings.NO_INTEGRATION_AUTHENTICATION_ID_PROVIDED);
                return;
            }

            var totalPage = CalculateTotalPageInQueue(queueId, PAGE_SIZE, false);
            await MultipleEnqueueCustomerChargesAsync(queueId, sqsValues, totalPage, false);

            if (sqsValues.PageNumber == totalPage)
            {
                // Enqueue run check is processed
                await _customerChargeQueueRepository.EnqueueCheckCustomerChargesIsProcessedAsync(queueId, sqsValues.PortalTypeId, sqsValues.InstanceIds, sqsValues.IsMultipleInstanceId, sqsValues.IsLastInstanceId, sqsValues.CurrentIntegrationAuthenticationId);
            }
        }

        private void AddFileToZip(ZipArchive zipArchive, string nameFile, DataSet dataSet)
        {
            var fileBytes = GenerateExcelFileWithDataSet(dataSet);
            var file = zipArchive.CreateEntry($"{nameFile}{EXCEL_FORMAT}");
            using (var entryStream = file.Open())
            {
                using (var streamWriter = new StreamWriter(entryStream))
                {
                    streamWriter.BaseStream.Write(fileBytes, 0, fileBytes.Length);
                    streamWriter.Flush();
                    streamWriter.Close();
                }
            }
        }

        private byte[] GenerateExcelFileWithDataSet(DataSet dataSet)
        {
            byte[] fileByte;
            using (ExcelPackage package = new ExcelPackage())
            {
                for (int index = 0; index < dataSet.Tables.Count; index++)
                {
                    var worksheet = package.Workbook.Worksheets.Add(dataSet.Tables[index].ToString());
                    worksheet.Cells["A1"].LoadFromDataTable(dataSet.Tables[index], true);
                }
                fileByte = package.GetAsByteArray();
            }
            return fileByte;
        }

        private void BuildDataSetHeader(DataSet dataSet, int index, string title)
        {
            dataSet.Tables.Add(title);
            dataSet.Tables[index].Columns.Add("Customer");
            dataSet.Tables[index].Columns.Add("MSISDN");
            dataSet.Tables[index].Columns.Add("IsSuccessful");
            dataSet.Tables[index].Columns.Add("ChargeId");
            dataSet.Tables[index].Columns.Add("ChargeAmount");
            dataSet.Tables[index].Columns.Add("BillingPeriodStart");
            dataSet.Tables[index].Columns.Add("BillingPeriodEnd");
            dataSet.Tables[index].Columns.Add("DateCharged");
            dataSet.Tables[index].Columns.Add("ErrorMessage");
        }

        private List<string> PrepareTextData(string text)
        {
            var result = new List<string>();
            result = text.Split("\n").Where(item => !string.IsNullOrEmpty(item)).ToList();
            result.RemoveAt(0);
            result.RemoveAt(result.Count - 1);
            return result;
        }

        private float ApplyLstTextToDataSet(List<string> lstText, DataTable collectionDetail, DataTable collectionSummary, string customerName)
        {
            CultureInfo culture = new CultureInfo("en-US");
            culture.NumberFormat.NumberDecimalSeparator = ".";
            float amount = 0;
            foreach (var txtItem in lstText)
            {
                var valueList = txtItem.Split("\t");
                var drDetail = collectionDetail.NewRow();
                drDetail[0] = customerName;
                drDetail[1] = valueList[0];
                drDetail[2] = valueList[1];
                drDetail[3] = valueList[2];
                drDetail[4] = valueList[3];
                drDetail[5] = valueList[4];
                drDetail[6] = valueList[5];
                drDetail[7] = valueList[6];
                drDetail[8] = valueList[7];
                collectionDetail.Rows.Add(drDetail);

                var drSum = collectionSummary.NewRow();
                drSum[0] = customerName;
                drSum[1] = valueList[0];
                drSum[2] = valueList[1];
                drSum[3] = valueList[2];
                drSum[4] = valueList[3];
                drSum[5] = valueList[4];
                drSum[6] = valueList[5];
                drSum[7] = valueList[6];
                drSum[8] = valueList[7];
                collectionSummary.Rows.Add(drSum);
                amount += float.Parse(valueList[3], culture);
            }

            // add row summary
            var summaryRow = collectionDetail.NewRow();
            summaryRow[4] = amount;
            collectionDetail.Rows.Add(summaryRow);
            return amount;
        }

        private byte[] ConvertTxtToExcel(string text, string customerName)
        {
            byte[] fileByte;
            using (ExcelPackage package = new ExcelPackage())
            {
                DataSet dataSet = new DataSet();
                dataSet.Tables.Add("Summary");
                dataSet.Tables[0].Columns.Add("Customer");
                dataSet.Tables[0].Columns.Add("MSISDN");
                dataSet.Tables[0].Columns.Add("IsSuccessful");
                dataSet.Tables[0].Columns.Add("ChargeId");
                dataSet.Tables[0].Columns.Add("ChargeAmount");
                dataSet.Tables[0].Columns.Add("BillingPeriodStart");
                dataSet.Tables[0].Columns.Add("BillingPeriodEnd");
                dataSet.Tables[0].Columns.Add("DateCharged");
                dataSet.Tables[0].Columns.Add("ErrorMessage");

                var textList = text.Split("\n").ToList();
                string head = textList.FirstOrDefault();
                textList.RemoveAt(0); //remove head
                textList.RemoveAt(textList.Count - 1); //remove head
                foreach (var txtItem in textList.Select((line, index) => new { line, index }))
                {
                    var valueList = txtItem.line.Split("\t");
                    var dr = dataSet.Tables[0].NewRow();
                    dr[0] = txtItem.index == textList.Count - 1 ? "" : customerName;
                    dr[1] = valueList[0];
                    dr[2] = valueList[1];
                    dr[3] = valueList[2];
                    dr[4] = valueList[3];
                    dr[5] = valueList[4];
                    dr[6] = valueList[5];
                    dr[7] = valueList[6];
                    dr[8] = valueList[7];
                    dataSet.Tables[0].Rows.Add(dr);
                }
                // create the worksheet
                var worksheet = package.Workbook.Worksheets.Add("Sheet1");
                worksheet.Cells["A1"].LoadFromDataTable(dataSet.Tables[0], true);
                fileByte = package.GetAsByteArray();
            }
            return fileByte;
        }

        private async Task ProcessDeviceList(IEnumerable<RevIOCommon.DeviceCustomerChargeQueueRecord> deviceList, long queueId, SqsValues sqsValues, OptimizationInstance instance, RevioApiAuthentication revIoAuth, TimeZoneInfo billingTimeZone, List<ServiceProvider> serviceProviders, bool useNewLogicCustomerCharge = false)
        {
            _logger.LogInfo("INFO", $"Use New Logic For Customer Charge: {useNewLogicCustomerCharge.ToString()}");
            bool retryFlag = false;
            List<DeviceCustomerChargeQueueRecord> errorDeviceList = new List<DeviceCustomerChargeQueueRecord>();
            foreach (var device in deviceList)
            {
                //New logic - Split Chagre To Rate Charge And Overage Charge
                if (useNewLogicCustomerCharge)
                {
                    if (device.DeviceCharge > 0.0M)
                    {
                        if (device.CalculatedRateCharge > 0.0M)
                        {
                            if (device.RevProductTypeId != null || device.RevProductId != null)
                            {
                                retryFlag = await ProcessDevice(device, instance, revIoAuth, serviceProviders, billingTimeZone, useNewLogicCustomerCharge, true, false);
                            }
                            else
                            {
                                _logger.LogInfo("EXCEPTION", $"RevProductTypeId For '{device.MSISDN}' Has Not Been Setup");
                            }
                        }
                        if (device.CalculatedOverageCharge > 0.0M)
                        {
                            if (device.OverageRevProductTypeId != null || device.OverageRevProductId != null)
                            {
                                retryFlag = await ProcessDevice(device, instance, revIoAuth, serviceProviders, billingTimeZone, useNewLogicCustomerCharge, false, true);
                            }
                            else
                            {
                                _logger.LogInfo("EXCEPTION", $"OverageRevProductTypeId For '{device.MSISDN}' Has Not Been Setup");
                            }
                        }
                    }
                    if (device.SmsChargeAmount > 0.0M && (device.SmsRevProductTypeId != null || device.SmsRevProductId != null))
                    {
                        retryFlag = await ProcessDevice(device, instance, revIoAuth, serviceProviders, billingTimeZone, useNewLogicCustomerCharge, false, false, true);
                    }
                    if (device.DeviceCharge <= 0.0M && device.SmsChargeAmount <= 0.0M)
                    {
                        _customerChargeQueueRepository.MarkRecordProcessed(device.Id, "-1", device.DeviceCharge, device.BaseRate,
                            device.DeviceCharge + device.BaseRate, false, string.Empty, "-1", device.SmsChargeAmount);
                    }
                }
                else
                {
                    //Old logic - Single Usage Charge
                    retryFlag = await ProcessDevice(device, instance, revIoAuth, serviceProviders, billingTimeZone, useNewLogicCustomerCharge);
                }
                if (retryFlag)
                {
                    errorDeviceList.Add(device);
                }
            }
            //Enqueueing the same Lambda if there are any errors
            if (retryFlag)
            {
                await SendErrorEmailNotificationAsync(errorDeviceList);
                if (sqsValues.RetryCount <= CommonConstants.MAX_RETRY_COUNT)
                {
                    _logger.LogInfo("INFO", $"Retry Count: {sqsValues.RetryCount}");
                    await _customerChargeQueueRepository.EnqueueCustomerChargesAsync(queueId, sqsValues.PortalTypeId, sqsValues.InstanceIds, sqsValues.IsMultipleInstanceId, sqsValues.IsLastInstanceId, sqsValues.PageNumber, sqsValues.CurrentIntegrationAuthenticationId, false, 0, sqsValues.RetryCount + 1);
                }
                else if (sqsValues.RetryCount > CommonConstants.MAX_RETRY_COUNT)
                {
                    _logger.LogInfo("INFO", $"Retry Count reached maximum: {sqsValues.RetryCount}");
                    var errorMessage = LogCommonStrings.ERROR_WHEN_AT_FINAL_RETRY;
                    foreach (var device in errorDeviceList)
                    {
                        _customerChargeQueueRepository.MarkRecordProcessed(device.Id, "-1", device.DeviceCharge, device.BaseRate,
                           device.DeviceCharge + device.BaseRate, true, errorMessage, "-1", device.SmsChargeAmount);
                    }
                }
            }
        }
        public async Task<bool> ProcessDevice(DeviceCustomerChargeQueueRecord device, OptimizationInstance instance, RevioApiAuthentication revIoAuth, List<ServiceProvider> serviceProviders, TimeZoneInfo billingTimeZone, bool useNewLogicCustomerCharge, bool isRateCharge = false, bool isOverageCharge = false, bool isSMSCharge = false)
        {
            // add customer charge to rev.io
            var chargeId = 0;
            var smsChargeId = 0;
            var hasErrors = false;
            var errorMessage = string.Empty;
            var integrationId = 0;
            var statusCode = 0;
            if (serviceProviders.Count > 0)
            {
                integrationId = serviceProviders.FirstOrDefault(x => x.Id == device.ServiceProviderId).IntegrationId;
            }
            if (string.IsNullOrWhiteSpace(device.RatePlanCode))
            {
                hasErrors = true;
                errorMessage = LogCommonStrings.REV_RATE_PLAN_NOT_FOUND;
                // mark item processed with errors when there is error - rate plan not found
                _customerChargeQueueRepository.MarkRecordProcessed(device.Id, chargeId.ToString(), device.DeviceCharge,
                    device.BaseRate,
                    device.DeviceCharge + device.BaseRate, hasErrors, errorMessage,
                    smsChargeId.ToString(), device.SmsChargeAmount);
                return false;
            }
            else
            {
                if (Convert.ToBoolean(_environmentRepository.GetEnvironmentVariable(_context, "SendToRev")))
                {
                    _logger.LogInfo(CommonConstants.INFO, LogCommonStrings.SEND_CHARGE_TO_REV_IS_ENABLED);
                    if (!isSMSCharge && device.DeviceCharge > 0.0M)
                    {
                        var customerChargeResponse = await
                            AddCustomerUsageChargeAsync(device, instance, billingTimeZone, integrationId, useNewLogicCustomerCharge, isRateCharge, isOverageCharge);
                        if (customerChargeResponse != null)
                        {
                            chargeId = customerChargeResponse.ChargeId;
                            hasErrors = customerChargeResponse.HasErrors;
                            statusCode = customerChargeResponse.StatusCode;
                            errorMessage = customerChargeResponse.ErrorMessage + Environment.NewLine;
                        }
                    }
                    else
                    {
                        chargeId = -1;
                    }

                    //for old logic
                    if (!useNewLogicCustomerCharge && device.SmsChargeAmount > 0.0M &&
                        (device.SmsRevProductTypeId != null || device.SmsRevProductId != null))
                    {
                        isSMSCharge = true;
                    }

                    if (isSMSCharge && !device.IsBillInAdvance && device.SmsChargeAmount > 0.0M &&
                        (device.SmsRevProductTypeId != null || device.SmsRevProductId != null))
                    {
                        var customerChargeResponse = await
                            AddCustomerSmsChargeAsync(device, integrationId, instance,
                                billingTimeZone, useNewLogicCustomerCharge);
                        if (customerChargeResponse != null)
                        {
                            smsChargeId = customerChargeResponse.ChargeId;
                            hasErrors = hasErrors || customerChargeResponse.HasErrors;
                            errorMessage += customerChargeResponse.ErrorMessage;
                        }
                    }
                    else
                    {
                        smsChargeId = -1;
                    }
                }
                else
                {
                    _logger.LogInfo(CommonConstants.WARNING, LogCommonStrings.SEND_CHARGE_TO_REV_IS_DISABLED);
                }
            }

            if (!hasErrors && statusCode != (int)HttpStatusCode.TooManyRequests)
            {
                // mark item processed
                _customerChargeQueueRepository.MarkRecordProcessed(device.Id, chargeId.ToString(), device.DeviceCharge,
                    device.BaseRate,
                    device.DeviceCharge + device.BaseRate, hasErrors, errorMessage,
                    smsChargeId.ToString(), device.SmsChargeAmount);
                return false;
            }
            else
            {
                return true;
            }
        }

        public async Task ProcessQueueAsync(int fileId, SqsValues sqsValues)
        {
            _logger.LogInfo(CommonConstants.SUB, $"({fileId})");

            var offset = (sqsValues.PageNumber - 1) * PAGE_SIZE;
            var connectionString = _environmentRepository.GetEnvironmentVariable(_context, SQSMessageKeyConstant.CONNECTION_STRING);

            // get next devices to update
            var file = _customerChargeQueueRepository.GetUploadedFile(fileId);
            var deviceList = _customerChargeQueueRepository.GetDeviceList(fileId, PAGE_SIZE, offset).Where(x => x.IsProcessed == false).ToList();
            var revIoAuth = _revIoAuthenticationRepository.GetRevioApiAuthentication(file.IntegrationAuthenticationId.GetValueOrDefault(0));
            var serviceProviderList = ServiceProviderCommon.GetServiceProviders(connectionString);
            var chargeId = 0;
            var smsChargeId = 0;
            var hasErrors = false;
            var errorMessage = string.Empty;
            var integrationId = 0;
            var statusCode = 0;
            DeviceCustomerChargeQueueRecord deviceDetails = new DeviceCustomerChargeQueueRecord();
            List<DeviceCustomerChargeQueueRecord> errorDeviceList = new List<DeviceCustomerChargeQueueRecord>();
            foreach (var device in deviceList)
            {
                deviceDetails = device;
                // add customer charge to rev.io
                if (serviceProviderList.Count > 0)
                {
                    integrationId = serviceProviderList.FirstOrDefault(x => x.Id == device.ServiceProviderId).IntegrationId;
                }

                if (Convert.ToBoolean(_environmentRepository.GetEnvironmentVariable(_context, SQSMessageKeyConstant.SEND_TO_REV)))
                {
                    _logger.LogInfo(CommonConstants.INFO, LogCommonStrings.SEND_CHARGE_TO_REV_IS_ENABLED);
                    if (device.DeviceCharge > 0.0M && (device.RevProductTypeId != null || device.RevProductId != null))
                    {
                        var customerChargeResponse = await
                            AddCustomerUsageChargeAsync(device, integrationId);
                        chargeId = customerChargeResponse.ChargeId;
                        hasErrors = customerChargeResponse.HasErrors;
                        statusCode = customerChargeResponse.StatusCode;
                        errorMessage = customerChargeResponse.ErrorMessage + Environment.NewLine;
                    }
                    else
                    {
                        chargeId = -1;
                    }

                    if (device.SmsChargeAmount > 0.0M && (device.SmsRevProductTypeId != null || device.SmsRevProductId != null))
                    {
                        var customerChargeResponse = await
                            AddCustomerSmsChargeAsync(device, integrationId);
                        if (customerChargeResponse != null)
                        {
                            smsChargeId = customerChargeResponse.ChargeId;
                            hasErrors = hasErrors || customerChargeResponse.HasErrors;
                            errorMessage += customerChargeResponse.ErrorMessage;
                        }
                    }
                    else
                    {
                        smsChargeId = -1;
                    }
                }
                else
                {
                    _logger.LogInfo(CommonConstants.WARNING, LogCommonStrings.SEND_CHARGE_TO_REV_IS_DISABLED);
                }
                if (hasErrors)
                {
                    errorDeviceList.Add(device);
                }
                // mark item processed
                _customerChargeQueueRepository.MarkRecordProcessed(device.Id, chargeId.ToString(), device.DeviceCharge,
                    device.BaseRate,
                    device.DeviceCharge + device.BaseRate, hasErrors, errorMessage,
                    smsChargeId.ToString(), device.SmsChargeAmount);
            }

            if (!hasErrors)
            {   // mark item processed
                _customerChargeQueueRepository.MarkRecordProcessed(deviceDetails.Id, chargeId.ToString(), deviceDetails.DeviceCharge,
                    deviceDetails.BaseRate,
                    deviceDetails.DeviceCharge + deviceDetails.BaseRate, hasErrors, errorMessage,
                    smsChargeId.ToString(), deviceDetails.SmsChargeAmount);
            }
            //Enqueueing the same Lambda if there are any errors
            else
            {
                if (errorDeviceList.Count > 0)
                {
                    await SendErrorEmailNotificationAsync(errorDeviceList);
                }
                if (sqsValues.RetryCount > 0 && sqsValues.RetryCount < CommonConstants.MAX_RETRY_COUNT)
                {
                    await _customerChargeQueueRepository.EnqueueCustomerChargesAsync(fileId, sqsValues.PageNumber, sqsValues.CurrentIntegrationAuthenticationId, sqsValues.RetryCount + 1);
                }
                else if (sqsValues.RetryCount > CommonConstants.MAX_RETRY_COUNT)
                {
                    foreach (var device in deviceList)
                    {
                        _customerChargeQueueRepository.MarkRecordProcessed(device.Id, "-1", device.DeviceCharge, device.BaseRate,
                           device.DeviceCharge + device.BaseRate, true, LogCommonStrings.ERROR_WHEN_UPLOADING_CHARGE_AT_FINAL_RETRY, "-1", device.SmsChargeAmount);
                    }
                }
            }
            // Get total page and enqueue all pages
            var totalPage = CalculateTotalPageInFile(fileId, PAGE_SIZE);
            if (totalPage > 1 && sqsValues.PageNumber == 1)
            {
                for (var pageNumber = 2; pageNumber <= totalPage; pageNumber++)
                {
                    await _customerChargeQueueRepository.EnqueueCustomerChargesAsync(fileId, pageNumber, sqsValues.CurrentIntegrationAuthenticationId);
                }
            }

            if (sqsValues.PageNumber == totalPage)
            {
                // Enqueue Check Is Processed
                await _customerChargeQueueRepository.EnqueueCheckCustomerChargesIsProcessedAsync(fileId, sqsValues.CurrentIntegrationAuthenticationId);
            }
        }

        private async Task<CustomerChargeResponse> AddCustomerUsageChargeAsync(
            RevIOCommon.DeviceCustomerChargeQueueRecord device, OptimizationInstance instance,
            TimeZoneInfo billingTimeZone, int integrationId, bool useNewLogicCustomerCharge = false, bool isRateCharge = false, bool isOverageCharge = false)
        {
            _logger.LogInfo(CommonConstants.SUB, $"({device.Id},{instance.Id})");

            var (revService, statusCode) = await LookupRevServiceAsync(device);
            if (revService != null)
            {
                return await AddRevCustomerUsageChargeAsync(device, revService, instance, billingTimeZone, integrationId, useNewLogicCustomerCharge, isRateCharge, isOverageCharge);
            }

            var response = new CustomerChargeResponse
            {
                ChargeId = 0,
                HasErrors = true,
                StatusCode = statusCode,
                ErrorMessage = statusCode == CommonConstants.TOO_MANY_REQUEST_HTTP_STATUS_CODE
                            ? string.Format(LogCommonStrings.ERROR_WHEN_LOOKUP_REV_SERVICE_AT_FINAL_RETRY, device.MSISDN)
                            : string.Format(LogCommonStrings.SERVICE_RECORD_NOT_FOUND_FOR_DEVICE, device.MSISDN)
            };
            return response;
        }

        private async Task<CustomerChargeResponse> AddCustomerUsageChargeAsync(RevIOCommon.DeviceCustomerChargeQueueRecord device, int integrationId)
        {
            _logger.LogInfo(CommonConstants.SUB, $"({device.Id})");

            var (revService, statusCode) = await LookupRevServiceAsync(device);
            if (revService != null)
            {
                return await AddRevCustomerUsageChargeAsync(device, revService, integrationId);
            }

            var response = new CustomerChargeResponse
            {
                ChargeId = 0,
                HasErrors = true,
                StatusCode = statusCode,
                ErrorMessage = statusCode == CommonConstants.TOO_MANY_REQUEST_HTTP_STATUS_CODE
                           ? string.Format(LogCommonStrings.ERROR_WHEN_LOOKUP_REV_SERVICE_AT_FINAL_RETRY, device.MSISDN)
                           : string.Format(LogCommonStrings.SERVICE_RECORD_NOT_FOUND_FOR_DEVICE, device.MSISDN)
            };
            return response;
        }

        private async Task<CustomerChargeResponse> AddCustomerSmsChargeAsync(
            RevIOCommon.DeviceCustomerChargeQueueRecord device, int integrationId, OptimizationInstance instance,
            TimeZoneInfo billingTimeZone, bool useNewLogicCustomerCharge = false)
        {
            _logger.LogInfo(CommonConstants.SUB, $"({device.Id},{instance.Id})");

            var (revService, statusCode) = await LookupRevServiceAsync(device);
            if (revService != null)
            {
                return await AddRevCustomerSmsChargeAsync(device, revService, integrationId, instance, billingTimeZone, useNewLogicCustomerCharge);
            }

            var response = new CustomerChargeResponse
            {
                ChargeId = 0,
                HasErrors = true,
                ErrorMessage = statusCode == CommonConstants.TOO_MANY_REQUEST_HTTP_STATUS_CODE
                           ? string.Format(LogCommonStrings.ERROR_WHEN_LOOKUP_REV_SERVICE_AT_FINAL_RETRY, device.MSISDN)
                           : string.Format(LogCommonStrings.SERVICE_RECORD_NOT_FOUND_FOR_DEVICE, device.MSISDN)
            };
            return response;
        }

        private async Task<CustomerChargeResponse> AddCustomerSmsChargeAsync(RevIOCommon.DeviceCustomerChargeQueueRecord device, int integrationId)
        {
            _logger.LogInfo(CommonConstants.SUB, $"({device.Id})");

            var (revService, statusCode) = await LookupRevServiceAsync(device);
            if (revService != null)
            {
                return await AddRevCustomerSmsChargeAsync(device, revService, integrationId);
            }

            var response = new CustomerChargeResponse
            {
                ChargeId = 0,
                HasErrors = true,
                ErrorMessage = statusCode == CommonConstants.TOO_MANY_REQUEST_HTTP_STATUS_CODE
                            ? string.Format(LogCommonStrings.ERROR_WHEN_LOOKUP_REV_SERVICE_AT_FINAL_RETRY, device.MSISDN)
                            : string.Format(LogCommonStrings.SERVICE_RECORD_NOT_FOUND_FOR_DEVICE, device.MSISDN)
            };
            return response;
        }

        private async Task<CustomerChargeResponse> AddRevCustomerUsageChargeAsync(
            RevIOCommon.DeviceCustomerChargeQueueRecord device, RevIOCommon.RevService revService,
            OptimizationInstance instance, TimeZoneInfo billingTimeZone, int integrationId, bool useNewLogicCustomerCharge = false, bool isRateCharge = false, bool isOverageCharge = false)
        {
            if (useNewLogicCustomerCharge)
            {
                if (isRateCharge)
                {
                    var requestRateCharge = new RevIOCommon.CreateDeviceChargeRequest(device, revService, instance.BillingPeriodStartDate, instance.BillingPeriodEndDate, billingTimeZone, integrationId, false, useNewLogicCustomerCharge, false, true);
                    var responseRateCharge = await _deviceChargeRepository.AddChargeAsync(requestRateCharge);
                    if (responseRateCharge == null || responseRateCharge.HasErrors)
                    {
                        _logger.LogInfo(CommonConstants.EXCEPTION, string.Format(LogCommonStrings.DEVICE_CHARGE_NOT_UPDATE, device.MSISDN));
                    }
                    return responseRateCharge;
                }
                if (isOverageCharge)
                {
                    var requestOverCharge = new RevIOCommon.CreateDeviceChargeRequest(device, revService, instance.BillingPeriodStartDate, instance.BillingPeriodEndDate, billingTimeZone, integrationId, false, useNewLogicCustomerCharge, true, false);
                    var responseOverCharge = await _deviceChargeRepository.AddChargeAsync(requestOverCharge);
                    if (responseOverCharge == null || responseOverCharge.HasErrors)
                    {
                        _logger.LogInfo(CommonConstants.EXCEPTION, string.Format(LogCommonStrings.DEVICE_CHARGE_NOT_UPDATE, device.MSISDN));
                    }
                    return responseOverCharge;
                }
            }
            else
            {
                //old logic
                var request = new RevIOCommon.CreateDeviceChargeRequest(device, revService, instance.BillingPeriodStartDate,
               instance.BillingPeriodEndDate, billingTimeZone, integrationId);
                var response = await _deviceChargeRepository.AddChargeAsync(request);
                if (response == null || response.HasErrors)
                {
                    _logger.LogInfo(CommonConstants.EXCEPTION, string.Format(LogCommonStrings.DEVICE_CHARGE_NOT_UPDATE, device.MSISDN));
                }
                return response;
            }
            return new CustomerChargeResponse
            { ChargeId = 0, HasErrors = true, ErrorMessage = string.Format(LogCommonStrings.DEVICE_CHARGE_NOT_UPDATE, device.MSISDN) };
        }

        private async Task<CustomerChargeResponse> AddRevCustomerUsageChargeAsync(
            RevIOCommon.DeviceCustomerChargeQueueRecord device, RevIOCommon.RevService revService, int integrationId)
        {
            var request = new RevIOCommon.CreateDeviceChargeRequest(device, revService, integrationId);
            var response = await _deviceChargeRepository.AddChargeAsync(request);
            if (response == null || response.HasErrors)
            {
                _logger.LogInfo(CommonConstants.EXCEPTION, string.Format(LogCommonStrings.DEVICE_CHARGE_NOT_UPDATE, device.MSISDN));
            }

            return response;
        }

        private async Task<CustomerChargeResponse> AddRevCustomerSmsChargeAsync(
            RevIOCommon.DeviceCustomerChargeQueueRecord device, RevIOCommon.RevService revService, int integrationId,
            OptimizationInstance instance, TimeZoneInfo billingTimeZone, bool useNewLogicCustomerCharge = false)
        {
            var request = new RevIOCommon.CreateDeviceChargeRequest(device, revService, instance.BillingPeriodStartDate,
                instance.BillingPeriodEndDate, billingTimeZone, integrationId, true, useNewLogicCustomerCharge);
            var response = await _deviceChargeRepository.AddChargeAsync(request);
            if (response == null || response.HasErrors)
            {
                _logger.LogInfo(CommonConstants.EXCEPTION, string.Format(LogCommonStrings.DEVICE_SMS_CHARGE_NOT_UPDATE, device.MSISDN));
            }

            return response;
        }

        private async Task<CustomerChargeResponse> AddRevCustomerSmsChargeAsync(
            RevIOCommon.DeviceCustomerChargeQueueRecord device, RevIOCommon.RevService revService, int integrationId)
        {
            var request = new RevIOCommon.CreateDeviceChargeRequest(device, revService, integrationId, true);
            var response = await _deviceChargeRepository.AddChargeAsync(request);
            if (response == null || response.HasErrors)
            {
                _logger.LogInfo(CommonConstants.EXCEPTION, string.Format(LogCommonStrings.DEVICE_SMS_CHARGE_NOT_UPDATE, device.MSISDN));
            }

            return response;
        }

        public async Task<Tuple<RevService, int>> LookupRevServiceAsync(RevIOCommon.DeviceCustomerChargeQueueRecord device)
        {
            var serviceNumber = !string.IsNullOrWhiteSpace(device.RevServiceNumber) ? device.RevServiceNumber : device.MSISDN;
            var response = new RevServiceList();
            //should not call API when serviceNumber is null
            if (!string.IsNullOrWhiteSpace(serviceNumber))
            {
                response = await _revApiClient.GetServicesAsync<RevServiceList>(serviceNumber, _logger);
            }
            if (response == null || !response.OK ||
             response.RecordCount < 1 ||
             response.Records == null ||
             response.Records.Count < 1)
            {
                if (response.StatusCode == CommonConstants.TOO_MANY_REQUEST_HTTP_STATUS_CODE)
                {
                    return Tuple.Create<RevService, int>(null, response.StatusCode);
                }
                return Tuple.Create<RevService, int>(null, 0);
            }
            return Tuple.Create<RevService, int>(response.Records.Where(x => !string.IsNullOrWhiteSpace(x.ActivatedDate) && DateTime.Parse(x.ActivatedDate) != DateTime.MinValue)
                .OrderByDescending(x => x.ServiceId).FirstOrDefault(), 0);
        }

        private int CalculateTotalPageInQueue(long queueId, int PAGE_SIZE, bool isNonRev = false)
        {
            var m2mTotalRecord = _customerChargeQueueRepository.CountAllM2MItemInQueue(queueId, isNonRev);
            var mobilityTotalRecord = _customerChargeQueueRepository.CountAllMobilityItemInQueue(queueId, isNonRev);
            var totalRecord = mobilityTotalRecord;
            if (m2mTotalRecord > mobilityTotalRecord)
            {
                totalRecord = m2mTotalRecord;
            }

            return (int)Math.Ceiling((double)totalRecord / PAGE_SIZE);
        }

        private int CalculateTotalPageInFile(int fileId, int PAGE_SIZE, bool isNonRev = false)
        {
            var m2mTotalRecord = _customerChargeQueueRepository.CountAllM2MItemInFile(fileId, isNonRev);
            var mobilityTotalRecord = _customerChargeQueueRepository.CountAllMobilityItemInFile(fileId, isNonRev);
            var totalRecord = mobilityTotalRecord;
            if (m2mTotalRecord > mobilityTotalRecord)
            {
                totalRecord = m2mTotalRecord;
            }

            return (int)Math.Ceiling((double)totalRecord / PAGE_SIZE);
        }

        public async Task MultipleEnqueueCustomerChargesAsync(long queueId, SqsValues sqsValues, int totalPage, bool isNonRev = false)
        {
            if (totalPage > 1 && sqsValues.PageNumber == 1)
            {
                for (var pageNumber = 2; pageNumber <= totalPage; pageNumber++)
                {
                    await (isNonRev ?
                            _customerChargeQueueRepository.EnqueueCustomerChargesAsync(queueId, sqsValues.PortalTypeId, sqsValues.InstanceIds, sqsValues.IsMultipleInstanceId, sqsValues.IsLastInstanceId, pageNumber)
                            :
                            _customerChargeQueueRepository.EnqueueCustomerChargesAsync(queueId, sqsValues.PortalTypeId, sqsValues.InstanceIds, sqsValues.IsMultipleInstanceId, sqsValues.IsLastInstanceId, pageNumber, sqsValues.CurrentIntegrationAuthenticationId)
                            );
                }
            }
        }
        private async Task SendErrorEmailNotificationAsync(List<DeviceCustomerChargeQueueRecord> errorDeviceList)
        {
            _logger.LogInfo("SUB", "SendErrorEmailNotificationAsync()");

            var fromAddress = _settings.CustomerChargeFromEmailAddress;
            var toAddresses = _settings.CustomerChargeToEmailAddresses.Split(';');
            var subject = $"Charges Upload Error Summary";
            var errorDeviceBodyText = "";
            var errorBodyText = "<dd>- For Customer Number - {0}, ICCID - {1}, Desciption - {2}, Charges - {3}</dd>";
            foreach (var device in errorDeviceList)
            {
                errorDeviceBodyText += string.Format(errorBodyText, device.RevAccountNumber, device.ICCID, device.Description, device.ChargeAmount);
            }
            var body = BuildUploadErrorEmailBody(errorDeviceBodyText);
            await _emailSender.SendEmailAsync(fromAddress, toAddresses, subject, body);
        }

        private static BodyBuilder BuildUploadErrorEmailBody(string errorDeviceBodyText)
        {
            var htmlBody = $@"<html>
                    <h1>Customer Charge Upload Error</h1>
                    <h2>An error occurred while uploading the charges or the retry attempts were exhausted.</h2>
                    <div><dl><dt>Below are the list of Device Details which has Errors</dt>{errorDeviceBodyText}</dl></div>
                </html>";
            var textBody = Regex.Replace(htmlBody, "<[^>]*>", string.Empty);
            return new BodyBuilder
            {
                HtmlBody = htmlBody,
                TextBody = textBody
            };
        }

    }
}

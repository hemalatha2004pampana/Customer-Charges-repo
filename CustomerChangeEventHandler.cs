using System;
using System.Threading.Tasks;
using Altaworx.AWS.Core.Repositories.OptimizationInstance;
using Altaworx.AWS.Core.Repositories.OptimizationQueue;
using AltaworxRevAWSCreateCustomerChange.Models;
using AltaworxRevAWSCreateCustomerChange.Services.DeviceCustomerCharge;
using Amazon.Lambda.SQSEvents;
using Amop.Core.Constants;
using Amop.Core.Logger;

namespace AltaworxRevAWSCreateCustomerChange.EventHandlers
{
    public class CustomerChangeEventHandler
    {
        private readonly IDeviceCustomerChargeService _deviceCustomerChargeService;
        private readonly IKeysysLogger _logger;
        private readonly IOptimizationInstanceRepository _optimizationInstanceRepository;
        private readonly IOptimizationQueueRepository _optimizationQueueRepository;

        public CustomerChangeEventHandler(IKeysysLogger logger,
            IOptimizationQueueRepository optimizationQueueRepository,
            IOptimizationInstanceRepository optimizationInstanceRepository,
            IDeviceCustomerChargeService deviceCustomerChargeService)
        {
            _logger = logger;
            _optimizationQueueRepository = optimizationQueueRepository;
            _optimizationInstanceRepository = optimizationInstanceRepository;
            _deviceCustomerChargeService = deviceCustomerChargeService;
        }

        public async Task HandleEventAsync(SQSEvent sqsEvent, SqsValues sqsValues)
        {
            try
            {
                await ProcessEventAsync(sqsEvent, sqsValues);
            }
            catch (Exception ex)
            {
                _logger.LogInfo(CommonConstants.EXCEPTION, ex.Message + " " + ex.StackTrace);
            }

            _logger.Flush();
        }

        private async Task ProcessEventAsync(SQSEvent sqsEvent, SqsValues sqsValues)
        {
            _logger.LogInfo(CommonConstants.SUB);
            switch (sqsEvent.Records.Count)
            {
                case 0:
                    return;
                case 1:
                    await ProcessEventRecordAsync(sqsEvent.Records[0], sqsValues);
                    break;
                default:
                    _logger.LogInfo(CommonConstants.EXCEPTION, string.Format(LogCommonStrings.MULTIPLE_MESSAGE_RECEIVED, sqsEvent.Records.Count));
                    break;
            }
        }

        private async Task ProcessEventRecordAsync(SQSEvent.SQSMessage message, SqsValues sqsValues)
        {
            _logger.LogInfo(CommonConstants.SUB);
            if (message.MessageAttributes.ContainsKey(SQSMessageKeyConstant.QUEUE_ID))
            {
                var queueIdString = message.MessageAttributes[SQSMessageKeyConstant.QUEUE_ID].StringValue;

                var queueId = long.Parse(queueIdString);
                var queue = _optimizationQueueRepository.GetQueue(queueId);
                var instance = _optimizationInstanceRepository.GetInstance(queue.InstanceId);

                await _deviceCustomerChargeService.ProcessQueueAsync(queueId, instance, sqsValues);
            }
            else if (message.MessageAttributes.ContainsKey(SQSMessageKeyConstant.FILE_ID))
            {
                var fileIdString = message.MessageAttributes[SQSMessageKeyConstant.FILE_ID].StringValue;

                var fileId = int.Parse(fileIdString);
                await _deviceCustomerChargeService.ProcessQueueAsync(fileId, sqsValues);
            }
            else
            {
                _logger.LogInfo(CommonConstants.EXCEPTION, LogCommonStrings.NO_QUEUE_ID_OR_FILE_ID);
            }
        }
    }
}

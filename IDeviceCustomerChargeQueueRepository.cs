using System.Collections.Generic;
using System.Threading.Tasks;
using Altaworx.AWS.Core;
using AltaworxRevAWSCreateCustomerChange.Models;
using Amop.Core.Models.Revio;

namespace AltaworxRevAWSCreateCustomerChange.Repositories.DeviceCustomerCharge
{
    public interface IDeviceCustomerChargeQueueRepository
    {
        IEnumerable<RevIOCommon.DeviceCustomerChargeQueueRecord> GetDeviceList(long queueId, int pageSize, int offset, bool isNonRev = false);
        IEnumerable<RevIOCommon.DeviceCustomerChargeQueueRecord> GetDeviceList(int fileId, int pageSize, int offset, bool isNonRev = false);
        CustomerChargeUploadedFile GetUploadedFile(int fileId);
        bool QueueHasMoreItems(long queueId, bool isNonRev = false);
        bool QueueHasMoreItems(int fileId, bool isNonRev = false);

        Task EnqueueCustomerChargesAsync(long queueId, int portalTypeId, string instanceIds, bool isMultipleInstance, bool isLastInstanceId, int pageNumber, int currentIntegrationAuthenticationId = 0, bool IsSendSummaryEmailForMultipleInstaceStep = false, int retryNumber = 0, int retryCount = 0);
        Task EnqueueCustomerChargesAsync(int fileId, int pageNumber, int currentIntegrationAuthenticationId = 0, int retryCount = 0);

        void MarkRecordProcessed(long id, string chargeId, decimal chargeAmount, decimal baseChargeAmount,
            decimal totalChargeAmount, bool hasErrors, string errorMessage, string smsChargeId, decimal smsChargeAmount);

        IEnumerable<RevIOCommon.DeviceCustomerChargeQueueRecord> GetChargeList(long queueId);
        IEnumerable<RevIOCommon.DeviceCustomerChargeQueueRecord> GetChargeList(int fileId);

        List<CustomerChargeQueueOfInstance> GetQueueIsNeedSendMailSumary(string InstanceIds, int portalTypeId);
        IEnumerable<RevIOCommon.RevProductType> GetProductTypeList();
        bool VerifyAnyInstanceStillInProgress(string sessionId, int portalTypeId, bool isNonRev);

        int CountAllM2MItemInQueue(long queueId, bool isNonRev = false);
        int CountAllMobilityItemInQueue(long queueId, bool isNonRev = false);
        int CountAllM2MItemInFile(int fileId, bool isNonRev = false);
        int CountAllMobilityItemInFile(int fileId, bool isNonRev = false);
        Task EnqueueCheckCustomerChargesIsProcessedAsync(long queueId, int portalTypeId, string instanceIds, bool isMultipleInstance, bool isLastInstanceId, int currentIntegrationAuthenticationId = 0, bool IsSendSummaryEmailForMultipleInstanceStep = false, int customDelayTime = 0, int retryNumber = 0);
        Task EnqueueCheckCustomerChargesIsProcessedAsync(int fileId, int currentIntegrationAuthenticationId = 0, int retryNumber = 0);
    }
}

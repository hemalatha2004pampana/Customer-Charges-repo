using System;
using System.Threading.Tasks;
using Altaworx.AWS.Core;
using AltaworxRevAWSCreateCustomerChange.Models;
using static Altaworx.AWS.Core.RevIOCommon;

namespace AltaworxRevAWSCreateCustomerChange.Services.DeviceCustomerCharge
{
    public interface IDeviceCustomerChargeService
    {
        Task ProcessQueueAsync(long queueId, OptimizationInstance instance, SqsValues sqsValues);
        Task ProcessQueueAsync(int fileId, SqsValues sqsValues);
        Task<Tuple<RevService, int>> LookupRevServiceAsync(RevIOCommon.DeviceCustomerChargeQueueRecord device);
    }
}

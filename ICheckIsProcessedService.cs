using Altaworx.AWS.Core;
using AltaworxRevAWSCreateCustomerChange.Models;
using Amazon.Lambda.SQSEvents;
using System.Threading.Tasks;

namespace AltaworxRevAWSCheckCustomerChargeIsProcessed.Services
{
    public interface ICheckIsProcessedService
    {
        Task ProcessQueueAsync(long queueId, OptimizationInstance instance, SqsValues sqsValues);

        Task ProcessQueueAsync(int fileId, SqsValues sqsValues);
    }
}
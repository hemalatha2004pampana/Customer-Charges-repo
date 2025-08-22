using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Altaworx.AWS.Core;
using Altaworx.AWS.Core.Models;
using AltaworxRevAWSCreateCustomerChange.Models;
using Amop.Core.Models.Revio;

namespace AltaworxRevAWSCreateCustomerChange.Services.ChargeList
{
    public interface ICustomerChargeListEmailService
    {
        Task SendEmailSummaryAsync(long queueId, OptimizationInstance instance, byte[] chargeListFileBytes, string fileName,
            int errorCount, bool isNonRev = false);

        Task SendEmailSummaryMultipleInstanceAsync(List<CustomerChargeQueueOfInstance> queueOfInstances, MemoryStream streamFile, string fileName, int errorCount, List<RevCustomerModel> lstCustomer, bool isNonRev = false);

        // Send mail when have FileId
        Task SendEmailSummaryAsync(CustomerChargeUploadedFile file, byte[] chargeListFileBytes, string fileName,
            int errorCount, bool isNonRev = false);
    }
}

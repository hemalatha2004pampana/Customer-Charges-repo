using System.Threading.Tasks;
using Altaworx.AWS.Core;
using AltaworxRevAWSCreateCustomerChange.Models;

namespace AltaworxRevAWSCreateCustomerChange.Repositories.DeviceCustomerCharge
{
    public interface IDeviceChargeRepository
    {
        Task<CustomerChargeResponse> AddChargeAsync(RevIOCommon.CreateDeviceChargeRequest request);
    }
}

using System;
using System.Collections.Generic;
using Altaworx.AWS.Core;

namespace AltaworxRevAWSCreateCustomerChange.Services.ChargeList
{
    public interface ICustomerChargeListFileService
    {
        byte[] GenerateChargeListFile(IEnumerable<RevIOCommon.DeviceCustomerChargeQueueRecord> chargeList,
            DateTime billingPeriodStartDate,
            DateTime billingPeriodEndDate,
            List<ServiceProvider> serviceProviders);

        byte[] GenerateChargeListFile(IEnumerable<RevIOCommon.DeviceCustomerChargeQueueRecord> chargeList, List<ServiceProvider> serviceProviders);
    }
}
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Altaworx.AWS.Core;
using Amop.Core.Helpers;

namespace AltaworxRevAWSCreateCustomerChange.Services.ChargeList
{
    public class CustomerChargeListFileService : ICustomerChargeListFileService
    {
        public byte[] GenerateChargeListFile(IEnumerable<RevIOCommon.DeviceCustomerChargeQueueRecord> chargeList,
            DateTime billingPeriodStartDate, DateTime billingPeriodEndDate, List<ServiceProvider> serviceProviders)
        {
            using (var ms = new MemoryStream())
            using (var sw = new StreamWriter(ms))
            {
                WriteChargeListFileHeader(sw);
                WriteChargeListFileBody(sw, chargeList.ToList(), billingPeriodStartDate, billingPeriodEndDate, serviceProviders);

                sw.Flush();

                var fileBytes = new byte[ms.Length];
                ms.Position = 0;
                ms.Read(fileBytes, 0, fileBytes.Length);

                sw.Close();

                return fileBytes;
            }
        }

        public byte[] GenerateChargeListFile(IEnumerable<RevIOCommon.DeviceCustomerChargeQueueRecord> chargeList, List<ServiceProvider> serviceProviders)
        {
            using (var ms = new MemoryStream())
            using (var sw = new StreamWriter(ms))
            {
                WriteChargeListFileHeader(sw);
                WriteChargeListFileBody(sw, chargeList.ToList(), serviceProviders);

                sw.Flush();

                var fileBytes = new byte[ms.Length];
                ms.Position = 0;
                ms.Read(fileBytes, 0, fileBytes.Length);

                sw.Close();

                return fileBytes;
            }
        }

        private static void WriteChargeListFileHeader(TextWriter sw)
        {
            sw.WriteLine(
                "MSISDN\tIsSuccessful\tChargeId\tChargeAmount\tSMSChargeId\tSMSChargeAmount\tBillingPeriodStart\tBillingPeriodEnd\tDateCharged\tErrorMessage");
        }

        private static void WriteChargeListFileBody(TextWriter sw,
            ICollection<RevIOCommon.DeviceCustomerChargeQueueRecord> chargeList, DateTime billingPeriodStart,
            DateTime billingPeriodEnd, List<ServiceProvider> serviceProviders)
        {
            foreach (var charge in chargeList)
            {
                var integrationId = 0;
                if (serviceProviders.Count > 0)
                {
                    integrationId = serviceProviders.FirstOrDefault(x => x.Id == charge.ServiceProviderId).IntegrationId;
                }
                var billingPeriodDay = RevIOHelper.BuildBillingPeriodDay(integrationId, billingPeriodStart, billingPeriodEnd);
                WriteChargeRow(sw, charge, billingPeriodDay.Item1, billingPeriodDay.Item2);
            }

            WriteChargeListFileFooter(sw, chargeList);
        }

        private static void WriteChargeListFileBody(TextWriter sw,
            ICollection<RevIOCommon.DeviceCustomerChargeQueueRecord> chargeList, List<ServiceProvider> serviceProviders)
        {
            foreach (var charge in chargeList)
            {
                var integrationId = 0;
                if (serviceProviders.Count > 0)
                {
                    integrationId = serviceProviders.FirstOrDefault(x => x.Id == charge.ServiceProviderId).IntegrationId;
                }
                var billingPeriodDay = RevIOHelper.BuildBillingPeriodDay(integrationId, charge.BillingStartDate, charge.BillingEndDate);
                WriteChargeRow(sw, charge, billingPeriodDay.Item1, billingPeriodDay.Item2);
            }

            WriteChargeListFileFooter(sw, chargeList);
        }

        private static void WriteChargeRow(TextWriter sw, RevIOCommon.DeviceCustomerChargeQueueRecord charge,
            string billingPeriodStart, string billingPeriodEnd)
        {
            var isSuccessful = charge.IsProcessed && (charge.ChargeId > 0 || charge.SmsChargeId > 0);
            var chargeId = isSuccessful ? charge.ChargeId.ToString() : string.Empty;
            var smsChargeId = isSuccessful ? charge.SmsChargeId.ToString() : string.Empty;
            var errorMessage = charge.ErrorMessage.Replace('\r', ' ').Replace('\n', ' ').Replace('\t', ' ');
            sw.WriteLine(
                $"{charge.MSISDN}\t{isSuccessful}\t{chargeId}\t{charge.ChargeAmount}\t{smsChargeId}\t{charge.SmsChargeAmount}\t{billingPeriodStart}\t{billingPeriodEnd}\t{charge.ModifiedDate}\t{errorMessage}");
        }

        private static void WriteChargeRow(TextWriter sw, RevIOCommon.DeviceCustomerChargeQueueRecord charge)
        {
            var isSuccessful = charge.IsProcessed && charge.ChargeId > 0;
            var chargeId = isSuccessful ? charge.ChargeId.ToString() : string.Empty;
            var errorMessage = charge.ErrorMessage.Replace('\r', ' ').Replace('\n', ' ').Replace('\t', ' ');
            sw.WriteLine(
                $"{charge.MSISDN}\t{isSuccessful}\t{chargeId}\t{charge.ChargeAmount}\t{charge.BillingStartDate.GetValueOrDefault().AddDays(1):yyyy-MM-dd}\t{charge.BillingEndDate.GetValueOrDefault():yyyy-MM-dd}\t{charge.ModifiedDate}\t{errorMessage}");
        }

        private static void WriteChargeListFileFooter(TextWriter sw,
            IEnumerable<RevIOCommon.DeviceCustomerChargeQueueRecord> chargeList)
        {
            var totalCharges = chargeList.Sum(x => x.ChargeAmount);
            sw.WriteLine($"\t\t\t{totalCharges}\t\t\t\t");
        }
    }
}
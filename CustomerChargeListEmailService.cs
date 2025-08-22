using Altaworx.AWS.Core;
using Altaworx.AWS.Core.Repositories.Customer;
using Altaworx.AWS.Core.Services;
using AltaworxRevAWSCreateCustomerChange.Models;
using Amazon;
using Amazon.SimpleEmail.Model;
using Amop.Core.Logger;
using Amop.Core.Models.Revio;
using Amop.Core.Models.Settings;
using MimeKit;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Mime;
using System.Threading.Tasks;

namespace AltaworxRevAWSCreateCustomerChange.Services.ChargeList
{
    public class CustomerChargeListEmailService : ICustomerChargeListEmailService
    {
        private readonly ICustomerRepository _customerRepository;
        private readonly ISimpleEmailServiceFactory _emailServiceFactory;
        private readonly IKeysysLogger _keysysLogger;
        private readonly ISettingsRepository _settingsRepository;

        public CustomerChargeListEmailService(IKeysysLogger keysysLogger,
            ISimpleEmailServiceFactory emailServiceFactory,
            ISettingsRepository settingsRepository, ICustomerRepository customerRepository)
        {
            _keysysLogger = keysysLogger;
            _emailServiceFactory = emailServiceFactory;
            _settingsRepository = settingsRepository;
            _customerRepository = customerRepository;
        }

        public async Task SendEmailSummaryAsync(long queueId, OptimizationInstance instance, byte[] chargeListFileBytes,
            string fileName,
            int errorCount, bool isNonRev = false)
        {
            // Handle for non rev customer
            _keysysLogger.LogInfo("SUB", $"SendEmailSummaryAsync({queueId})");
            string customerName = string.Empty;
            if (isNonRev)
            {
                customerName = _customerRepository.GetNonRevCustomerName(instance.AMOPCustomerId);
            }
            else
            {
                customerName = _customerRepository.GetCustomerName(instance.RevCustomerId ?? Guid.Empty);

            }
            var generalSettings = _settingsRepository.GetGeneralProviderSettings();
            var credentials = generalSettings.AwsSesCredentials;
            using (var client = _emailServiceFactory.getClient(credentials, RegionEndpoint.USEast1))
            {
                var message = new MimeMessage();
                message.From.Add(MailboxAddress.Parse(generalSettings.CustomerChargeFromEmailAddress));
                var recipientAddressList = generalSettings.CustomerChargeToEmailAddresses.Split(';').ToList();
                foreach (var recipientAddress in recipientAddressList)
                {
                    message.To.Add(MailboxAddress.Parse(recipientAddress));
                }

                message.Subject = generalSettings.CustomerChargeResultsEmailSubject;
                message.Body =
                    BuildResultsEmailBody(queueId, instance, customerName, chargeListFileBytes, fileName, errorCount)
                        .ToMessageBody();
                var stream = new MemoryStream();
                message.WriteTo(stream);

                var sendRequest = new SendRawEmailRequest
                {
                    RawMessage = new RawMessage(stream)
                };
                try
                {
                    await client.SendRawEmailAsync(sendRequest);
                }
                catch (Exception ex)
                {
                    _keysysLogger.LogInfo("Error Sending Summary Email", ex.Message);
                }
            }
        }

        public async Task SendEmailSummaryAsync(CustomerChargeUploadedFile file, byte[] chargeListFileBytes, string fileName,
            int errorCount, bool isNonRev = false)
        {
            _keysysLogger.LogInfo("SUB", $"SendEmailSummaryAsync({file.Id})");

            var generalSettings = _settingsRepository.GetGeneralProviderSettings();
            var credentials = generalSettings.AwsSesCredentials;
            using (var client = _emailServiceFactory.getClient(credentials, RegionEndpoint.USEast1))
            {
                var message = new MimeMessage();
                message.From.Add(MailboxAddress.Parse(generalSettings.CustomerChargeFromEmailAddress));
                var recipientAddressList = generalSettings.CustomerChargeToEmailAddresses.Split(';').ToList();
                foreach (var recipientAddress in recipientAddressList)
                {
                    message.To.Add(MailboxAddress.Parse(recipientAddress));
                }

                message.Subject = generalSettings.CustomerChargeResultsEmailSubject;
                message.Body = BuildResultsEmailBody(file, chargeListFileBytes, fileName, errorCount).ToMessageBody();
                var stream = new MemoryStream();
                message.WriteTo(stream);

                var sendRequest = new SendRawEmailRequest
                {
                    RawMessage = new RawMessage(stream)
                };
                try
                {
                    await client.SendRawEmailAsync(sendRequest);
                }
                catch (Exception ex)
                {
                    _keysysLogger.LogInfo("Error Sending Summary Email", ex.Message);
                }
            }
        }

        public async Task SendEmailSummaryMultipleInstanceAsync(List<CustomerChargeQueueOfInstance> queueOfInstances, MemoryStream streamFile, string fileName, int errorCount, List<RevCustomerModel> lstCustomer, bool isNonRev = false)
        {
            _keysysLogger.LogInfo("SUB", $"SendEmailSummaryMultipleInstanceAsync()");

            var generalSettings = _settingsRepository.GetGeneralProviderSettings();
            var credentials = generalSettings.AwsSesCredentials;
            using (var client = _emailServiceFactory.getClient(credentials, RegionEndpoint.USEast1))
            {
                var message = new MimeMessage();
                message.From.Add(MailboxAddress.Parse(generalSettings.CustomerChargeFromEmailAddress));
                var recipientAddressList = generalSettings.CustomerChargeToEmailAddresses.Split(';').ToList();
                foreach (var recipientAddress in recipientAddressList)
                {
                    message.To.Add(MailboxAddress.Parse(recipientAddress));
                }

                message.Subject = generalSettings.CustomerChargeResultsEmailSubject;
                message.Body =
                    BuildResultsEmailAttachZipBody(queueOfInstances, streamFile, fileName, errorCount, lstCustomer, isNonRev)
                        .ToMessageBody();
                var stream = new MemoryStream();
                message.WriteTo(stream);

                var sendRequest = new SendRawEmailRequest
                {
                    RawMessage = new RawMessage(stream)
                };
                try
                {
                    await client.SendRawEmailAsync(sendRequest);
                }
                catch (Exception ex)
                {
                    _keysysLogger.LogInfo("Error Sending Summary Email", ex.Message);
                }
            }
        }

        private BodyBuilder BuildResultsEmailAttachZipBody(List<CustomerChargeQueueOfInstance> queueOfInstances, MemoryStream stream, string fileName, int errorCount, List<RevCustomerModel> lstCustomer, bool isNonRev = false)
        {
            _keysysLogger.LogInfo("SUB", $"BuildResultsEmailAttachZipBody()");

            // body email 
            string bodyCustomerText = "";
            string bodyErrorMessage = "";
            string customerText = "<dd>- For {0} and Billing Period Ending on {1} {2}</dd>";
            string bodyTemplate = "<div><dl><dt>Here are your Customer Charges for </dt>{0}</dl></div>";
            if (errorCount > 0)
            {
                bodyErrorMessage = "<div style='color: red;'><dl><dt>THERE WERE ERRORS IN THIS PUSH. Please check the Optimization Session Details to view the errors.</dt></dl></div>";
            }

            if (isNonRev)
            {
                var customerGroupIds = queueOfInstances.DistinctBy(q => q.AMOPCustomerId).ToList();
                foreach (var queue in customerGroupIds)
                {
                    var customerName = lstCustomer.FirstOrDefault(c => c.Id == queue.AMOPCustomerId?.ToString())?.CustomerName ?? string.Empty;
                    bodyCustomerText += string.Format(customerText, customerName, queue.BillingPeriodEndDate.ToShortDateString(), queue.BillingPeriodEndDate.ToShortTimeString());
                }
            }
            else
            {
                var customerGroupIds = queueOfInstances.DistinctBy(q => q.RevCustomerId).ToList();
                foreach (var queue in customerGroupIds)
                {
                    var customerName = lstCustomer.FirstOrDefault(c => c.Id == queue.RevCustomerId.ToString())?.CustomerName ?? string.Empty;
                    bodyCustomerText += string.Format(customerText, customerName, queue.BillingPeriodEndDate.ToShortDateString(), queue.BillingPeriodEndDate.ToShortTimeString());
                }
            }
            bodyTemplate = string.Format(bodyTemplate, bodyCustomerText);
            //add error message if there were errors at the top of the body
            if (!string.IsNullOrEmpty(bodyErrorMessage))
            {
                bodyTemplate = bodyErrorMessage + bodyTemplate;
            }
            var body = new BodyBuilder
            {
                HtmlBody = bodyTemplate,
                TextBody = bodyTemplate
            };
            body.Attachments.Add(fileName, stream, MimeKit.ContentType.Parse(MediaTypeNames.Application.Zip));

            return body;
        }

        private BodyBuilder BuildResultsEmailBody(long queueId, OptimizationInstance instance, string customerName,
            byte[] chargeListFileBytes, string fileName, int errorCount)
        {
            _keysysLogger.LogInfo("SUB", $"BuildResultsEmailBody({queueId})");
            var body = new BodyBuilder
            {
                HtmlBody =
                    $"<div>Here are your Customer Charges for {customerName} and Billing Period Ending on {instance.BillingPeriodEndDate.ToShortDateString()} {instance.BillingPeriodEndDate.ToShortTimeString()}.{(errorCount > 0 ? "<br/>" + errorCount + " Error(s) occurred when uploading charges to Rev.IO" : "")}</div>",
                TextBody =
                    $"Customer Charges for {customerName} and Billing Period Ending on {instance.BillingPeriodEndDate.ToShortDateString()} {instance.BillingPeriodEndDate.ToShortTimeString()}.{(errorCount > 0 ? "\r\n" + errorCount + " Error(s) occurred when uploading charges to Rev.IO" : "")}"
            };

            body.Attachments.Add(fileName, chargeListFileBytes, new MimeKit.ContentType("text", "tab-separated-values"));

            return body;
        }

        private BodyBuilder BuildResultsEmailBody(CustomerChargeUploadedFile file, byte[] chargeListFileBytes,
            string fileName, int errorCount)
        {
            _keysysLogger.LogInfo("SUB", $"BuildResultsEmailBody({file.Id})");
            var body = new BodyBuilder
            {
                HtmlBody =
                    $"<div>Here are your Customer Charges for uploaded file \"{file.FileName}\". {(errorCount > 0 ? $"<br/>{errorCount} Error(s) occurred when uploading charges to Rev.IO" : "")}</div>",
                TextBody =
                    $"Customer Charges for uploaded file \"{file.FileName}\". {(errorCount > 0 ? "\r\n" + errorCount + " Error(s) occurred when uploading charges to Rev.IO" : "")}"
            };

            body.Attachments.Add(fileName, chargeListFileBytes, new MimeKit.ContentType("text", "tab-separated-values"));

            return body;
        }
    }
}

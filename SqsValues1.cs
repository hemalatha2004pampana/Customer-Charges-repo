using Altaworx.AWS.Core.Models;
using Amop.Core.Constants;
using Amop.Core.Logger;
using System;
using System.Collections.Generic;
using System.Text;
using static Amazon.Lambda.SQSEvents.SQSEvent;

namespace AltaworxRevAWSCreateCustomerChange.Models
{
    public class SqsValues
    {
        public bool IsMultipleInstanceId { get; set; }
        public bool IsLastInstanceId { get; set; }
        public string InstanceIds { get; set; }
        public int PortalTypeId { get; set; }
        public int CurrentIntegrationAuthenticationId { get; set; }
        public bool IsSendSummaryEmailForMultipleInstanceStep { get; set; }
        public int RetryNumber { get; set; }
        public int PageNumber { get; set; }
        public int RetryCount { get; set; }

        public SqsValues(IKeysysLogger logger, SQSMessage message)
        {
            if (message.MessageAttributes.ContainsKey("IsMultipleInstanceId"))
            {
                IsMultipleInstanceId = Convert.ToBoolean(Int32.Parse(message.MessageAttributes["IsMultipleInstanceId"].StringValue));
                logger.LogInfo("IsMultipleInstanceId", IsMultipleInstanceId);
            }
            else
            {
                IsMultipleInstanceId = false;
                logger.LogInfo("IsMultipleInstanceId", IsMultipleInstanceId);
            }

            if (message.MessageAttributes.ContainsKey("IsLastInstanceId"))
            {
                IsLastInstanceId = Convert.ToBoolean(Int32.Parse(message.MessageAttributes["IsLastInstanceId"].StringValue));
                logger.LogInfo("IsLastInstanceId", IsLastInstanceId);
            }
            else
            {
                IsLastInstanceId = false;
                logger.LogInfo("IsLastInstanceId", IsLastInstanceId);
            }

            if (message.MessageAttributes.ContainsKey("InstanceIds"))
            {
                InstanceIds = message.MessageAttributes["InstanceIds"].StringValue;
                logger.LogInfo("InstanceIds", InstanceIds);
            }
            else
            {
                InstanceIds = "";
                logger.LogInfo("InstanceIds", InstanceIds);
            }

            if (message.MessageAttributes.ContainsKey("PortalTypeId"))
            {
                PortalTypeId = Int32.Parse(message.MessageAttributes["PortalTypeId"].StringValue);
                logger.LogInfo("PortalTypeId", PortalTypeId);
            }
            else
            {
                PortalTypeId = 0;
                logger.LogInfo("PortalTypeId", PortalTypeId);
            }

            if (message.MessageAttributes.ContainsKey("CurrentIntegrationAuthenticationId"))
            {
                CurrentIntegrationAuthenticationId = Int32.Parse(message.MessageAttributes["CurrentIntegrationAuthenticationId"].StringValue);
                logger.LogInfo("CurrentIntegrationAuthenticationId", CurrentIntegrationAuthenticationId);
            }
            else
            {
                CurrentIntegrationAuthenticationId = 0;
                logger.LogInfo("CurrentIntegrationAuthenticationId", CurrentIntegrationAuthenticationId);
            }
            if (message.MessageAttributes.ContainsKey("IsSendSummaryEmailForMultipleInstaceStep"))
            {
                IsSendSummaryEmailForMultipleInstanceStep = Convert.ToBoolean(Int32.Parse(message.MessageAttributes["IsSendSummaryEmailForMultipleInstaceStep"].StringValue));
            }
            else
            {
                IsSendSummaryEmailForMultipleInstanceStep = false;
            }
            if (message.MessageAttributes.ContainsKey(SQSMessageKeyConstant.RETRY_NUMBER))
            {
                RetryNumber = Int32.Parse(message.MessageAttributes[SQSMessageKeyConstant.RETRY_NUMBER].StringValue);
            }
            if (message.MessageAttributes.ContainsKey(SQSMessageKeyConstant.PAGE_NUMBER))
            {
                PageNumber = Int32.Parse(message.MessageAttributes[SQSMessageKeyConstant.PAGE_NUMBER].StringValue);
            }
            else
            {
                PageNumber = 1;
            }
            if (message.MessageAttributes.ContainsKey(SQSMessageKeyConstant.RETRY_COUNT))
            {
                RetryCount = Int32.Parse(message.MessageAttributes[SQSMessageKeyConstant.RETRY_COUNT].StringValue);
                logger.LogInfo(SQSMessageKeyConstant.RETRY_COUNT, RetryCount);
            }
            else
            {
                RetryCount = 0;
                logger.LogInfo(SQSMessageKeyConstant.RETRY_COUNT, RetryCount);
            }
        }
    }
}

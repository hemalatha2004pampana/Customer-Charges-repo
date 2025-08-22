using Altaworx.AWS.Core.Models;
using System;
using System.Collections.Generic;
using System.Text;
using static Amazon.Lambda.SQSEvents.SQSEvent;

namespace AltaworxRevAWSEnqueueCustomerCharges
{
    public class SqsValues
    {
        public int IsMultipleInstanceId { get; set; }
        public int IsLastInstanceId { get; set; }
        public string InstanceIds { get; set; }
        public string CurrentIntegrationAuthenticationId { get; set; }

        public SqsValues(KeySysLambdaContext context, SQSMessage message)
        {
            if (message.MessageAttributes.ContainsKey("IsMultipleInstanceId"))
            {
                IsMultipleInstanceId = Int32.Parse(message.MessageAttributes["IsMultipleInstanceId"].StringValue);
                context.LogInfo("IsMultipleInstanceId", IsMultipleInstanceId);
            }
            else
            {
                IsMultipleInstanceId = 0;
                context.LogInfo("IsMultipleInstanceId", IsMultipleInstanceId);
            }
            if (message.MessageAttributes.ContainsKey("IsLastInstanceId"))
            {
                IsLastInstanceId = Int32.Parse(message.MessageAttributes["IsLastInstanceId"].StringValue);
                context.LogInfo("IsLastInstanceId", IsLastInstanceId);
            }
            else
            {
                IsLastInstanceId = 0;
                context.LogInfo("IsLastInstanceId", IsLastInstanceId);
            }

            if (message.MessageAttributes.ContainsKey("InstanceIds"))
            {
                InstanceIds = message.MessageAttributes["InstanceIds"].StringValue;
                context.LogInfo("InstanceIds", InstanceIds);
            }
            else
            {
                InstanceIds = null;
                context.LogInfo("InstanceIds", InstanceIds);
            }

            if (message.MessageAttributes.ContainsKey("CurrentIntegrationAuthenticationId"))
            {
                CurrentIntegrationAuthenticationId = message.MessageAttributes["CurrentIntegrationAuthenticationId"].StringValue;
                context.LogInfo("CurrentIntegrationAuthenticationId", CurrentIntegrationAuthenticationId);
            }
            else
            {
                CurrentIntegrationAuthenticationId = "";
                context.LogInfo("CurrentIntegrationAuthenticationId", CurrentIntegrationAuthenticationId);
            }
        }
    }
}

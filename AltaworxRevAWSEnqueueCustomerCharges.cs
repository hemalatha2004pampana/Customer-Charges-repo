using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Altaworx.AWS.Core;
using Altaworx.AWS.Core.Models;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.SQS;
using Amazon.SQS.Model;
using Amop.Core.Constants;
using Amop.Core.Enumerations;
using Microsoft.Data.SqlClient;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace AltaworxRevAWSEnqueueCustomerCharges
{
    public class Function : AwsFunctionBase
    {
        private string DeviceCustomerChargeQueueUrl = Environment.GetEnvironmentVariable("DeviceCustomerChargeQueueUrl");
        private const int PortalTypeM2M = 0;
        private const int PortalTypeMobility = 2;

        /// <summary>
        /// A simple function that takes a string and does a ToUpper
        /// </summary>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public void FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
        {
            KeySysLambdaContext keysysContext = null;
            try
            {
                keysysContext = base.BaseFunctionHandler(context);
                if (string.IsNullOrWhiteSpace(DeviceCustomerChargeQueueUrl))
                {
                    DeviceCustomerChargeQueueUrl = context.ClientContext.Environment["DeviceCustomerChargeQueueUrl"];
                }

                ProcessEvent(keysysContext, sqsEvent);
            }
            catch (Exception ex)
            {
                LogInfo(keysysContext, "EXCEPTION", ex.Message);
            }

            base.CleanUp(keysysContext);
        }

        private void ProcessEvent(KeySysLambdaContext context, SQSEvent sqsEvent)
        {
            LogInfo(context, "SUB", "ProcessEvent");
            if (sqsEvent.Records.Count > 0)
            {
                if (sqsEvent.Records.Count == 1)
                {
                    ProcessEventRecord(context, sqsEvent.Records[0]);
                }
                else
                {
                    LogInfo(context, "EXCEPTION", $"Expected a single message, received {sqsEvent.Records.Count}");
                }
            }
        }

        private void ProcessEventRecord(KeySysLambdaContext context, SQSEvent.SQSMessage message)
        {
            LogInfo(context, "SUB", "ProcessEventRecord");
            if (message.MessageAttributes.ContainsKey("InstanceId"))
            {
                string instanceIdString = message.MessageAttributes["InstanceId"].StringValue;
                long instanceId = long.Parse(instanceIdString);

                var sqsValues = new SqsValues(context, message);

                ProcessInstance(context, instanceId, sqsValues);
            }
            else
            {
                LogInfo(context, "EXCEPTION", $"No Instance Id provided in message");
            }
        }

        private void ProcessInstance(KeySysLambdaContext context, long instanceId, SqsValues sqsValues)
        {
            LogInfo(context, "SUB", $"ProcessInstance({instanceId})");
            LogInfo(context, "INFO", $"IsMultipleInstanceId: {sqsValues.IsMultipleInstanceId}");
            LogInfo(context, "INFO", $"IsLastInstanceId: {sqsValues.IsLastInstanceId}");
            LogInfo(context, "INFO", $"InstanceIds: {sqsValues.InstanceIds}");


            // get instance
            OptimizationInstance instance = GetInstance(context, instanceId);

            context.LoadOptimizationSettingsByTenantId(instance.TenantId);

            // get comm groups
            List<OptimizationCommGroup> commGroups = GetCommGroups(context, instanceId);

            // cleanup each comm group
            int messageIsLastInstanceId = sqsValues.IsLastInstanceId;
            sqsValues.IsLastInstanceId = 0; // set default is false

            foreach (var item in commGroups.Select((commGroup, index) => new { index, commGroup }))
            {
                // get winning queue for each comm group
                long winningQueueId = GetWinningQueueId(context, item.commGroup.Id, instance.PortalTypeId);

                // enqueue devices that need charges
                if (sqsValues.IsMultipleInstanceId == 1 && messageIsLastInstanceId == 1 && item.index == commGroups.Count - 1)
                {
                    sqsValues.IsLastInstanceId = 1;
                }
                EnqueueCustomerCharges(context, winningQueueId, instance.PortalTypeId, sqsValues, (int)instance.IntegrationAuthenticationId);
            }
        }

        private long GetWinningQueueId(KeySysLambdaContext context, long commGroupId, int portalTypeId)
        {
            LogInfo(context, "SUB", $"GetWinningQueueId({commGroupId})");
            List<OptimizationCommGroup> commGroups = new List<OptimizationCommGroup>();
            var sqlCommand = portalTypeId == PortalTypeMobility
                ? GetMobilityDeviceWinningQueueSql()
                : GetDeviceWinningQueueSql();

            if (portalTypeId == (int)PortalTypeEnum.CrossProvider)
            {
                sqlCommand = GetCrossProviderDeviceWinningQueueSql();
            }
            using (var Conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var Cmd = new SqlCommand(sqlCommand, Conn))
                {
                    Cmd.CommandType = CommandType.Text;
                    Cmd.Parameters.AddWithValue("@commGroupId", commGroupId);
                    Conn.Open();

                    SqlDataReader rdr = Cmd.ExecuteReader();
                    while (rdr.Read())
                    {
                        return long.Parse(rdr["Id"].ToString());
                    }

                    Conn.Close();
                }
            }

            return 0;
        }

        private void EnqueueCustomerCharges(KeySysLambdaContext context, long queueId, int portalTypeId, SqsValues sqsValues, int integrationAuthenticationId)
        {
            LogInfo(context, "SUB", $"EnqueueRatePlanChanges({queueId})");
            if (portalTypeId == (int)PortalTypeEnum.CrossProvider)
            {
                LogInfo(context, CommonConstants.SUB, $"{nameof(sqsValues.CurrentIntegrationAuthenticationId)}: {sqsValues.CurrentIntegrationAuthenticationId}");
                EnqueueCustomerChargesDb(context, queueId, PortalTypeM2M);
                EnqueueCustomerChargesDb(context, queueId, PortalTypeMobility);
                EnqueueCustomerChargesSqs(context, queueId, sqsValues, portalTypeId, Convert.ToInt32(sqsValues.CurrentIntegrationAuthenticationId));
            }
            else
            {
                LogInfo(context, CommonConstants.SUB, $"{nameof(integrationAuthenticationId)}: {integrationAuthenticationId}");
                EnqueueCustomerChargesDb(context, queueId, portalTypeId);
                EnqueueCustomerChargesSqs(context, queueId, sqsValues, portalTypeId, integrationAuthenticationId);
            }
        }

        private void EnqueueCustomerChargesDb(KeySysLambdaContext context, long queueId, int portalTypeId)
        {
            LogInfo(context, "SUB", $"EnqueueCustomerChargesDb({queueId})");
            using (var Conn = new SqlConnection(context.CentralDbConnectionString))
            {
                Conn.Open();
                using (var Cmd = new SqlCommand("SET ARITHABORT ON", Conn))
                {
                    Cmd.ExecuteNonQuery();
                }

                var sqlCommand = portalTypeId == PortalTypeMobility
                    ? "usp_Optimization_Mobility_EnqueueCustomerCharges"
                    : "usp_Optimization_EnqueueCustomerCharges";
                using (var Cmd = new SqlCommand(sqlCommand, Conn))
                {
                    Cmd.CommandType = CommandType.StoredProcedure;
                    Cmd.Parameters.AddWithValue("@QueueId", queueId);
                    Cmd.CommandTimeout = 240;
                    Cmd.ExecuteNonQuery();
                }
                Conn.Close();
            }
        }

        private void EnqueueCustomerChargesSqs(KeySysLambdaContext context, long queueId, SqsValues sqsValues, int portalTypeId, int integrationAuthenticationId)
        {
            LogInfo(context, "SUB", $"EnqueueCustomerChargesSqs({queueId})");

            var awsCredentials = AwsCredentials(context);
            using (var client = new AmazonSQSClient(awsCredentials, Amazon.RegionEndpoint.USEast1))
            {
                var requestMsgBody = $"Queue to work is {queueId}";
                var isLastQueue = sqsValues.IsLastInstanceId == 1;
                var request = new SendMessageRequest
                {
                    DelaySeconds = isLastQueue ? CommonConstants.DELAY_IN_SECONDS_FIVE_MINUTES : 0, // if the last queue delay 5 minutes to make sure that all txt file uploaded to S3.
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        {
                            "QueueId", new MessageAttributeValue
                            { DataType = "String", StringValue = queueId.ToString()}
                        },
                        {
                            "IsMultipleInstanceId", new MessageAttributeValue
                            { DataType = "String", StringValue = sqsValues.IsMultipleInstanceId.ToString()}
                        },
                        {
                            "IsLastInstanceId", new MessageAttributeValue
                            { DataType = "String", StringValue = sqsValues.IsLastInstanceId.ToString()}
                        },
                        {
                            "InstanceIds", new MessageAttributeValue
                            { DataType = "String", StringValue = sqsValues.InstanceIds.ToString()}
                        },
                        {
                            "PortalTypeId", new MessageAttributeValue
                            { DataType = "String", StringValue = portalTypeId.ToString()}
                        },
                        {
                            "CurrentIntegrationAuthenticationId", new MessageAttributeValue
                            { DataType = "String", StringValue = integrationAuthenticationId.ToString()}
                        },
                    },
                    MessageBody = requestMsgBody,
                    QueueUrl = DeviceCustomerChargeQueueUrl
                };

                var response = client.SendMessageAsync(request);
                response.Wait();
                if (response.Status == TaskStatus.Faulted || response.Status == TaskStatus.Canceled)
                {
                    LogInfo(context, "RESPONSE STATUS", $"Error Sending {queueId}: {response.Status}");
                }
            }
        }

        private static string GetDeviceWinningQueueSql()
        {
            return @"SELECT TOP 1 Id FROM OptimizationQueue oq
                      WHERE EXISTS (
                        SELECT 1 FROM OptimizationDeviceResult odr
                        WHERE oq.Id = odr.QueueId
                      )
                      AND CommPlanGroupId = @commGroupId
                      AND TotalCost IS NOT NULL
                      AND RunEndTime IS NOT NULL
                      ORDER BY TotalCost";
        }

        private static string GetMobilityDeviceWinningQueueSql()
        {
            return @"SELECT TOP 1 Id FROM OptimizationQueue oq
                      WHERE EXISTS (
                        SELECT 1 FROM OptimizationMobilityDeviceResult odr
                        WHERE oq.Id = odr.QueueId
                      )
                      AND CommPlanGroupId = @commGroupId
                      AND TotalCost IS NOT NULL
                      AND RunEndTime IS NOT NULL
                      ORDER BY TotalCost";
        }

        private static string GetCrossProviderDeviceWinningQueueSql()
        {
            return @"SELECT TOP 1 Id FROM OptimizationQueue oq
                      WHERE EXISTS (
                        SELECT 1 FROM OptimizationMobilityDeviceResult odr
                        WHERE oq.Id = odr.QueueId

                        UNION 

                        SELECT 1 FROM OptimizationDeviceResult odr
                        WHERE oq.Id = odr.QueueId
                      )
                      AND CommPlanGroupId = @commGroupId
                      AND TotalCost IS NOT NULL
                      AND RunEndTime IS NOT NULL
                      ORDER BY TotalCost";
        }
    }
}

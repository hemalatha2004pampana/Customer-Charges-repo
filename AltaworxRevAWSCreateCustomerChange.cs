using System.Threading.Tasks;
using Altaworx.AWS.Core;
using Altaworx.AWS.Core.Repositories.Customer;
using Altaworx.AWS.Core.Repositories.OptimizationInstance;
using Altaworx.AWS.Core.Repositories.OptimizationQueue;
using Altaworx.AWS.Core.Repositories.RevIo;
using Altaworx.AWS.Core.Services;
using Amop.Core.Services.Http;
using AltaworxRevAWSCreateCustomerChange.EventHandlers;
using AltaworxRevAWSCreateCustomerChange.Repositories.DeviceCustomerCharge;
using AltaworxRevAWSCreateCustomerChange.Services.ChargeList;
using AltaworxRevAWSCreateCustomerChange.Services.DeviceCustomerCharge;
using Amazon;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amop.Core.Logger;
using Amop.Core.Models.Settings;
using Amop.Core.Repositories.Environment;
using Amop.Core.Resilience;
using Amop.Core.Services.Base64Service;
using AltaworxRevAWSCreateCustomerChange.Models;
using Amop.Core.Repositories.Revio;
using Amop.Core.Services.Revio;
using System.Linq;
using Altaworx.AWS.Core.Models;
using System;
using Amop.Core.Constants;


// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]
namespace AltaworxRevAWSCreateCustomerChange
{
    public class Function : AwsFunctionBase
    {
        private const int HttpRetryMaxCount = 3;

        /// <summary>
        /// This method is called for every Lambda invocation. This method takes in an SQS event object and can be used 
        /// to respond to SQS messages.
        /// </summary>
        /// <param name="sqsEvent"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
        {
            KeySysLambdaContext keysysContext = null;
            try
            {
                keysysContext = BaseFunctionHandler(context);
                var environmentRepo = new EnvironmentRepository();
                var connectionString = environmentRepo.GetEnvironmentVariable(context, "ConnectionString");
                var logger = keysysContext.logger;
                var optimizationInstanceRepo = new OptimizationInstanceRepository(logger, connectionString);
                var optimizationQueueRepo = new OptimizationQueueRepository(logger, connectionString);
                var base64Service = new Base64Service();
                var settingsRepo = new SettingsRepository(logger, connectionString, base64Service);
                var deviceCustomerChargeQueueRepo =
                    new DeviceCustomerChargeQueueRepository(logger, environmentRepo, context, connectionString,
                        settingsRepo);
                var revIoAuthRepo = new RevioAuthenticationRepository(connectionString, base64Service, logger);
                var currentRecord = sqsEvent.Records.First();
                var integrationAuthenticationId = GetCurrentIntegrationAuthenticationId(currentRecord);
                LogInfo(keysysContext, CommonConstants.INFO, $"Integration Authentication Id: {integrationAuthenticationId}");
                var revIoAuth = revIoAuthRepo.GetRevioApiAuthentication(integrationAuthenticationId);
                var revioApiClient = new RevioApiClient(new SingletonHttpClientFactory(), new HttpRequestFactory(), revIoAuth, keysysContext.IsProduction, CommonConstants.NUMBER_OF_REV_IO_RETRIES_3);
                var chargeListFileService = new CustomerChargeListFileService();
                var emailClientFactory = new SimpleEmailServiceFactory();
                var customerRepo = new CustomerRepository(logger, connectionString);
                var chargeListEmailService =
                    new CustomerChargeListEmailService(logger, emailClientFactory, settingsRepo, customerRepo);
                var generalProviderSettings = settingsRepo.GetGeneralProviderSettings();
                var s3Wrapper = new S3Wrapper(generalProviderSettings.AwsCredentials,
                    environmentRepo.GetEnvironmentVariable(context, "CustomerChargesS3BucketName"));
                var httpClientFactory = new KeysysHttpClientFactory();
                var emailFactory = new SimpleEmailServiceFactory();
                using var client = emailFactory.getClient(generalProviderSettings.AwsSesCredentials, RegionEndpoint.USEast1);
                var awsEnv = environmentRepo.GetEnvironmentVariable(context, "AWSEnv");
                var emailSender = new EmailSender(client, logger, awsEnv);
                var httpRetryPolicy = Amop.Core.Helpers.RetryPolicyHelper.PollyRetryRevIOHttpRequestAsync(logger, CommonConstants.NUMBER_OF_REV_IO_RETRIES_3);

                var deviceChargeRepository =
                    new DeviceChargeRepository(logger, base64Service, environmentRepo, context, httpClientFactory, httpRetryPolicy, emailSender,
                        generalProviderSettings, revioApiClient);
                var chargeService = new DeviceCustomerChargeService(logger, deviceCustomerChargeQueueRepo, revIoAuthRepo,
                    environmentRepo, context, settingsRepo, chargeListFileService,
                    s3Wrapper, chargeListEmailService, deviceChargeRepository, customerRepo, revioApiClient, emailSender, generalProviderSettings);
                var changeHandler =
                    new CustomerChangeEventHandler(logger, optimizationQueueRepo, optimizationInstanceRepo,
                        chargeService);
                var sqsValues = new SqsValues(logger, sqsEvent.Records[0]);
                var proxyUrl = environmentRepo.GetEnvironmentVariable(context, "ProxyUrl");

                await changeHandler.HandleEventAsync(sqsEvent, sqsValues);
                logger.Flush();
            }
            catch (Exception ex)
            {
                LogInfo(keysysContext, CommonConstants.EXCEPTION, ex.Message + " " + ex.StackTrace);
            }
        }

        private static int GetCurrentIntegrationAuthenticationId(SQSEvent.SQSMessage message)
        {
            int integrationAuthenticationId = -1;
            if (message.MessageAttributes.ContainsKey("CurrentIntegrationAuthenticationId"))
            {
                integrationAuthenticationId = int.Parse(message.MessageAttributes["CurrentIntegrationAuthenticationId"].StringValue);
            }
            return integrationAuthenticationId;
        }
    }
}

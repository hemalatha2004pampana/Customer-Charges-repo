using Altaworx.AWS.Core;
using Altaworx.AWS.Core.Models;
using Altaworx.AWS.Core.Repositories.Customer;
using Altaworx.AWS.Core.Repositories.OptimizationInstance;
using Altaworx.AWS.Core.Repositories.OptimizationQueue;
using Altaworx.AWS.Core.Services;
using AltaworxRevAWSCheckCustomerChargeIsProcessed.EventHandlers;
using AltaworxRevAWSCheckCustomerChargeIsProcessed.Services;
using AltaworxRevAWSCreateCustomerChange.Models;
using AltaworxRevAWSCreateCustomerChange.Repositories.DeviceCustomerCharge;
using AltaworxRevAWSCreateCustomerChange.Services.ChargeList;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amop.Core.Constants;
using Amop.Core.Models.Settings;
using Amop.Core.Repositories.Environment;
using Amop.Core.Services.Base64Service;
using System.Threading.Tasks;
using Microsoft.IdentityModel.Logging;
using Polly;
using System;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]
namespace AltaworxRevAWSCheckCustomerChargeIsProcessed
{
    public class Function : AwsFunctionBase
    {
        public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
        {
            KeySysLambdaContext keysysContext = null;
            try
            {
                keysysContext = BaseFunctionHandler(context);
                var environmentRepo = new EnvironmentRepository();
                var connectionString = environmentRepo.GetEnvironmentVariable(context, SQSMessageKeyConstant.CONNECTION_STRING);
                var logger = keysysContext.logger;
                var base64Service = new Base64Service();
                var settingsRepo = new SettingsRepository(logger, connectionString, base64Service);
                var emailClientFactory = new SimpleEmailServiceFactory();
                var deviceCustomerChargeQueueRepo = new DeviceCustomerChargeQueueRepository(logger, environmentRepo, context, connectionString, settingsRepo);
                var customerRepo = new CustomerRepository(logger, connectionString);
                var chargeListEmailService = new CustomerChargeListEmailService(logger, emailClientFactory, settingsRepo, customerRepo);
                var chargeListFileService = new CustomerChargeListFileService();
                var generalProviderSettings = settingsRepo.GetGeneralProviderSettings();
                var s3Wrapper = new S3Wrapper(generalProviderSettings.AwsCredentials,
                    environmentRepo.GetEnvironmentVariable(context, SQSMessageKeyConstant.CUSTOMER_CHARGES_S3_BUCKET_NAME));
                var checkIsProcessedService = new CheckIsProcessedService(context, chargeListEmailService, deviceCustomerChargeQueueRepo, logger, chargeListFileService, environmentRepo, s3Wrapper, customerRepo);
                var sqsValues = new SqsValues(logger, sqsEvent.Records[0]);
                var optimizationInstanceRepo = new OptimizationInstanceRepository(logger, connectionString);
                var optimizationQueueRepo = new OptimizationQueueRepository(logger, connectionString);
                var checkIsProcessEventHandler = new CheckIsProcessedEventHandler(logger, optimizationQueueRepo, optimizationInstanceRepo, checkIsProcessedService);

                await checkIsProcessEventHandler.HandleEventAsync(sqsEvent, sqsValues);
            }
            catch (Exception ex)
            {
                LogInfo(keysysContext, CommonConstants.EXCEPTION, ex.Message + " " + ex.StackTrace);
            }

            base.CleanUp(keysysContext);
        }
    }
}
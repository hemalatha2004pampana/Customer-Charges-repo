using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Altaworx.AWS.Core;
using Altaworx.AWS.Core.Services;
using Amop.Core.Services.Http;
using AltaworxRevAWSCreateCustomerChange.Models;
using Amazon.Lambda.Core;
using Amop.Core.Logger;
using Amop.Core.Models.Settings;
using Amop.Core.Repositories.Environment;
using Amop.Core.Services.Base64Service;
using MimeKit;
using Newtonsoft.Json;
using Polly;
using Amop.Core.Constants;
using Amop.Core.Services.Revio;

namespace AltaworxRevAWSCreateCustomerChange.Repositories.DeviceCustomerCharge
{
    public class DeviceChargeRepository : IDeviceChargeRepository
    {
        private readonly IKeysysLogger logger;
        private readonly IBase64Service base64Service;
        private readonly IEnvironmentRepository environmentRepository;
        private readonly ILambdaContext context;
        private readonly IKeysysHttpClientFactory keysysHttpClientFactory;
        private readonly IEmailSender emailSender;
        private readonly GeneralProviderSettings settings;
        private readonly IAsyncPolicy<HttpResponseMessage> retryPolicy;
        private readonly RevioApiClient revioApiClient;

        public DeviceChargeRepository(IKeysysLogger logger, IBase64Service base64Service, IEnvironmentRepository environmentRepository, ILambdaContext context,
            IKeysysHttpClientFactory keysysHttpClientFactory, IAsyncPolicy<HttpResponseMessage> retryPolicy, IEmailSender emailSender, GeneralProviderSettings settings, RevioApiClient revioApiClient)
        {
            this.logger = logger;
            this.base64Service = base64Service;
            this.environmentRepository = environmentRepository;
            this.context = context;
            this.keysysHttpClientFactory = keysysHttpClientFactory;
            this.emailSender = emailSender;
            this.settings = settings;
            this.retryPolicy = retryPolicy;
            this.revioApiClient = revioApiClient;
        }

        public async Task<CustomerChargeResponse> AddChargeAsync(RevIOCommon.CreateDeviceChargeRequest request)
        {
            var requestString = JsonConvert.SerializeObject(request);

            var response = await revioApiClient.AddChargeAsync(requestString, retryPolicy, logger);
            if (response == null || response?.Id <= 0)
            {
                logger.LogInfo(CommonConstants.WARNING, string.Format(LogCommonStrings.ERROR_WHILE_UPLOADING_CHARGES, response));
                var errorMessage = JsonConvert.SerializeObject(response);
                return new CustomerChargeResponse
                {
                    HasErrors = true,
                    ErrorMessage = errorMessage
                };
            }

            return new CustomerChargeResponse
            {
                ChargeId = response.Id,
                HasErrors = false,
                ErrorMessage = string.Empty
            };
        }
        private static string GetErrorMessage(string response)
        {
            var responseBody = !string.IsNullOrEmpty(response) ? response.Trim() : response;
            if (responseBody != null && responseBody.Length > 1000)
            {
                responseBody = responseBody.Substring(0, 1000);
            }

            return responseBody;
        }
    }
}

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Runtime.Caching;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using System.Web.ModelBinding;
using System.Web.Mvc;
using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Amop.Core.Constants;
using Amop.Core.Helpers;
using Amop.Core.Models;
using CsvHelper;
using CsvHelper.Configuration;
using KeySys.BaseMultiTenant.Helpers;
using KeySys.BaseMultiTenant.Mapping;
using KeySys.BaseMultiTenant.Models.CustomClasses;
using KeySys.BaseMultiTenant.Models.CustomerCharge;
using KeySys.BaseMultiTenant.Models.Device;
using KeySys.BaseMultiTenant.Models.Mobility;
using KeySys.BaseMultiTenant.Models.Report;
using KeySys.BaseMultiTenant.Models.Repositories;
using KeySys.BaseMultiTenant.Permissions;
using KeySys.BaseMultiTenant.Repositories.Rev;
using KeySys.BaseMultiTenant.Utilities;
using OfficeOpenXml;

namespace KeySys.BaseMultiTenant.Controllers
{
    public class CustomerChargeController : AmopBaseController
    {
        private const string CUSTOMER_CHARGE_QUEUE_NAME = "Customer Charge Queue";
        private const string CREATE_CUSTOMER_CHARGE_QUEUE_NAME = "Create Customer Charge Queue";

        // GET: CustomerCharge
        public ActionResult Index(string selectedSessions, string accountNumber, int page = 1, string sort = "", string sortDir = "")
        {
            if (permissionManager.UserCannotAccess(Session, ModuleEnum.CustomerCharge))
            {
                return RedirectToAction("Index", "Home");
            }
            ViewBag.PageTitle = "Customer Charges";
            var siteType = SiteType.Rev;
            if (!permissionManager.UserCanAccess(Session, ModuleEnum.RevCustomers))
            {
                siteType = SiteType.AMOP;
            }
            var model = new CustomerChargeSummaryModel(altaWrxDb, permissionManager, accountNumber, selectedSessions, page, sort, sortDir, siteType);
            return View(model);
        }

        public FileContentResult Download(long id)
        {
            if (permissionManager.UserCannotAccess(Session, ModuleEnum.CustomerCharge))
            {
                return null;
            }
            var siteType = SiteType.Rev;
            if (!permissionManager.UserCanAccess(Session, ModuleEnum.RevCustomers))
            {
                siteType = SiteType.AMOP;
            }
            var model = new CustomerChargeDetailModel(altaWrxDb, id, siteType);
            var data = new System.Data.DataSet();
            var accountNumber = model.RevAccountNumber;
            if (siteType == SiteType.AMOP)
            {
                data = model.DetailListNonRev.ToDataSet("CustomerChargeDetail");
                accountNumber = model.AMOPCustomerId?.ToString() ?? string.Empty;
            }
            else
            {
                data = model.DetailList.ToDataSet("CustomerChargeDetail");
            }
            var bytes = ExcelUtilities.CustomExportForCustomerCharge(data, new List<int> { 7, 8, 9, 10, 11, 16, 17 }, new List<int> { 18 }, new List<int> { 3, 4, 5 });
            var file = File(bytes, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                string.Format("CustomerChargeDetail{5}_{0}{1}{2}{3}{4}.xlsx",
                    DateTime.UtcNow.Year, DateTime.UtcNow.Month, DateTime.UtcNow.Day, DateTime.UtcNow.Hour,
                    DateTime.UtcNow.Minute, model.RevAccountNumber));
            return file;
        }

        public FileContentResult Export(string selectedSessions = "", string accountNumber = "")
        {
            // neeed be change
            if (permissionManager.UserCannotAccess(Session, ModuleEnum.CustomerCharge))
            {
                return null;
            }
            var siteType = SiteType.Rev;
            if (!permissionManager.UserCanAccess(Session, ModuleEnum.RevCustomers))
            {
                siteType = SiteType.AMOP;
            }
            var model = new CustomerChargeExportModel(altaWrxDb, permissionManager, accountNumber, selectedSessions, siteType);

            Zipper zipper = new Zipper();
            foreach (var item in model.DetailList)
            {
                var data = item.Value.ToDataSet("CustomerChargeDetail");
                var bytes = ExcelUtilities.CustomExportForCustomerCharge(data, new List<int> { 7, 8, 9, 10, 11, 16, 17 }, new List<int> { 18 }, new List<int> { 3, 4, 5 });
                zipper.AddFile(new MemoryStream(bytes), string.Empty, string.Format("CustomerChargeDetail_{5}_{0}{1}{2}{3}{4}.xlsx",
           DateTime.UtcNow.Year, DateTime.UtcNow.Month, DateTime.UtcNow.Day, DateTime.UtcNow.Hour,
                DateTime.UtcNow.Minute, item.Key));
            }
            foreach (var item in model.DetailListNonRev)
            {
                var data = item.Value.ToDataSet("CustomerChargeDetail");
                var bytes = ExcelUtilities.CustomExportForCustomerCharge(data, new List<int> { 7, 8, 9, 10, 11, 16, 17 }, new List<int> { 18 }, new List<int> { 3, 4, 5 });
                zipper.AddFile(new MemoryStream(bytes), string.Empty, string.Format("CustomerChargeDetail_{5}_{0}{1}{2}{3}{4}.xlsx",
           DateTime.UtcNow.Year, DateTime.UtcNow.Month, DateTime.UtcNow.Day, DateTime.UtcNow.Hour,
                DateTime.UtcNow.Minute, item.Key));
            }
            byte[] zipData = zipper.GetStream();
            return File(zipData, "application/octet-stream", string.Format("CustomerChargeDetail_{0}{1}{2}{3}{4}.zip",
            DateTime.UtcNow.Year, DateTime.UtcNow.Month, DateTime.UtcNow.Day, DateTime.UtcNow.Hour, DateTime.UtcNow.Minute));
        }
        public ActionResult CreateConfirm(Guid sessionId, long id)
        {
            if (permissionManager.UserCannotAccess(Session, ModuleEnum.CustomerCharge))
            {
                return RedirectToAction("Index", "Home");
            }

            ViewBag.PageTitle = "Create Customer Charges";

            // get tenant custom fields
            var customObjectDbList = permissionManager.CustomFields;
            var awsAccessKey = AwsAccessKeyFromCustomObjects(customObjectDbList);
            var awsSecretAccessKey = AwsSecretAccessKeyFromCustomObjects(customObjectDbList);
            var createCustomerChargeQueueName = CreateCustomerChargeQueueFromCustomObjects(customObjectDbList);

            // validate custom fields
            if (string.IsNullOrWhiteSpace(awsAccessKey) || string.IsNullOrWhiteSpace(awsSecretAccessKey) ||
                string.IsNullOrWhiteSpace(createCustomerChargeQueueName))
            {
                SessionHelper.SetAlert(Session, LogCommonStrings.UNABLE_TO_CREATE_CHARGE_AWS_IS_NOT_SETUP);
                SessionHelper.SetAlertType(Session, CommonConstants.DANGER);
            }

            var model = new CreateSessionCustomerChargeModel(altaWrxDb, sessionId, permissionManager.PermissionFilter.GetSiteIdFilter(), permissionManager.Tenant.id, permissionManager, id);

            if (model.RevProductTypeId == null && model.RevProductId == null && model.CreateCustomerChargeInstanceList.FirstOrDefault(x => x.InstanceId == id)?.AMOPCustomerId == null)
            {
                SessionHelper.SetAlert(Session, LogCommonStrings.UNABLE_TO_CREATE_CHARGE_REV_PRODUCT_OR_PRODUCT_TYPE_HAS_NOT_BEEN_SET);
                SessionHelper.SetAlertType(Session, CommonConstants.DANGER);
            }

            if (model.SmsRevProductTypeId == null && model.SmsRevProductId == null && model.CreateCustomerChargeInstanceList.FirstOrDefault(x => x.InstanceId == id)?.AMOPCustomerId == null)
            {
                SessionHelper.SetAlert(Session, LogCommonStrings.UNABLE_TO_CREATE_CHARGE_REV_SMS_PRODUCT_OR_PRODUCT_TYPE_HAS_NOT_BEEN_SET);
                SessionHelper.SetAlertType(Session, CommonConstants.DANGER);
            }

            return View(CommonConstants.CREATE_CONFIRM, model);
        }

        public ActionResult Create(Guid sessionId, long id)
        {
            if (permissionManager.UserCannotAccess(Session, ModuleEnum.CustomerCharge))
            {
                return RedirectToAction("Index", "Home");
            }

            ViewBag.PageTitle = "Create Customer Charges";

            // get tenant custom fields
            var customObjectDbList = permissionManager.CustomFields;
            var awsAccessKey = AwsAccessKeyFromCustomObjects(customObjectDbList);
            var awsSecretAccessKey = AwsSecretAccessKeyFromCustomObjects(customObjectDbList);
            var createCustomerChargeQueueName = CreateCustomerChargeQueueFromCustomObjects(customObjectDbList);

            // validate custom fields
            if (string.IsNullOrWhiteSpace(awsAccessKey) || string.IsNullOrWhiteSpace(awsSecretAccessKey) ||
                string.IsNullOrWhiteSpace(createCustomerChargeQueueName))
            {
                SessionHelper.SetAlert(Session, LogCommonStrings.UNABLE_TO_CREATE_CHARGE_AWS_IS_NOT_SETUP);
                SessionHelper.SetAlertType(Session, CommonConstants.DANGER);

                var model = new CreateSessionCustomerChargeModel(altaWrxDb, sessionId, permissionManager.PermissionFilter.GetSiteIdFilter(), permissionManager.Tenant.id, permissionManager, id);
                return View(CommonConstants.CREATE_CONFIRM, model);
            }
            var tenantId = permissionManager.Tenant.id;

            // send to rev
            var errorMessage =
                EnqueueCreateCustomerChargesSqs(id, awsAccessKey, awsSecretAccessKey, createCustomerChargeQueueName, altaWrxDb, tenantId, 1, 1, id.ToString());
            if (!string.IsNullOrWhiteSpace(errorMessage))
            {
                SessionHelper.SetAlert(Session, errorMessage);
                SessionHelper.SetAlertType(Session, CommonConstants.DANGER);

                var model = new CreateSessionCustomerChargeModel(altaWrxDb, sessionId, permissionManager.PermissionFilter.GetSiteIdFilter(), permissionManager.Tenant.id, permissionManager, id);
                return View(CommonConstants.CREATE_CONFIRM, model);
            }

            SessionHelper.SetAlert(Session, LogCommonStrings.SUCCESSFULLY_PUSHED_CHARGE);
            SessionHelper.SetAlertType(Session, CommonConstants.SUCCESS);

            return RedirectToAction(CommonConstants.CUSTOMER_CHARGE_CONFIRM, new { id });
        }

        [HttpGet]
        public ActionResult CreateConfirmSession(Guid sessionId)
        {
            if (permissionManager.UserCannotAccess(Session, ModuleEnum.CustomerCharge))
            {
                return RedirectToAction("Index", "Home");
            }

            ViewBag.PageTitle = "Create Customer Charges for Session";

            // get tenant custom fields
            var customObjectDbList = permissionManager.CustomFields;
            var awsAccessKey = AwsAccessKeyFromCustomObjects(customObjectDbList);
            var awsSecretAccessKey = AwsSecretAccessKeyFromCustomObjects(customObjectDbList);
            var createCustomerChargeQueueName = CreateCustomerChargeQueueFromCustomObjects(customObjectDbList);

            // validate custom fields
            if (string.IsNullOrWhiteSpace(awsAccessKey) || string.IsNullOrWhiteSpace(awsSecretAccessKey) ||
                string.IsNullOrWhiteSpace(createCustomerChargeQueueName))
            {
                SessionHelper.SetAlert(Session,
                    "Unable to create charges. AWS setup is not complete for this Partner. Please complete AWS setup and return to this page.");
                SessionHelper.SetAlertType(Session, "danger");
            }

            var model = new CreateSessionCustomerChargeModel(altaWrxDb, sessionId, permissionManager.PermissionFilter.GetSiteIdFilter(), permissionManager.Tenant.id, permissionManager, null);

            if (model.RevProductTypeId == null && model.RevProductId == null && !model.HasNonRevCustomer)
            {
                SessionHelper.SetAlert(Session, LogCommonStrings.UNABLE_TO_CREATE_CHARGE_REV_PRODUCT_OR_PRODUCT_TYPE_HAS_NOT_BEEN_SET);
                SessionHelper.SetAlertType(Session, CommonConstants.DANGER);
            }

            if (model.SmsRevProductTypeId == null && model.SmsRevProductId == null && !model.HasNonRevCustomer)
            {
                SessionHelper.SetAlert(Session, LogCommonStrings.UNABLE_TO_CREATE_CHARGE_REV_SMS_PRODUCT_OR_PRODUCT_TYPE_HAS_NOT_BEEN_SET);
                SessionHelper.SetAlertType(Session, CommonConstants.DANGER);
            }

            // if there is only one customer in the list, just take them to the single page
            if (model.CreateCustomerChargeInstanceList != null && model.CreateCustomerChargeInstanceList.Count == 1)
            {
                return View(CommonConstants.CREATE_CONFIRM, model);
            }

            return View(model);
        }

        [ValidateAntiForgeryToken]
        [HttpPost]
        public async Task<ActionResult> CreateConfirmSession(Guid sessionId, string selectedInstances, string selectedUsageInstances, string pushType)
        {
            if (permissionManager.UserCannotAccess(Session, ModuleEnum.CustomerCharge))
            {
                return RedirectToAction("Index", "Home");
            }

            ViewBag.PageTitle = "Create Customer Charges for Session";
            var errorMessage = string.Empty;
            var hasErrorWhenPushingCharge = false;
            var hasErrorWhenPushingUsage = false;
            var isPushingForSingleCustomer = false;
            long chargeInstanceId = 0;
            var isCrossProviderCustomerOptimization = false;
            if (permissionManager.OptimizationSettings != null && permissionManager.OptimizationSettings.OptIntoCrossProviderCustomerOptimization)
            {
                var optimizationSession = altaWrxDb.OptimizationSessions.Where(x => x.SessionId.Equals(sessionId)).FirstOrDefault();
                if (optimizationSession == null)
                {
                    Log.Info($"{errorMessage} Session not found.");
                    errorMessage = $"{errorMessage}\n Session not found.";
                    hasErrorWhenPushingCharge = true;
                }
                else if (!string.IsNullOrWhiteSpace(optimizationSession.ServiceProviderIds))
                {
                    isCrossProviderCustomerOptimization = true;
                }
            }

            if (string.IsNullOrWhiteSpace(pushType) || (pushType != CommonConstants.CHARGES && pushType != CommonConstants.USAGE && pushType != CommonConstants.BOTH))
            {
                Log.Info($"{LogCommonStrings.UNABLE_TO_PUSH_WITHOUT_PUSH_TYPE}");
                errorMessage = LogCommonStrings.UNABLE_TO_PUSH_WITHOUT_PUSH_TYPE;
                hasErrorWhenPushingCharge = true;
                hasErrorWhenPushingUsage = true;
            }

            var customObjectDbList = permissionManager.CustomFields;

            if (pushType == CommonConstants.CHARGES || pushType == CommonConstants.BOTH)
            {
                // get tenant custom fields
                var awsAccessKey = AwsAccessKeyFromCustomObjects(customObjectDbList);
                var awsSecretAccessKey = AwsSecretAccessKeyFromCustomObjects(customObjectDbList);
                var createCustomerChargeQueueName = CreateCustomerChargeQueueFromCustomObjects(customObjectDbList);
                var createCDRCustomerChargeQueueName = CreateCDRCustomerChargeQueueFromCustomObjects(customObjectDbList);
                var pushCustomerChargeType = PushCustomerChargeTypeFromCustomObjects(customObjectDbList);

                // validate custom fields
                if (string.IsNullOrWhiteSpace(awsAccessKey) || string.IsNullOrWhiteSpace(awsSecretAccessKey) ||
                    string.IsNullOrWhiteSpace(createCustomerChargeQueueName))
                {
                    Log.Info($"{LogCommonStrings.UNABLE_TO_PUSH_CHARGES_AWS_IS_NOT_SETUP}");
                    errorMessage = $"{errorMessage}\n{LogCommonStrings.UNABLE_TO_PUSH_CHARGES_AWS_IS_NOT_SETUP}";
                    hasErrorWhenPushingCharge = true;
                }

                // validate selected instances
                var instances = selectedInstances?.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
                if (instances == null || instances.Length == 0)
                {
                    Log.Info($"{LogCommonStrings.UNABLE_TO_PUSH_CHARGES_WITH_NO_INSTANCES}");
                    errorMessage = $"{errorMessage}\n{LogCommonStrings.UNABLE_TO_PUSH_CHARGES_WITH_NO_INSTANCES}";
                    hasErrorWhenPushingCharge = true;
                }

                long[] instanceIds;
                try
                {
                    if (!hasErrorWhenPushingCharge)
                    {
                        instanceIds = instances.Select(x => long.Parse(x.Replace("\"", ""))).ToArray();
                        if (string.Equals(pushCustomerChargeType, PushCustomerChargeType.CDR, StringComparison.OrdinalIgnoreCase))
                        {
                            var enqueueChargesErrorMessage = await InitializeUploadCustomerChargeCDRs(sessionId, awsAccessKey, awsSecretAccessKey, createCDRCustomerChargeQueueName, instanceIds, isCrossProviderCustomerOptimization);
                            if (!string.IsNullOrWhiteSpace(enqueueChargesErrorMessage))
                            {
                                Log.Info($"{errorMessage} {enqueueChargesErrorMessage}");
                                errorMessage = $"{errorMessage}\n{enqueueChargesErrorMessage}";
                                hasErrorWhenPushingCharge = true;
                            }
                        }
                        else
                        {
                            foreach (var item in instanceIds.Select((instanceId, index) => new { instanceId, index }))
                            {
                                var isLastInstanceId = 0;
                                if (item.index == instanceIds.Length - 1)
                                {
                                    isLastInstanceId = 1;
                                }
                                var tenantId = permissionManager.Tenant.id;
                                var enqueueCreateCustomerChargeErrorMessage = EnqueueCreateCustomerChargesWithSesstionSqs(item.instanceId, awsAccessKey, awsSecretAccessKey, createCustomerChargeQueueName, altaWrxDb, tenantId, 1, isLastInstanceId, string.Join(",", instanceIds));
                                if (!string.IsNullOrWhiteSpace(enqueueCreateCustomerChargeErrorMessage))
                                {
                                    Log.Info($"{errorMessage} {enqueueCreateCustomerChargeErrorMessage}");
                                    errorMessage = $"{errorMessage}\n{enqueueCreateCustomerChargeErrorMessage}";
                                    hasErrorWhenPushingCharge = true;
                                    break;
                                }
                            }
                        }
                    }
                }
                catch (InvalidCastException)
                {
                    Log.Info($"{LogCommonStrings.UNABLE_TO_PUSH_CHARGES_WITH_INVALID_INSTANCES}");
                    errorMessage = $"{errorMessage}\n{LogCommonStrings.UNABLE_TO_PUSH_CHARGES_WITH_INVALID_INSTANCES}";
                    hasErrorWhenPushingCharge = true;
                }
            }

            if (pushType == CommonConstants.USAGE || pushType == CommonConstants.BOTH)
            {
                // get tenant custom fields for Rev.io FTP
                var revFTPHost = RevFTPHostFromCustomObjects(customObjectDbList);
                var revFTPUsername = RevFTPUsernameFromCustomObjects(customObjectDbList);
                var revFTPPassword = RevFTPPasswordFromCustomObjects(customObjectDbList);
                var revFTPPath = RevFTPPathFromCustomObjects(customObjectDbList);

                // validate custom fields for Rev.io FTP
                if (string.IsNullOrWhiteSpace(revFTPHost) || string.IsNullOrWhiteSpace(revFTPUsername) ||
                    string.IsNullOrWhiteSpace(revFTPPassword) || string.IsNullOrWhiteSpace(revFTPPath))
                {
                    Log.Info($"{LogCommonStrings.UNABLE_TO_PUSH_USAGE_REV_FTP_IS_NOT_SETUP}");
                    errorMessage = $"{errorMessage}\n{LogCommonStrings.UNABLE_TO_PUSH_USAGE_REV_FTP_IS_NOT_SETUP}";
                    hasErrorWhenPushingUsage = true;
                }

                // validate usage instances
                var instances = selectedUsageInstances?.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
                if (instances == null || instances.Length == 0)
                {
                    Log.Info($"{LogCommonStrings.UNABLE_TO_PUSH_USAGE_WITH_NO_INSTANCES}");
                    errorMessage = $"{errorMessage}\n{LogCommonStrings.UNABLE_TO_PUSH_USAGE_WITH_NO_INSTANCES}";
                    hasErrorWhenPushingUsage = true;
                }
                try
                {
                    if (!hasErrorWhenPushingUsage)
                    {
                        var createSessionCustomerCharge = new CreateSessionCustomerChargeModel(altaWrxDb, sessionId, permissionManager.PermissionFilter.GetSiteIdFilter(), permissionManager.Tenant.id, permissionManager, null);
                        var dataToUpload = createSessionCustomerCharge.CreateCustomerUsageInstanceList
                            .Where(x => instances.Contains(x.ICCID))
                            .Select(x => new UploadDeviceUsageByLineModel()
                            {
                                BilledNumber = x.IntegrationId == (int)IntegrationType.Pond ? x.ICCID : x.MSISDN,
                                CallDate = createSessionCustomerCharge.BillingPeriodEnd.ToString(CommonConstants.AMOP_DATE_TIME_FORMAT),
                                CarrierRateType = CommonConstants.CARRIER_RATE_TYPE,
                                Kilobytes = (decimal)x.DataUsageMB * 1024,
                            }).ToList();
                        string fileName = $"UsagePush_{FileNameTimestamp()}.csv";
                        StringBuilder stringBuilder = new StringBuilder();
                        PropertyInfo[] properties = typeof(UploadDeviceUsageByLineModel).GetProperties();
                        foreach (PropertyInfo property in properties)
                        {
                            var displayAttribute = property.GetCustomAttribute<DisplayNameAttribute>();
                            if (displayAttribute != null)
                            {
                                stringBuilder.Append(displayAttribute.DisplayName + ",");
                            }
                        }
                        stringBuilder.Remove(stringBuilder.Length - 1, 1).AppendLine();
                        foreach (var usage in dataToUpload)
                        {
                            stringBuilder.Append($"{usage.BilledCountry},{usage.BilledNumber},{usage.CallDate},{usage.OtherCountry},{usage.OtherNumber},{usage.Seconds},{usage.CarrierCode},{usage.CarrierRateType},{usage.Charge},{usage.Kilobytes}");
                            stringBuilder.AppendLine();
                        }
                        byte[] fileByte = Encoding.UTF8.GetBytes(stringBuilder.ToString());
                        RevFTPHelper.SendUsagePushToRevFTP(fileByte, fileName, revFTPHost, revFTPUsername, revFTPPassword, revFTPPath);
                        if (!isCrossProviderCustomerOptimization)
                        {
                            var session = altaWrxDb.vwOptimizationSessions.FirstOrDefault(x => x.SessionId == sessionId);
                            var serviceProvider = altaWrxDb.ServiceProviders.FirstOrDefault(sp => sp.id == session.ServiceProviderId);
                            var portalType = (PortalTypes)serviceProvider.Integration.PortalTypeId;
                            var usageByLineIds = createSessionCustomerCharge.CreateCustomerUsageInstanceList.Where(x => instances.Contains(x.ICCID)).Select(x => x.DeviceHistoryId).ToList();
                            if (portalType == PortalTypes.M2M)
                            {
                                var usageByLines = altaWrxDb.DeviceHistories.Where(x => usageByLineIds.Contains(x.DeviceHistoryId)).ToList();
                                foreach (var usageByLine in usageByLines)
                                {
                                    usageByLine.IsPushed = true;
                                    altaWrxDb.Entry(usageByLine).State = EntityState.Modified;
                                }
                            }
                            else
                            {
                                var usageByLines = altaWrxDb.MobilityDeviceHistories.Where(x => usageByLineIds.Contains(x.DeviceHistoryId)).ToList();
                                foreach (var usageByLine in usageByLines)
                                {
                                    usageByLine.IsPushed = true;
                                    altaWrxDb.Entry(usageByLine).State = EntityState.Modified;
                                }
                            }
                            altaWrxDb.SaveChanges();
                        }
                    }
                }
                catch (Exception ex)
                {
                    Log.Error($"{LogCommonStrings.ERROR_PUSHING_USAGE} {ex.Message}");
                    errorMessage = $"{errorMessage}\n{LogCommonStrings.ERROR_PUSHING_USAGE}";
                    hasErrorWhenPushingUsage = true;
                }
            }

            var chargeInstances = selectedInstances?.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);

            if (chargeInstances?.Length == 1)
            {
                isPushingForSingleCustomer = true;
                chargeInstanceId = chargeInstances.Select(x => long.Parse(x.Replace("\"", ""))).ToArray()[0];
            }

            if (hasErrorWhenPushingCharge || hasErrorWhenPushingUsage)
            {
                SessionHelper.SetAlert(Session, errorMessage);
                SessionHelper.SetAlertType(Session, CommonConstants.DANGER);
                if (isPushingForSingleCustomer)
                {
                    if (pushType == CommonConstants.USAGE)
                    {
                        return RedirectToAction(CommonConstants.CREATE_CONFIRM, new { sessionId, id = chargeInstanceId });
                    }
                    return RedirectToAction(CommonConstants.CUSTOMER_CHARGE_CONFIRM, new { id = chargeInstanceId });
                }
                return RedirectToAction(CommonConstants.CREATE_CONFIRM_SESSION, new { sessionId });
            }

            SessionHelper.SetAlert(Session, LogCommonStrings.SUCCESSFULLY_PUSHED_CHARGES_AND_USAGE);
            SessionHelper.SetAlertType(Session, CommonConstants.SUCCESS);
            if (isPushingForSingleCustomer)
            {
                if (pushType == CommonConstants.USAGE)
                {
                    return RedirectToAction(CommonConstants.CREATE_CONFIRM, new { sessionId, id = chargeInstanceId });
                }
                return RedirectToAction(CommonConstants.CUSTOMER_CHARGE_CONFIRM, new { id = chargeInstanceId });
            }
            if (pushType == CommonConstants.CHARGES || pushType == CommonConstants.BOTH)
            {
                return RedirectToAction(CommonConstants.CUSTOMER_CHARGE_SESSION_CONFIRM, new { sessionId });
            }
            else
            {
                return RedirectToAction(CommonConstants.CREATE_CONFIRM_SESSION, new { sessionId });
            }
        }

        public FileContentResult ExportCustomerChargesAndUsagesForSession([QueryString] Guid sessionId)
        {
            if (permissionManager.UserCannotAccess(Session, ModuleEnum.CustomerCharge))
            {
                return null;
            }

            var createSessionCustomerCharge = new CreateSessionCustomerChargeModel(altaWrxDb, sessionId, permissionManager.PermissionFilter.GetSiteIdFilter(), permissionManager.Tenant.id, permissionManager, null);
            var exportCharges = createSessionCustomerCharge.CreateCustomerChargeInstanceList
                .Where(x => x.OverageChargeTotal > 0 && x.CustomerChargeQueueCount == null)
                .Select(x => CustomerChargeMappingExtensions.CustomerChargeCreationExport(x)).ToList();
            var exportUsage = createSessionCustomerCharge.CreateCustomerUsageInstanceList
                .Select(x => CustomerChargeMappingExtensions.CustomerUsageCreationExport(x)).ToList();

            byte[] fileByte;

            using (ExcelPackage package = new ExcelPackage())
            {
                ExcelWorksheet chargeWorksheet = package.Workbook.Worksheets.Add(CommonConstants.CHARGES);
                chargeWorksheet.Cells["A1"].LoadFromCollection(exportCharges, true);
                ExcelWorksheet usageWorksheet = package.Workbook.Worksheets.Add(CommonConstants.USAGE);
                usageWorksheet.Cells["A1"].LoadFromCollection(exportUsage, true);
                fileByte = package.GetAsByteArray();
            }

            return File(fileByte, ExcelContentType, $"CustomerChargesAndUsagesForSession_{FileNameTimestamp()}.{ExcelFileExtension}");
        }

        public ActionResult CustomerChargeSessionConfirm(Guid sessionId)
        {
            if (permissionManager.UserCannotAccess(Session, ModuleEnum.CustomerCharge))
            {
                return RedirectToAction("Index", "Home");
            }

            ViewBag.PageTitle = "Customer Charge Upload for Session";

            var model = new CreateSessionCustomerChargeModel(altaWrxDb, sessionId, permissionManager.PermissionFilter.GetSiteIdFilter(), permissionManager.Tenant.id, permissionManager, null);
            return View(model);
        }

        public ActionResult CustomerChargeConfirm(long id)
        {
            if (permissionManager.UserCannotAccess(Session, ModuleEnum.CustomerCharge))
            {
                return RedirectToAction("Index", "Home");
            }

            ViewBag.PageTitle = "Customer Charge Upload";
            var isCrossProviderCustomerOptimization = false;
            if (permissionManager.OptimizationSettings != null && permissionManager.OptimizationSettings.OptIntoCrossProviderCustomerOptimization)
            {
                isCrossProviderCustomerOptimization = true;
            }

            var model = new CreateCustomerChargeModel(altaWrxDb, id, isCrossProviderCustomerOptimization);
            return View(model);
        }

        public ActionResult UploadConfirm(long id)
        {
            if (permissionManager.UserCannotAccess(Session, ModuleEnum.CustomerCharge))
            {
                return RedirectToAction("Index", "Home");
            }

            var siteType = SiteType.Rev;
            if (!permissionManager.UserCanAccess(Session, ModuleEnum.RevCustomers))
            {
                siteType = SiteType.AMOP;
            }

            ViewBag.PageTitle = "Upload Customer Charges";

            // get tenant custom fields
            var customObjectDbList = permissionManager.CustomFields;
            var awsAccessKey = AwsAccessKeyFromCustomObjects(customObjectDbList);
            var awsSecretAccessKey = AwsSecretAccessKeyFromCustomObjects(customObjectDbList);
            var deviceCustomerChargeQueueName = CustomerChargeQueueFromCustomObjects(customObjectDbList);

            // validate custom fields
            if (string.IsNullOrWhiteSpace(awsAccessKey) || string.IsNullOrWhiteSpace(awsSecretAccessKey) ||
                string.IsNullOrWhiteSpace(deviceCustomerChargeQueueName))
            {
                SessionHelper.SetAlert(Session,
                    "Unable to upload. AWS setup is not complete for this Partner. Please complete AWS setup and return to this page.");
                SessionHelper.SetAlertType(Session, "danger");
            }

            var model = new CustomerChargeDetailModel(altaWrxDb, id, siteType);
            return View(model);
        }

        [HttpPost]
        public ActionResult Upload(FormCollection collection)
        {
            ViewBag.PageTitle = "Customer Charges Upload";

            if (!permissionManager.UserCanEdit(Session, ModuleEnum.M2M) && !permissionManager.UserCanEdit(Session, ModuleEnum.Mobility))
            {
                return RedirectToAction("Index", "Home");
            }

            if (Request.Files.Count == 0)
            {
                return Content("OK");
            }

            var uploadedFile = Request.Files[0];
            if (uploadedFile?.FileName == null)
            {
                return ErrorMessage("Empty file name. Must select a valid file to process.");
            }

            if (Path.GetExtension(uploadedFile.FileName).ToUpper() != ".CSV")
            {
                return ErrorMessage($"Invalid File: {uploadedFile.FileName}.  The file must be in .CSV format.");
            }

            if (uploadedFile.FileName.Length > 255)
            {
                return ErrorMessage(
                    $"Invalid File: {uploadedFile.FileName}.  The filename must be less than 255 characters long.");
            }

            // A file name must be unique.  Check to see if the file name exists in [dbo].[JasperDeviceStatus_UploadedFile] already.
            bool fileExists;
            fileExists = altaWrxDb.CustomerCharge_UploadedFile.Any(f =>
                f.FileName.Replace(" ", "").ToLower() == uploadedFile.FileName.ToLower());
            if (fileExists)
            {
                return ErrorMessage(
                    $"'{uploadedFile.FileName}' was processed already and exists in the uploaded customer charges list. File name must be unique.");
            }

            try
            {
                using (var streamReader = new StreamReader(uploadedFile.InputStream))
                {
                    var csvConfiguration = new CsvConfiguration(CultureInfo.InvariantCulture, prepareHeaderForMatch: Helpers.CsvHelper.PrepareHeaderForMatch);
                    using (var csv = new CsvReader(streamReader, csvConfiguration))
                    {
                        IEnumerable<CustomerChargeCsvRow> lines;
                        try
                        {
                            lines = csv.GetRecords<CustomerChargeCsvRow>().ToList();
                        }
                        catch (Exception e)
                        {
                            var exceptionMessage =
                                $"Error processing file '{uploadedFile.FileName}'. Error: {e.GetBaseException().Message}";
                            return ErrorMessage(exceptionMessage);
                        }

                        if (!lines.Any()) // There must be at least one line after the header.
                        {
                            return ErrorMessage($"Customer charge information was not found in '{uploadedFile.FileName}'.");
                        }

                        var tenantRepository = new TenantRepository(db);
                        var tenant = tenantRepository.GetParentTenantId(permissionManager.Tenant.id);
                        var revIntegrationId = (int)IntegrationType.RevIO;
                        var intAuth = altaWrxDb.Integration_Authentication.FirstOrDefault(x => x.TenantId == tenant.id && x.IntegrationId == revIntegrationId);

                        if (intAuth == null)
                        {
                            var errorMessage = "This tenant does not have valid credentials for Rev.IO or Catapult. Please contact the admin to enter credentials and enable this feature.";
                            return ErrorMessage(errorMessage);
                        }

                        var revServiceRepo = new Repositories.Rev.RevServiceRepository(altaWrxDb, intAuth.id);
                        var revServices = revServiceRepo.GetAll();

                        var missingServiceNumbers = lines
                            .Where(line => !revServices.Any(s => string.Equals(s.Number, line.RevIoServiceNumber,
                                StringComparison.InvariantCultureIgnoreCase)))
                            .Select(line => line.RevIoServiceNumber).ToList();
                        if (missingServiceNumbers.Any())
                        {
                            var errorMessage =
                                $"'{uploadedFile.FileName}' contains invalid Rev.IO Service Numbers. Missing Service Numbers: ${string.Join(", ", missingServiceNumbers)}";
                            return ErrorMessage(errorMessage);
                        }

                        var revProductTypeRepo = new RevProductTypeRepository(altaWrxDb);
                        var revProductTypeIds = revProductTypeRepo.GetAllProductTypeIds(tenant.id);
                        var missingProductTypes = lines
                            .Where(line => !revProductTypeIds.Any(s => s == line.RevIoProductTypeId))
                            .Select(line => line.RevIoProductTypeId).ToList();
                        if (missingProductTypes.Any())
                        {
                            var errorMessage =
                                $"'{uploadedFile.FileName}' contains invalid Rev.IO Product Types. Missing Product Types: ${string.Join(", ", missingProductTypes)}";
                            return ErrorMessage(errorMessage);
                        }

                        var appFileRepository = new AppFileRepository(altaWrxDb);
                        try
                        {
                            var awsFile = UploadFileToAWS(uploadedFile);
                            if (!string.IsNullOrEmpty(awsFile))
                            {
                                var appFile = new Models.Repositories.AppFile
                                {
                                    AmazonFileName = awsFile,
                                    FileName = uploadedFile.FileName,
                                    TenantId = permissionManager.Tenant.id,
                                    CreatedBy = user.Name,
                                    CreatedDate = DateTime.UtcNow,
                                    IsActive = true,
                                    IsDeleted = false
                                };
                                appFileRepository.SaveNew(Session, appFile);
                            }
                        }
                        catch (Exception ex)
                        {
                            return ErrorMessage($"Could not save the file to S3 bucket. {ex.Message}");
                        }

                        var billingPeriodRepo = new Repositories.BillingPeriod.BillingPeriodRepository(altaWrxDb);
                        var deviceTenantRepo = new DeviceTenantRepository(altaWrxDb, tenant.id);
                        var m2mDevicesWithService = deviceTenantRepo.GetAllM2MRevServiceIds(intAuth.id);
                        var m2mServices = revServices
                            .Where(rs => m2mDevicesWithService.FirstOrDefault(x => x.RevServiceId.Value == rs.id) != null)
                            .Select(rs => new M2MCustomerChargeUploadRecord()
                            {
                                M2MDeviceRevServiceRecord = m2mDevicesWithService.First(x => x.RevServiceId.Value == rs.id),
                                RevService = rs
                            })
                            .ToList();

                        var mobilityDeviceTenantRepo = new MobilityDeviceTenantRepository(altaWrxDb, tenant.id);
                        var mobilityDevicesWithService = mobilityDeviceTenantRepo.GetAllMobilityRevServiceIds(intAuth.id);
                        var mobilityServices = revServices
                            .Where(rs => mobilityDevicesWithService.FirstOrDefault(x => x.RevServiceId.Value == rs.id) != null)
                            .Select(rs => new MobilityCustomerChargeUploadRecord()
                            {
                                MobilityDeviceRevServiceRecord = mobilityDevicesWithService.First(x => x.RevServiceId.Value == rs.id),
                                RevService = rs
                            })
                            .ToList();
                        var m2mLines = lines
                            .Where(line =>
                                m2mServices.Any(s => string.Equals(s.RevService.Number, line.RevIoServiceNumber, StringComparison.InvariantCultureIgnoreCase))
                            )
                            .Select(line =>
                                m2mServices.First(s => string.Equals(s.RevService.Number, line.RevIoServiceNumber, StringComparison.InvariantCultureIgnoreCase)).AddCsvRow(line)
                            )
                            .ToList();
                        var mobilityLines = lines
                            .Where(line =>
                                mobilityServices.Any(s => string.Equals(s.RevService.Number, line.RevIoServiceNumber, StringComparison.InvariantCultureIgnoreCase))
                            )
                            .Select(line =>
                                mobilityServices.First(s => string.Equals(s.RevService.Number, line.RevIoServiceNumber, StringComparison.InvariantCultureIgnoreCase)).AddCsvRow(line)
                            )
                            .ToList();
                        var customerChargeFileRepository = new CustomerChargeUploadedFileRepository(altaWrxDb);
                        var savedFile = customerChargeFileRepository.Create(Session, uploadedFile.FileName, intAuth.id, appFileRepository.Object.id);
                        var m2mQueueEntries = m2mLines.Select(r => MapOptimizationDeviceResultCustomerChargeQueue(billingPeriodRepo, r, savedFile, intAuth));
                        var mobilityQueueEntries = mobilityLines.Select(r => MapOptimizationMobilityDeviceResultCustomerChargeQueue(billingPeriodRepo, r, savedFile, intAuth));
                        try
                        {
                            using (var transaction = altaWrxDb.Database.BeginTransaction())
                            {
                                var customObjectDbList = permissionManager.CustomFields;
                                var awsAccessKey = AwsAccessKeyFromCustomObjects(customObjectDbList);
                                var awsSecretAccessKey = AwsSecretAccessKeyFromCustomObjects(customObjectDbList);
                                var deviceCustomerChargeQueueName = CustomerChargeQueueFromCustomObjects(customObjectDbList);

                                altaWrxDb.OptimizationDeviceResult_CustomerChargeQueue.AddRange(m2mQueueEntries);
                                altaWrxDb.OptimizationMobilityDeviceResult_CustomerChargeQueue.AddRange(mobilityQueueEntries);
                                altaWrxDb.SaveChanges();
                                var response = EnqueueCustomerChargesSqs(savedFile.id, awsAccessKey, awsSecretAccessKey,
                                    deviceCustomerChargeQueueName);
                                if (string.IsNullOrEmpty(response))
                                {
                                    transaction.Commit();
                                }
                                else
                                {
                                    return ErrorMessage($"Could not save the file. {response}");
                                }
                            }
                        }
                        catch (DbUpdateException ex)
                        {
                            return ErrorMessage($"Could not save the file. {ex.Message}");
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                return ErrorMessage($"Error adding the file. {ex.Message}");
            }

            SessionHelper.SetAlert(Session, "Successfully uploaded customer charges.");
            SessionHelper.SetAlertType(Session, "success");
            return Content("OK");
        }

        private string CustomerChargeQueueFromCustomObjects(IList<CustomObject> customObjectDbList)
        {
            return ValueFromCustomObjects(customObjectDbList, CUSTOMER_CHARGE_QUEUE_NAME);
        }

        private string CreateCustomerChargeQueueFromCustomObjects(IList<CustomObject> customObjectDbList)
        {
            return ValueFromCustomObjects(customObjectDbList, CREATE_CUSTOMER_CHARGE_QUEUE_NAME);
        }

        private static string EnqueueCustomerChargesSqs(int fileId, string awsAccessKey, string awsSecretAccessKey,
            string deviceCustomerChargeQueueName)
        {
            try
            {
                var awsCredentials = new BasicAWSCredentials(awsAccessKey, awsSecretAccessKey);
                using (var client = new AmazonSQSClient(awsCredentials, RegionEndpoint.USEast1))
                {
                    var queueList = client.ListQueues(deviceCustomerChargeQueueName);
                    if (queueList.HttpStatusCode == HttpStatusCode.OK && queueList.QueueUrls != null &&
                        queueList.QueueUrls.Count > 0)
                    {
                        var requestMsgBody = $"File to work is {fileId}";
                        var request = new SendMessageRequest
                        {
                            MessageAttributes = new Dictionary<string, MessageAttributeValue>
                            {
                                {
                                    "FileId", new MessageAttributeValue
                                        {DataType = "String", StringValue = fileId.ToString()}
                                }
                            },
                            MessageBody = requestMsgBody,
                            QueueUrl = queueList.QueueUrls[0]
                        };

                        var response = client.SendMessageAsync(request);
                        response.Wait();
                        if (response.Status == TaskStatus.Faulted || response.Status == TaskStatus.Canceled)
                        {
                            return $"Error Queuing Charges: {response.Status}";
                        }

                        // success
                        return string.Empty;
                    }

                    return "Error Queuing Charges: Queue not found";
                }
            }
            catch (Exception ex)
            {
                Log.Error($"Error Queuing Charges for {fileId}", ex);
                return "Error Queue Charges: Exception occured";
            }
        }

        private static string EnqueueCreateCustomerChargesSqs(long instanceId, string awsAccessKey,
            string awsSecretAccessKey,
            string createCustomerChargeQueueName, AltaWorxCentral_Entities altaWrxDb, int tenantId, int isMultipleInstanceId = 0, int isLastInstanceId = 0, string instanceIds = "")
        {
            try
            {
                var awsCredentials = new BasicAWSCredentials(awsAccessKey, awsSecretAccessKey);
                var integrationAuthenticationRepository = new IntegrationAuthenticationRepository(altaWrxDb);
                var integrationAuthentication = integrationAuthenticationRepository.GetAuthByIntegrationId(IntegrationEnum.RevIO.AsInt(), tenantId);
                using (var client = new AmazonSQSClient(awsCredentials, RegionEndpoint.USEast1))
                {
                    var queueList = client.ListQueues(createCustomerChargeQueueName);
                    if (queueList.HttpStatusCode == HttpStatusCode.OK && queueList.QueueUrls != null &&
                        queueList.QueueUrls.Count > 0)
                    {
                        var requestMsgBody = $"Instance to work is {instanceId}";
                        var request = new SendMessageRequest
                        {
                            MessageAttributes = new Dictionary<string, MessageAttributeValue>
                            {
                                {
                                    "InstanceId", new MessageAttributeValue
                                        {DataType = "String", StringValue = instanceId.ToString()}
                                },
                                {
                                    "IsMultipleInstanceId", new MessageAttributeValue {DataType = "String", StringValue = isMultipleInstanceId.ToString()}

                                },
                                {
                                    "IsLastInstanceId", new MessageAttributeValue {DataType = "String", StringValue = isLastInstanceId.ToString()}
                                },
                                {
                                    "InstanceIds", new MessageAttributeValue {DataType = "String", StringValue = instanceIds}
                                },
                                {
                                    "CurrentIntegrationAuthenticationId", new MessageAttributeValue {DataType = "String", StringValue = integrationAuthentication.id.ToString()}
                                }
                            },
                            MessageBody = requestMsgBody,
                            QueueUrl = queueList.QueueUrls[0]
                        };

                        var response = client.SendMessageAsync(request);
                        response.Wait();
                        if (response.Status == TaskStatus.Faulted || response.Status == TaskStatus.Canceled)
                        {
                            return $"Error Queuing Charges: {response.Status}";
                        }

                        // success
                        return string.Empty;
                    }

                    return "Error Queuing Charges: Queue not found";
                }
            }
            catch (Exception ex)
            {
                Log.Error($"Error Queuing Charges for {instanceId}", ex);
                return "Error Queue Charges: Exception occured";
            }
        }

        private static string EnqueueCreateCustomerChargesWithSesstionSqs(long instanceId, string awsAccessKey,
            string awsSecretAccessKey, string createCustomerChargeQueueName, AltaWorxCentral_Entities altaWrxDb, int tenantId, int isMultipleInstanceId = 0, int isLastInstanceId = 0, string instanceIds = "")
        {
            try
            {
                var awsCredentials = new BasicAWSCredentials(awsAccessKey, awsSecretAccessKey);
                var integrationAuthenticationRepository = new IntegrationAuthenticationRepository(altaWrxDb);
                var integrationAuthentication = integrationAuthenticationRepository.GetAuthByIntegrationId(IntegrationEnum.RevIO.AsInt(), tenantId);
                using (var client = new AmazonSQSClient(awsCredentials, RegionEndpoint.USEast1))
                {
                    var queueList = client.ListQueues(createCustomerChargeQueueName);
                    if (queueList.HttpStatusCode == HttpStatusCode.OK && queueList.QueueUrls != null &&
                        queueList.QueueUrls.Count > 0)
                    {
                        var requestMsgBody = $"Instance to work is {instanceId}";
                        var request = new SendMessageRequest
                        {
                            DelaySeconds = isLastInstanceId == 1 ? 90 : 0,
                            MessageAttributes = new Dictionary<string, MessageAttributeValue>
                            {
                                {
                                    "InstanceId", new MessageAttributeValue {DataType = "String", StringValue = instanceId.ToString()}
                                },
                                {
                                    "IsMultipleInstanceId", new MessageAttributeValue {DataType = "String", StringValue = isMultipleInstanceId.ToString()}

                                },
                                {
                                    "IsLastInstanceId", new MessageAttributeValue {DataType = "String", StringValue = isLastInstanceId.ToString()}
                                },
                                {
                                    "InstanceIds", new MessageAttributeValue {DataType = "String", StringValue = instanceIds}
                                },
                                {
                                    "CurrentIntegrationAuthenticationId", new MessageAttributeValue {DataType = "String", StringValue = integrationAuthentication.id.ToString()}
                                }
                            },
                            MessageBody = requestMsgBody,
                            QueueUrl = queueList.QueueUrls[0]
                        };
                        var response = client.SendMessageAsync(request);
                        response.Wait();
                        if (response.Status == TaskStatus.Faulted || response.Status == TaskStatus.Canceled)
                        {
                            return $"Error Queuing Charges: {response.Status}";
                        }

                        // success
                        return string.Empty;
                    }

                    return "Error Queuing Charges: Queue not found";
                }
            }
            catch (Exception ex)
            {
                Log.Error($"Error Queuing Charges for {instanceId}", ex);
                return "Error Queue Charges: Exception occured";
            }
        }

        private async Task<string> EnqueueUploadCustomerChargeCDRs(string awsAccessKey, int portalTypeId, string awsSecretAccessKey, string CustomerChargeFileQueueName, long[] instanceIds, long queueId)
        {
            try
            {
                var awsCredentials = new BasicAWSCredentials(awsAccessKey, awsSecretAccessKey);
                using (var client = new AmazonSQSClient(awsCredentials, RegionEndpoint.USEast1))
                {
                    var queueList = client.ListQueues(CustomerChargeFileQueueName);
                    var queueUrl = queueList.QueueUrls[0];
                    if (queueList.HttpStatusCode == HttpStatusCode.OK && queueList.QueueUrls != null && queueList.QueueUrls.Count > 0)
                    {
                        var requestMsgBody = string.Format(LogCommonStrings.SENDING_SQS_MESSAGE_TO_URL, queueUrl);
                        var request = new SendMessageRequest
                        {
                            MessageAttributes = new Dictionary<string, MessageAttributeValue>
                            {
                                {
                                    SQSMessageKeyConstant.QUEUE_ID, new MessageAttributeValue
                                    { DataType = nameof(String), StringValue = queueId.ToString()}
                                },
                                {
                                    SQSMessageKeyConstant.PORTAL_TYPE_ID, new MessageAttributeValue {DataType = nameof(String), StringValue = portalTypeId.ToString()}
                                },
                                {
                                    SQSMessageKeyConstant.INSTANCE_IDS, new MessageAttributeValue {DataType = nameof(String), StringValue = string.Join(",", instanceIds)}
                                }
                            },
                            MessageBody = requestMsgBody,
                            QueueUrl = queueUrl
                        };
                        var response = await RetryPolicyHelper.PollyRetryForSQSMessage().ExecuteAsync(async () => await client.SendMessageAsync(request).ConfigureAwait(false)).ConfigureAwait(false);
                        // Log error on any 4xx or 5xx response statuses
                        if (response.HttpStatusCode >= HttpStatusCode.BadRequest)
                        {
                            return $"{LogCommonStrings.ERROR_WHILE_QUEUING_CHARGES}: {response.HttpStatusCode:d}";
                        }
                        return string.Empty;
                    }
                    return $"{LogCommonStrings.ERROR_WHILE_QUEUING_CHARGES}: {string.Format(LogCommonStrings.QUEUE_NOT_FOUND, CustomerChargeFileQueueName)}";
                }
            }
            catch (Exception ex)
            {
                Log.Error($"{LogCommonStrings.ERROR_WHILE_QUEUING_CHARGES}", ex);
                return $"{LogCommonStrings.ERROR_WHILE_QUEUING_CHARGES}: {ex.Message}";
            }
        }

        private string UploadFileToAWS(HttpPostedFileBase uploadedFile)
        {
            var customObjectDbList = permissionManager.CustomFields;
            string awsAccessKey = AwsAccessKeyFromCustomObjects(customObjectDbList);
            string awsSecretAccessKey = AwsSecretAccessKeyFromCustomObjects(customObjectDbList);
            string s3BucketName = S3BucketNameFromCustomObject(customObjectDbList);

            var credentials = new BasicAWSCredentials(awsAccessKey, awsSecretAccessKey);
            S3Wrapper s3wrapper = new S3Wrapper(credentials, s3BucketName);
            var awsFile = s3wrapper.UploadAwsFile(uploadedFile.InputStream, null);
            return awsFile;
        }

        private string S3BucketNameFromCustomObject(IList<CustomObject> customObjectDbList)
        {
            return ValueFromCustomObjects(customObjectDbList, S3_BUCKET_NAME);
        }

        private static MemoryCache billingPeriodByServiceProviderAndDate = MemoryCache.Default;
        private static object cacheLockObject = new object();
        private BillingPeriod GetBillingPeriod(Repositories.BillingPeriod.BillingPeriodRepository repo, int serviceProviderId, DateTime billingPeriodEndDate)
        {
            var cacheKey = $"{serviceProviderId}_{billingPeriodEndDate.ToShortDateString()}";
            lock (cacheLockObject)
            {
                if (billingPeriodByServiceProviderAndDate.Contains(cacheKey))
                {
                    return (BillingPeriod)billingPeriodByServiceProviderAndDate[cacheKey];
                }
                else
                {
                    var billingPeriod = repo.GetBillingPeriodByServiceProviderAndDate(serviceProviderId, billingPeriodEndDate);

                    CacheItemPolicy policy = new CacheItemPolicy
                    {
                        AbsoluteExpiration = DateTime.UtcNow + TimeSpan.FromMinutes(10)
                    };

                    billingPeriodByServiceProviderAndDate.Add(cacheKey, billingPeriod, policy);
                    return billingPeriod;
                }
            }
        }

        private OptimizationDeviceResult_CustomerChargeQueue MapOptimizationDeviceResultCustomerChargeQueue(Repositories.BillingPeriod.BillingPeriodRepository repo, M2MCustomerChargeUploadRecord record, CustomerCharge_UploadedFile uploadedFile, Integration_Authentication integrationAuth)
        {
            var billingPeriod = GetBillingPeriod(repo, record.M2MDeviceRevServiceRecord.ServiceProviderId, record.CustomerChargeCsvRow.BillingEndDate);

            return new OptimizationDeviceResult_CustomerChargeQueue
            {
                RevProductTypeId = record.CustomerChargeCsvRow.RevIoProductTypeId,
                UploadedFileId = uploadedFile.id,
                ChargeAmount = record.CustomerChargeCsvRow.OverageChargeAmount,
                BaseChargeAmount = record.CustomerChargeCsvRow.BaseChargeAmount,
                TotalChargeAmount = record.CustomerChargeCsvRow.BaseChargeAmount + record.CustomerChargeCsvRow.OverageChargeAmount,
                CreatedBy = SessionHelper.GetAuditByName(Session),
                CreatedDate = DateTime.UtcNow,
                RevServiceNumber = record.CustomerChargeCsvRow.RevIoServiceNumber,
                Description = record.CustomerChargeCsvRow.Description,
                BillingStartDate = record.CustomerChargeCsvRow.BillingStartDate,
                BillingEndDate = record.CustomerChargeCsvRow.BillingEndDate,
                IntegrationAuthenticationId = integrationAuth.id,
                BillingPeriodId = billingPeriod?.id,
                SmsChargeAmount = record.CustomerChargeCsvRow.SmsChargeAmount,
                SmsRevProductTypeId = record.CustomerChargeCsvRow.SmsRevIoProductTypeId
            };
        }

        private OptimizationMobilityDeviceResult_CustomerChargeQueue MapOptimizationMobilityDeviceResultCustomerChargeQueue(Repositories.BillingPeriod.BillingPeriodRepository repo, MobilityCustomerChargeUploadRecord record, CustomerCharge_UploadedFile uploadedFile, Integration_Authentication integrationAuth)
        {
            var billingPeriod = GetBillingPeriod(repo, record.MobilityDeviceRevServiceRecord.ServiceProviderId, record.CustomerChargeCsvRow.BillingEndDate);

            return new OptimizationMobilityDeviceResult_CustomerChargeQueue
            {
                RevProductTypeId = record.CustomerChargeCsvRow.RevIoProductTypeId,
                UploadedFileId = uploadedFile.id,
                ChargeAmount = record.CustomerChargeCsvRow.OverageChargeAmount,
                BaseChargeAmount = record.CustomerChargeCsvRow.BaseChargeAmount,
                TotalChargeAmount = record.CustomerChargeCsvRow.BaseChargeAmount + record.CustomerChargeCsvRow.OverageChargeAmount,
                CreatedBy = SessionHelper.GetAuditByName(Session),
                CreatedDate = DateTime.UtcNow,
                RevServiceNumber = record.CustomerChargeCsvRow.RevIoServiceNumber,
                Description = record.CustomerChargeCsvRow.Description,
                BillingStartDate = record.CustomerChargeCsvRow.BillingStartDate,
                BillingEndDate = record.CustomerChargeCsvRow.BillingEndDate,
                IntegrationAuthenticationId = integrationAuth.id,
                BillingPeriodId = billingPeriod?.id,
                SmsChargeAmount = record.CustomerChargeCsvRow.SmsChargeAmount,
                SmsRevProductTypeId = record.CustomerChargeCsvRow.SmsRevIoProductTypeId
            };
        }

        private async Task<string> InitializeUploadCustomerChargeCDRs(Guid sessionId, string awsAccessKey, string awsSecretAccessKey, string createCDRCustomerChargeQueueName, long[] instanceIds, bool isCrossProviderCustomerOptimization)
        {
            var session = altaWrxDb.vwOptimizationSessions.FirstOrDefault(x => x.SessionId == sessionId);
            if (session == null)
            {
                return string.Format(LogCommonStrings.SESSION_ID_NOT_FOUND, sessionId);
            }
            int portalTypeId = 0;
            if (session.ServiceProviderId != null)
            {
                var serviceProvider = altaWrxDb.ServiceProviders.FirstOrDefault(sp => sp.id == session.ServiceProviderId);
                if (serviceProvider == null)
                {
                    return string.Format(LogCommonStrings.SERVICE_PROVIDER_ID_NOT_FOUND, session.ServiceProviderId);
                }
                else
                {
                    portalTypeId = serviceProvider.Integration.PortalTypeId;
                }
            }
            else
            {
                portalTypeId = (int)PortalTypes.CrossProvider;
            }
            var customerChargeRepository = new RevCustomerChargeRepository();
            customerChargeRepository.CreateCDRCustomerChargeQueues(altaWrxDb, instanceIds, portalTypeId, isCrossProviderCustomerOptimization);
            var queueIds = altaWrxDb.CustomerChargeQueueToProcesses.AsNoTracking().Select(x => x.QueueId).ToList();
            var enqueueErrorMessageBuilder = new StringBuilder();
            foreach (var queueId in queueIds)
            {
                var enqueueErrorMessage = await EnqueueUploadCustomerChargeCDRs(awsAccessKey, portalTypeId, awsSecretAccessKey, createCDRCustomerChargeQueueName, instanceIds, queueId);
                if (!string.IsNullOrEmpty(enqueueErrorMessage))
                {
                    enqueueErrorMessageBuilder.AppendLine(enqueueErrorMessage);
                }
            }
            return enqueueErrorMessageBuilder.ToString();
        }
    }
}

namespace AltaworxRevAWSCreateCustomerChange.Models
{
    public class CustomerChargeResponse
    {
        public int ChargeId { get; set; }
        public bool HasErrors { get; set; }
        public string ErrorMessage { get; set; }
        public int StatusCode { get; set; }
    }
}
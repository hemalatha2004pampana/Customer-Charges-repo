using System;

namespace AltaworxRevAWSCreateCustomerChange.Models
{
    public class CustomerChargeUploadedFile
    {
        public int Id { get; set; }
        public string FileName { get; set; }
        public string Status { get; set; }
        public string Description { get; set; }
        public DateTime? ProcessedDate { get; set; }
        public string ProcessedBy { get; set; }
        public string CreatedBy { get; set; }
        public DateTime CreatedDate { get; set; }
        public string ModifiedBy { get; set; }
        public DateTime? ModifiedDate { get; set; }
        public string DeletedBy { get; set; }
        public DateTime? DeletedDate { get; set; }
        public bool IsDeleted { get; set; }
        public bool IsActive { get; set; }
        public int? IntegrationAuthenticationId { get; set; }
    }
}
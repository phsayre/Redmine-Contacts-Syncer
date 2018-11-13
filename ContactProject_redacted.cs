using System;
using System.Data;
namespace SetonProjectsSyncer
{
    class ContactProject
    {
        public string pid { get; set; }
        public string cid { get; set; }
        public static ContactProject Create(IDataRecord record)
        {
            return new ContactProject
            {
                pid = record["project_id"].ToString(),
                cid = record["contact_id"].ToString()
            };
        }
    }
}
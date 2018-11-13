using System;
using System.Data;
namespace SetonProjectsSyncer
{
    class Contact
    {
        public string id { get; set; }
        public string fname { get; set; }
        public string lname { get; set; }
        public string email { get; set; }
        public string phone { get; set; }
        public string city { get; set; }
        public string state { get; set; }
        public string zip { get; set; }
        public string tags { get; set; }
        public string created { get; set; }
        public string edited { get; set; }
        public static bool IsEmptyAddress(IDataRecord record)
        {
            if((record["city"].ToString() == "" || record["city"].ToString() == " ")
                && (record["state"].ToString() == "" || record["state"].ToString() == " ")
                && (record["zip"].ToString() == "" || record["zip"].ToString() == " "))
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        public static Contact Create(IDataRecord record)
        {
            // Use the 'first name' index to determine which column names to use when building the contact.
            switch (record.GetName(0))
            { 
                // MySQL
                case "first_name":
                    // Format the zip code to be a uniform length of 5 characters.
                    string zipMySQLFormatString = record["postcode"].ToString();
                    zipMySQLFormatString = zipMySQLFormatString.Substring(0, Math.Min(zipMySQLFormatString.Length, 5));
                    return new Contact
                    {
                        fname = record["first_name"].ToString().ToUpper(),
                        lname = record["last_name"].ToString().ToUpper(),
                        email = record["email"].ToString().ToUpper(),
                        phone = record["phone"].ToString().ToUpper(),
                        city = record["city"].ToString().ToUpper(),
                        state = record["region"].ToString().ToUpper(),
                        zip = zipMySQLFormatString.ToUpper(),
                        tags = record["cached_tag_list"].ToString(),
                        created = record["created_on"].ToString().ToUpper(),
                        edited = record["updated_on"].ToString().ToUpper(),
                        id = record["id"].ToString().ToUpper()
                    };
#pragma warning disable CS0162 // Unreachable code detected
                    break;
#pragma warning restore CS0162 // Unreachable code detected
                // PostgreSQL
                case "fname":
                    // The custnum and famnum tags must be formatted to match Redmine's format of cust:XXXXXX and fam:XXXXXXX.
                    // Note: custnum and famnum length varies. We should not assume every custnum and famnum will be the same length.
                    string tagFormatString;
                    if (Convert.ToDouble(record["famnum"]) != 0.00 && !String.IsNullOrEmpty(record["famnum"].ToString())){
                        string famnumFormatString = record["famnum"].ToString();
                        // Note: famnum ends with .00 on redacted_list - this must be chopped off to match Redmine format.
                        famnumFormatString = famnumFormatString.Remove(famnumFormatString.Length - 3, 3);
                        tagFormatString = "cust:" + record["custnum"].ToString() + ", " + "fam:" + famnumFormatString;
                    }
                    else
                    {
                        tagFormatString = "cust:" + record["custnum"].ToString();
                    }
                    string stateString;
                    // If there is no state set in the redacted_list, set state to XX so it can still work with the "Show on map" button in Redmine.
                    if (!IsEmptyAddress(record) && (record["state"].ToString() == "" || record["state"].ToString() == " "))
                    {
                        stateString = "XX";
                    }
                    else
                    {
                        stateString = record["state"].ToString().ToUpper();
                    }
                    // Format the zip code to be a uniform length of 5 characters.
                    string zipPostgreSQLFormatString = record["zip"].ToString();
                    zipPostgreSQLFormatString = zipPostgreSQLFormatString.Substring(0, Math.Min(zipPostgreSQLFormatString.Length, 5));
                    return new Contact
                    {
                        fname = record["fname"].ToString().ToUpper(),
                        lname = record["lname"].ToString().ToUpper(),
                        email = record["email"].ToString().ToUpper(),
                        phone = record["phone"].ToString().ToUpper(),
                        city = record["city"].ToString().ToUpper(),
                        state = stateString,
                        zip = zipPostgreSQLFormatString.ToUpper(),
                        tags = tagFormatString,
                        created = record["createdate"].ToString().ToUpper(),
                        edited = record["editdate"].ToString().ToUpper(),
                        id = record["recno"].ToString().ToUpper()
                    };
#pragma warning disable CS0162 // Unreachable code detected
                    break;
#pragma warning restore CS0162 // Unreachable code detected
            }
            return new Contact
            {
                fname = "error",
                lname = "error",
                email = "error"
            };
        }
    }
}
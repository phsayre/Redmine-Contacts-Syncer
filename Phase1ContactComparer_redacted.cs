using System;
using System.Collections.Generic;
namespace SetonProjectsSyncer
{
    class Phase1ContactComparer : IEqualityComparer<Contact>
    {
        public static string RemoveWhitespace(string str) { return string.Join("", str.Split(default(string[]), StringSplitOptions.RemoveEmptyEntries)); }
        public bool Equals(Contact c1, Contact c2)
        { 
            string c1Lname = RemoveWhitespace(c1.lname);
            string c2Lname = RemoveWhitespace(c2.lname);
            string c1Email = RemoveWhitespace(c1.email);
            string c2Email = RemoveWhitespace(c2.email);
            string c1Tags = RemoveWhitespace(c1.tags);
            string c2Tags = RemoveWhitespace(c2.tags);
;            if (c1 == null && c2 == null)
            {
                return true;
            }
            // Not checking for 'state' because of inconsistencies between redacted_list and redmine's way of storing the state value. 
            else if ((c1.fname == c2.fname) && (c1Lname == c2Lname) 
                && (c1Email == c2Email) && (c1.phone == c2.phone)
                && (c1.city == c2.city) 
                && (c1.zip == c2.zip) && (c1Tags == c2Tags))
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        public int GetHashCode(Contact contact)
        {
            int hCode = 13;
            // A unique string that will be exactly the same for matching contacts.
            string identifierString;
            if (contact.email != "" && contact.email != " ")
            {
                // Use the email if it exists, because it is the most reliable indicator of matching contacts.
                identifierString = contact.email;
            }
            else
            {
                // The email does not exist, so we use the last name instead.
                identifierString = contact.lname;
            }
            // The hCode must be something that generates the exact same code for matching contacts
            //otherwise, Equals() will always return false.
            // Note that two contacts may have the same hCode and still return false - that is acceptable.
            hCode = (hCode * 7) + identifierString.GetHashCode();
            return hCode;
        }
    }
}
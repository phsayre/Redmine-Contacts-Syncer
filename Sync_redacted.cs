using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Text;
namespace SetonProjectsSyncer
{
    class Sync
    {
        private ContactsGenMySql mySqlContactsGenerator;
        private ContactsGenPostgres postgresContactsGenerator;
        // The list phase1ShallowCompareList contains unique Redmine contacts that don't perfectly match any redacted_list contact.
        private List<Contact> phase1ShallowCompareList;
        // The list phase2NonDupsReadyToSyncList contains redacted_list contacts that link with Redmine equivalents, and don't have possible duplicates.
        private List<Contact> phase2NonDupsReadyToSyncList;
        // The list phase3PossibleDupsList contains redacted_list contacts that link with Redmine equivalents, but are possibly duplicates with varying custnums/famnums.
        private List<Contact> phase3PossibleDupsList;
        // The list phase4DupsReadyToSyncList contains redacted_list contacts that have been combined with duplicates, retaining all unique tags
        // and have been re-compared with Redmine contacts to ensure these contacts actually need updated. 
        private List<Contact> phase4DupsReadyToSyncList;
        // The list cornerCase1ReadyToSyncList contains redacted_list contacts that link with a Redmine equivalents that do not have a last name set, but do have an email.
        private List<Contact> cornerCase1ReadyToSyncList;
        // The Dictionary combinedTags contains all tags associated with possible duplicate contacts. 
        private Dictionary<string, string> combinedTags;
        private String connectionStringMySql;
        private MySqlCommand cmd;
        private static MySqlConnection conn;
        public Sync()
        {
            try
            {
                Stopwatch stopWatch = new Stopwatch();
                stopWatch.Start();
                WriteOut.HandleMessage("START");
                // A switch for associating contacts.
                int associateSwitch = Int32.Parse(ConfigurationManager.AppSettings["associateSwitch"]);
                combinedTags = new Dictionary<string, string>();
                GenerateContacts();
                CompareContacts();
                if (phase2NonDupsReadyToSyncList.Count() >= 1)
                {
                    // Sync nonduplicate contacts.
                    SyncContacts(1);
                }
                if(phase4DupsReadyToSyncList.Count() >= 1)
                {
                    // Sync duplicate-combined contacts.
                    SyncContacts(2);
                }
                if(cornerCase1ReadyToSyncList.Count() >= 1)
                {
                    // Sync corner case contacts.
                    SyncContacts(3);
                }
                if(associateSwitch == 1)
                {
                    // Generate contacts a second time so they are up-to-date for the associating process.
                    GenerateContacts();
                    AssociateContactsWithProjects();
                }
                stopWatch.Stop();
                WriteOut.HandleMessage("RunTime: " + String.Format("{0:00}:{1:00}.{2:00}", stopWatch.Elapsed.Minutes, stopWatch.Elapsed.Seconds, stopWatch.Elapsed.Milliseconds));
                WriteOut.HandleMessage("END");
            }
            catch(Exception e)
            {
                // Get stack trace for the exception with source file information.
                var st = new StackTrace(e, true);
                WriteOut.HandleMessage("Error: " + e.Message);
                WriteOut.HandleMessage("StackTrace: " + st);
            }
        }
        private void GenerateContacts()
        {
            mySqlContactsGenerator = new ContactsGenMySql();
            postgresContactsGenerator = new ContactsGenPostgres();
        }
        private void CompareContacts()
        {
            var mySqlContacts = mySqlContactsGenerator.GetContacts().ToList();
            var postgresContacts = postgresContactsGenerator.GetContacts().ToList();
            // Return contacts that don't perfectly match any redacted_list contact.
            phase1ShallowCompareList = mySqlContacts.Except<Contact>(postgresContacts, new Phase1ContactComparer()).ToList();
            WriteOut.HandleMessage("Phase1: " + phase1ShallowCompareList.Count().ToString());
            // Return redacted_list contacts that link with Redmine equivalents, and don't have possible duplicates.
            phase2NonDupsReadyToSyncList = postgresContacts.Intersect<Contact>(phase1ShallowCompareList, new Phase2ContactComparer()).ToList();
            WriteOut.HandleMessage("Phase2: " + phase2NonDupsReadyToSyncList.Count().ToString());
            // Return redacted_list contacts that link with Redmine equivalents, but are possibly duplicates with varying custnums / famnums.
            phase3PossibleDupsList = postgresContacts.Intersect<Contact>(phase1ShallowCompareList, new Phase3ContactComparer()).ToList();
            // Return redacted_list contacts with combined tags taken from duplicate contacts. 
            phase3PossibleDupsList = CombineTags(phase3PossibleDupsList, postgresContacts);
            WriteOut.HandleMessage("Phase3: " + phase3PossibleDupsList.Count().ToString());
            // Compare phase3 with Redmine contacts again to ensure these contacts actually need updated. 
            phase4DupsReadyToSyncList = phase3PossibleDupsList.Except<Contact>(mySqlContacts, new Phase1ContactComparer()).ToList();
            WriteOut.HandleMessage("Phase4: " + phase4DupsReadyToSyncList.Count().ToString());
            // Return contacts that link with Redmine equivalents based only on email and last name being "-".
            cornerCase1ReadyToSyncList = postgresContacts.Intersect<Contact>(phase1ShallowCompareList, new CornerCase1ContactComparer()).ToList();
            WriteOut.HandleMessage("Corner1: " + cornerCase1ReadyToSyncList.Count().ToString());
            WriteOut.HandleMessage("Updated: " + (phase2NonDupsReadyToSyncList.Count() + phase4DupsReadyToSyncList.Count() + cornerCase1ReadyToSyncList.Count()).ToString());
            PrintInfo(phase2NonDupsReadyToSyncList);
            PrintInfo(phase4DupsReadyToSyncList);
            PrintInfo(cornerCase1ReadyToSyncList);
        }
        private void AssociateContactsWithProjects()
        {
            var mySqlContacts = mySqlContactsGenerator.GetContacts().ToList();
            var postgresContacts = postgresContactsGenerator.GetContacts().ToList();
            var contactsToAssociatePhase1 = mySqlContacts.Intersect<Contact>(postgresContacts, new Phase1ContactComparer()).ToList();
            var contactsProjectsFromGen = new ContactsProjectsFromGen();
            // A list of row data from table contacts_projects matching the specified project_id.
            var contactsProjectsFrom = contactsProjectsFromGen.GetContactsProjects().ToList();
            // Select from all potential contacts the contacts that match the specified 'from' project_id(s).
            var contactsToAssociatePhase2 = contactsToAssociatePhase1.Select(p1 => p1).Where(p1 => contactsProjectsFrom.Select(cF => cF.cid).Contains(p1.id));
            // Comma delimmited project-to IDs, set in App.config.
            var contactsProjectsTo = ConfigurationManager.AppSettings["contactsProjectsTo"].Split(',');
            // A list of commands that will insert the contact associations.
            var cmdList = new List<string>();
            var insertContactsProjectsCmd = new StringBuilder();
            foreach(var contact in contactsToAssociatePhase2)
            {
                foreach (var pid in contactsProjectsTo)
                {
                    // This command only inserts a new record if one does not already exist.
                    insertContactsProjectsCmd.AppendFormat("INSERT INTO contacts_projects (project_id, contact_id) SELECT * FROM (SELECT {0}, {1}) AS tmp WHERE NOT EXISTS (SELECT project_id, contact_id FROM contacts_projects WHERE project_id={0} AND contact_id={1}) LIMIT 1;",
                        pid, contact.id);
                    cmdList.Add(insertContactsProjectsCmd.ToString());
                    insertContactsProjectsCmd.Clear();
                }
            }
            SetupMySqlConnection();
            int counter = 0;
            insertContactsProjectsCmd.Clear();
            // Concatenate every 10 insert command strings and run the command.
            foreach(var cmdString in cmdList)
            {
                if(counter < 10)
                {
                    insertContactsProjectsCmd.Append(cmdString);
                    counter++;
                }
                else
                {
                    InsertContactAssociations(insertContactsProjectsCmd.ToString());
                    insertContactsProjectsCmd.Clear();
                    insertContactsProjectsCmd.Append(cmdString);
                    counter = 1;
                }
            }
            WriteOut.HandleMessage("Associated: " + cmdList.Count().ToString());
            conn.Close();
        }
        private void InsertContactAssociations(string insertCommand)
        {
            cmd = new MySqlCommand(insertCommand);
            cmd.Connection = conn;
            cmd.ExecuteNonQuery();
        }
        private void PrintInfo(List<Contact> contacts)
        {
            foreach (var contact in contacts)
            {
                WriteOut.HandleMessage("        Contact: " + contact.email + " | " + contact.lname + " | " + contact.tags);
            }
        }
        private List<Contact> CombineTags(List<Contact> possibleDuplicateContacts, List<Contact> redacted_list)
        {
            foreach(var c1 in possibleDuplicateContacts)
            {
                combinedTags[c1.email] = c1.tags;
                foreach(var c2 in redacted_list)
                {
                    // If the contacts match, combine tags.
                    // Do not combine tags after 150 characters worth of tags have been combined. 
                    if(c1.lname == c2.lname && c1.email == c2.email && c1.tags != c2.tags && combinedTags[c1.email].Length <= 150)
                    {
                        combinedTags[c1.email] += ", " + c2.tags;
                    }
                }
            }
            foreach(var contact in possibleDuplicateContacts)
            {
                contact.tags = combinedTags[contact.email];
            }
            return possibleDuplicateContacts;
        }
        private void SyncContacts(int switcher)
        {
            SetupMySqlConnection();
            switch (switcher)
            {
                // Update phase2 contacts.
                case 1:
                    foreach (var contact in phase2NonDupsReadyToSyncList)
                    {
                        UpdateContact(contact, switcher);
                    }
                    break;
                // Update phase4 contacts.
                case 2:
                    foreach (var contact in phase4DupsReadyToSyncList)
                    {
                        UpdateContact(contact, switcher);
                    }
                    break;
                // Update cornerCase1 contacts.
                case 3:
                    foreach (var contact in cornerCase1ReadyToSyncList)
                    {
                        UpdateContact(contact, switcher);
                    }
                    break;
            }
            conn.Close();
        }
        private void SetupMySqlConnection()
        {
            try
            {
                connectionStringMySql = ConfigurationManager.AppSettings["connectionStringMySql"];
                conn = new MySqlConnection(connectionStringMySql);
                conn.Open();
            }
            catch (Exception e)
            {
                WriteOut.HandleMessage(e.Message);
            }
        }
        public static string RemoveWhitespace(string str) { return string.Join("", str.Split(default(string[]), StringSplitOptions.RemoveEmptyEntries)); }
        private void UpdateContact(Contact contact, int switcher)
        {
            string selectAddressCountCmd;
            string selectTaggablesCountCmd;
            string updateAddressCmd;
            string updatePersonalCmd;
            string updateTagsCmd = "";
            selectAddressCountCmd = String.Format("SELECT COUNT(*) FROM addresses WHERE addressable_id=\"{0}\";", contact.id);
            cmd = new MySqlCommand(selectAddressCountCmd);
            cmd.Connection = conn;
            // Determine if an address record exists for this contact.
            if (Convert.ToInt32(cmd.ExecuteScalar()) == 0)
            {
                // Create an address record if necessary.
                string fullAddress = contact.city + ", " + contact.zip + ", " + contact.state;
                updateAddressCmd = String.Format("INSERT INTO addresses (city, region, postcode, full_address, address_type, addressable_id, addressable_type, created_at, updated_at) VALUES (\"{0}\", \"{1}\", \"{2}\", \"{3}\", \"{4}\", \"{5}\", \"{6}\" , Now(), Now());",
                    contact.city, contact.state, contact.zip, fullAddress, "business", contact.id, "Contact");
            }
            else 
            {
                // Update the address if it already exists.
                updateAddressCmd = String.Format("UPDATE addresses SET city=\"{0}\", region=\"{1}\", postcode=\"{2}\", updated_at=Now() WHERE addressable_id=\"{3}\";", contact.city, contact.state, contact.zip, contact.id);
            }
            //Contact last name with whitespace removed to deal with corner case comparisons.
            string lnameWhitespaceRemoved = RemoveWhitespace(contact.lname);
            switch (switcher)
            {
                // Update phase2 contacts.
                case 1:
                    updatePersonalCmd = String.Format("UPDATE contacts SET first_name=\"{0}\", last_name=\"{1}\", email=\"{2}\", phone=\"{3}\", cached_tag_list=\"{4}\", updated_on=Now() WHERE (last_name=\"{5}\" OR last_name=\"{6}\") AND email=\"{7}\";",
                        contact.fname, contact.lname, contact.email, contact.phone, contact.tags, contact.lname, lnameWhitespaceRemoved, contact.email);
                    cmd = new MySqlCommand(updatePersonalCmd + updateAddressCmd);
                    cmd.Connection = conn;
                    cmd.ExecuteNonQuery();
                    break;
                // Update phase4 contacts.
                case 2:
                    updatePersonalCmd = String.Format("UPDATE contacts SET first_name=\"{0}\", last_name=\"{1}\", email=\"{2}\", phone=\"{3}\", cached_tag_list=\"{4}\", updated_on=Now() WHERE (last_name=\"{5}\" OR last_name=\"{6}\") AND email=\"{7}\";",
                        contact.fname, contact.lname, contact.email, contact.phone, contact.tags, contact.lname, lnameWhitespaceRemoved, contact.email);
                    string[] tagsArray = contact.tags.Split(',');
                    // Concatenate all of the insert commands into one command string.
                    foreach (var tag in tagsArray)
                    {
                        // Insert a new tags record only if one does not already exist.
                        updateTagsCmd += String.Format("INSERT INTO tags (name) SELECT * FROM (SELECT \"{0}\") AS tmp WHERE NOT EXISTS (SELECT name FROM tags WHERE name = \"{0}\") LIMIT 1;", tag);
                    }
                    cmd = new MySqlCommand(updatePersonalCmd + updateAddressCmd + updateTagsCmd);
                    cmd.Connection = conn;
                    cmd.ExecuteNonQuery();
                    List<string> tagsIDList = new List<string>();
                    string getTagIDCmd;
                    string updateTaggingsCmd;
                    foreach(var tag in tagsArray)
                    {
                        // Get all tag IDs to update the taggings records.
                        getTagIDCmd = String.Format("SELECT id FROM tags WHERE name=\"{0}\";", tag);
                        cmd = new MySqlCommand(getTagIDCmd);
                        cmd.Connection = conn;
                        tagsIDList.Add(cmd.ExecuteScalar().ToString()); 
                    }
                    foreach(var tagID in tagsIDList)
                    {
                        selectTaggablesCountCmd = String.Format("SELECT COUNT(*) FROM taggings WHERE tag_id={0}", tagID);
                        cmd = new MySqlCommand(selectTaggablesCountCmd);
                        cmd.Connection = conn;
                        // Insert a new taggings record only if one does not already exist.
                        if(Convert.ToInt32(cmd.ExecuteScalar()) == 0)
                        {
                            updateTaggingsCmd = String.Format("INSERT INTO taggings (tag_id, taggable_id, taggable_type, created_at) VALUES (\"{0}\", \"{1}\", \"{2}\", Now());", tagID, contact.id, "Contact");
                        }
                        else
                        {
                            // Clear existing taggings associations with the contact.
                            updateTaggingsCmd = String.Format("UPDATE taggings SET taggable_id=NULL WHERE taggable_id=\"{0}\";", contact.id);
                            // Associate the correct taggings record with the contact.
                            updateTaggingsCmd += String.Format("UPDATE taggings SET taggable_id=\"{0}\" WHERE tag_id=\"{1}\";", contact.id, tagID);
                        }
                        cmd = new MySqlCommand(updateTaggingsCmd);
                        cmd.Connection = conn;
                        cmd.ExecuteNonQuery();
                    }
                    break;
                // Update cornerCase1 contacts.
                case 3:
                    updatePersonalCmd = String.Format("UPDATE contacts SET first_name=\"{0}\", last_name=\"{1}\", email=\"{2}\", phone=\"{3}\", cached_tag_list=\"{4}\", updated_on=Now() WHERE email=\"{5}\";",
                        contact.fname, contact.lname, contact.email, contact.phone, contact.tags, contact.email);
                    cmd = new MySqlCommand(updatePersonalCmd + updateAddressCmd);
                    cmd.Connection = conn;
                    cmd.ExecuteNonQuery();
                    break;
            }
        }
    }
}
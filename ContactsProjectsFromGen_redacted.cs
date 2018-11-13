using MySql.Data.MySqlClient;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
namespace SetonProjectsSyncer
{
    class ContactsProjectsFromGen : IEnumerable
    {
        private String connectionStringMySql;
        private static MySqlConnection conn;
        private MySqlCommand cmd;
        private IEnumerable<ContactProject> contactsProjects;
        // Comma delimmited project IDs, set in App.config.
        private string[] projectContactsFrom;
        public ContactsProjectsFromGen()
        {
            try
            {
                connectionStringMySql = ConfigurationManager.AppSettings["connectionStringMySql"];
                projectContactsFrom = ConfigurationManager.AppSettings["contactsProjectsFrom"].Split(',');
                conn = new MySqlConnection(connectionStringMySql);
                GenerateProjects();
            }
            catch (Exception e)
            {
                WriteOut.HandleMessage(e.Message);
            }
        }
        private void GenerateProjects()
        {
            try
            {
                contactsProjects = GetData(ReadQueryData(), ContactProject.Create);
            }
            catch (Exception e)
            {
                WriteOut.HandleMessage(e.Message);
            }
        }
        private MySqlDataReader ReadQueryData()
        {
            string selectContactsProjectsCmd;
            int counter;
            conn.Open();
            // Build the select statement, taking only the rows corresponding with the given project IDs.
            selectContactsProjectsCmd = "SELECT * FROM contacts_projects WHERE ";
            counter = 0;
            foreach (var projectID in projectContactsFrom)
            {
                if (counter >= 1)
                {
                    selectContactsProjectsCmd += " AND ";
                    counter++;
                }
                else
                {
                    counter++;
                }
                selectContactsProjectsCmd += "project_id=" + projectID;
            }
            selectContactsProjectsCmd += ";";
            cmd = new MySqlCommand(selectContactsProjectsCmd);
            cmd.Connection = conn;
            MySqlDataReader reader = cmd.ExecuteReader();
            return reader;
        }
        public IEnumerable<ContactProject> GetContactsProjects()
        {
            return contactsProjects;
        }
        public IEnumerable<ContactProject> ContactsProjects
        {
            get
            {
                return contactsProjects;
            }
        }
        IEnumerator IEnumerable.GetEnumerator()
        {
            return ContactsProjects.GetEnumerator();
        }
        public IEnumerable<T> GetData<T>(IDataReader reader, Func<IDataReader, T> BuildObject)
        {
            try
            {
                while (reader.Read())
                {
                    yield return BuildObject(reader);
                }
            }
            finally
            {
                reader.Dispose();
                conn.Close();
            }
        }
    }
}
using MySql.Data.MySqlClient;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
namespace SetonProjectsSyncer
{
    class ContactsGenMySql : IEnumerable
    {
        private String connectionStringMySql;
        private static MySqlConnection conn;
        private MySqlCommand cmd;
        private IEnumerable<Contact> contacts;
        public ContactsGenMySql()
        { 
            try
            {
                connectionStringMySql = ConfigurationManager.AppSettings["connectionStringMySql"];
                conn = new MySqlConnection(connectionStringMySql);
                GenerateContacts();
            }
            catch(Exception e)
            {
                WriteOut.HandleMessage(e.Message);
            }
        }
        private void GenerateContacts()
        {
            try
            {
                contacts = GetData(ReadQueryData(), Contact.Create);
            }
            catch(Exception e)
            {
                WriteOut.HandleMessage(e.Message);
            }
        }
        private MySqlDataReader ReadQueryData()
        {
            conn.Open();
            // The column first_name serves as a switch in Contact.cs and must be the first column selected, otherwise the switch logic will break.
            cmd = new MySqlCommand("SELECT first_name, last_name, email, cached_tag_list, phone, city, region, postcode, created_on, updated_on, contacts.id FROM contacts LEFT OUTER JOIN addresses ON contacts.id = addressable_id ORDER BY contacts.id DESC;");
            cmd.Connection = conn;
            MySqlDataReader reader = cmd.ExecuteReader();
            return reader;
        }
        public IEnumerable<Contact> GetContacts()
        {
            return contacts;
        }
        public IEnumerable<Contact> Contacts
        {
            get
            {
                return contacts;
            }
        }
        IEnumerator IEnumerable.GetEnumerator()
        {
            return Contacts.GetEnumerator();
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
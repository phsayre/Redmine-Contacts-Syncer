using Npgsql;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
namespace SetonProjectsSyncer
{
    class ContactsGenPostgres : IEnumerable
    {
        private String connectionStringPostgres;
        private static NpgsqlConnection conn;
        private NpgsqlCommand cmd;
        private IEnumerable<Contact> contacts;
        public ContactsGenPostgres()
        {
            try
            {
                connectionStringPostgres = ConfigurationManager.AppSettings["connectionStringPostgres"];
                conn = new NpgsqlConnection(connectionStringPostgres);
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
        public IDataReader ReadQueryData()
        {
            conn.Open();
            // The column fname serves as a switch in Contact.cs and must be the first column selected, otherwise the switch logic will break.
            cmd = new NpgsqlCommand("SET CLIENT_ENCODING = SQL_ASCII; SELECT redacted FROM redacted_list ORDER BY redacted DESC", conn);
            NpgsqlDataReader reader = cmd.ExecuteReader();
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
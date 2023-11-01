"""
    salesforce.py

    This handles the communications with Salesforce, accepting as input
    a SQL SELECT statement that has to be translated to SOQL, and then
    executing that SOQL statement.

    Along the way there's a bunch of other stuff this module does to
    support that high level goal.

"""
import os
from simple_salesforce import Salesforce, SalesforceExpiredSession
from plog import Plog
from sql2soql import sql2soql


class SalesforceFunctions:

    """
        Init defines all the SObjects we'll let GPT talk to and which fields of those objects
        it can access.  You want to limit this to fields that will either (a) be useful in
        joining parent/child data or (b) something the user will want to be told about.

        Naturally, this list is dependent upon your specific needs.
    """
    def __init__(self):

        self.sf: Salesforce = None

        self.user_id = None

        self.reconnect()

        self.row_limit = 5

        self.table2columns = {}

        self.table2columns['Account'] = ['AnnualRevenue', 'Description', 'Id', 'Industry',
                                         'Name', 'NumberOfEmployees', 'Ownership', 'ParentId',
                                         'Phone', 'PrimaryContactId__c', 'PrimaryContactId__r.Name', 'Sic', 'SicDesc',
                                         'Site', 'TickerSymbol', 'Type',
                                         'YearStarted']

        self.table2columns['Contact'] = ['AccountId', 'CreatedDate', 'Department', 'Description', 'FirstName',
                                         'LastName', 'Id', 'MobilePhone', 'Name', 'Phone', 'Salutation', 'Title',
                                         'Email']

        self.table2columns['Opportunity'] = ['AccountId', 'Amount', 'CloseDate',  'Description',
                                             'ExpectedRevenue', 'ForecastCategoryName', 'Id', 'IsClosed', 'IsDeleted',
                                             'IsWon', 'Name', 'NextStep', 'OwnerId', 'Owner.Name', 'Probability',
                                             'StageName',
                                             'TotalOpportunityQuantity', 'Type']

        self.table2columns['OpportunityContactRole'] = ['ContactId', 'Id', 'IsPrimary', 'OpportunityId', 'Role']

        # self.table2columns['Task'] = ['AccountId', 'ActivityDate', 'CallDisposition', 'CallDurationInSeconds',
        #                               'CallObject', 'CallType', 'CompletedDateTime', 'Description',
        #                               'Id', 'IsArchived', 'IsClosed', 'IsDeleted', 'IsHighPriority', 'IsRecurrence',
        #                               'IsReminderSet', 'OwnerId', 'Owner.Name', 'Priority',
        #                               'Status', 'Subject', 'TaskSubtype', 'WhatId', 'WhoId', 'What.Name', 'Who.Name']

        self.table2columns['User'] = ['CompanyName', 'ContactId', 'Email', 'EmailEncodingKey', 'FirstName', 'Id',
                                      'LastName', 'MobilePhone', 'Name', 'Phone', 'Title']

        self.foreign_keys = [
            ('Account.PrimaryContactId__c','Contact.Id'),  ('Account.ParentId','Account.Id'),

            ('Contact.AccountId','Account.Id'),

            ('Opportunity.AccountId','Account.Id'),  ('Opportunity.ContactId','Contact.Id'),
            ('Opportunity.OwnerId', 'User.Id'),

            ('OpportunityContactRole.ContactId','Contact.Id'),
            ('OpportunityContactRole.OpportunityId','Opportunity.Id'),

            # ('Task.AccountId','Account.Id'), ('Task.OwnerId','User.Id'),
            # ('Task.WhatId','Account.Id'),  ('Task.WhoId','Contact.Id'),

            ('User.ContactId','Contact.Id'),
        ]

        # Make sure GPT understands the nature of the relationships between SObjects

        self.tables_descrption = self.prompt_trim(f"""
            Notes about the schema:
            * The primary key for all tables is Id
            * The following column names are foreign keys to the following tables, in the format [Table.Column=Table.Column, ...]:
            [{', '.join([t1+'='+t2 for t1,t2 in self.foreign_keys])}]
            * Owner and Owner.Name are owners of a record, not a business
            * Only query one table at a time.
            * Do not perform subselect or subqueries in a SQL statement.
            * Do not use JOINs. Take it step by step to do mulitple SELECTs to get the data you need.
            * Do not use SELECTs in the where clause
        """)

        #
        #   End of Init
        #

    # Because of the way multi-line strings are formatted in
    # source code, they end up with lots of leading spaces.
    # So we remove them as they are not meaningful.
    @staticmethod
    def prompt_trim(prompt: str) -> str:
        lines = prompt.split('\n')
        trimmed = '\n'.join([l.strip() for l in lines])
        return trimmed

    #
    #   Establish (or reestablish) our authenticated session with Salesforce
    #
    #   NOTA BENE: This is not implementing a per-user connection, rather a
    #              general purpose shared use
    #
    def reconnect(self):
        p_username = os.getenv('SF_USERNAME')
        p_password = os.getenv('SF_PASSWORD')
        p_key = os.getenv('SF_KEY')
        p_secret = os.getenv('SF_SECRET')

        self.sf = Salesforce(username=p_username, password=p_password,
                             consumer_key=p_key, consumer_secret=p_secret)

        if self.user_id is None:
            rows = self.sf.query(
                f"SELECT Name, Phone, Username, CompanyName, Email, Id, Title FROM User WHERE Username = '{p_username}'")
            self.user_name = rows['records'][0]['Name']
            self.user_phone = rows['records'][0]['Phone']
            self.user_username = rows['records'][0]['Username']
            self.user_companyname = rows['records'][0]['CompanyName']
            self.user_email = rows['records'][0]['Email']
            self.user_id = rows['records'][0]['Id']
            self.user_title = rows['records'][0]['Title']

    #
    # This generates the text that will be added into the system prompt to explain the database schema
    #
    def get_schema(self):

        # database_schema_string = "The schema of the database is as follows:\n\n"
        database_schema_string = ""
        for t in self.table2columns:
            database_schema_string += f'Table: {t}\n'
            database_schema_string += 'Columns: ' + ', '.join(self.table2columns[t]) + '\n'

        database_schema_string += self.tables_descrption + "\n\n"

        return database_schema_string

    #   ##########################################################################
    #   This generates the functions parameter which is inserted into every call to OpenAI
    #   ##########################################################################

    def get_functions_parameter(self):
        f = [
            {
                "name": "ask_database",
                "description": """Use this function to answer user questions about accounts, contacts, and opportunities. 
                                Input should be a fully formed SQL query.""",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": self.prompt_trim(f"""
                            SQL SELECT extracting info to answer the user's question.
                            SQL should be written using this database schema:
                            {self.get_schema()}
                            The query should be returned in plain text, not in JSON.
                                    """),
                        }
                    },
                    "required": ["query"],
                },
            },
        ]

        return f

    #   ##########################################################################
    #   Perform a query
    #   ##########################################################################

    def do_query(self, query: str):

        Plog.info('SQL: ' + query)
        # Translate from SQL to SOQL
        query, table = sql2soql(query, self.table2columns, hard_limit=self.row_limit)
        Plog.info('SOQL: ' + query)

        retries = 3
        recs = None

        if self.sf is None:
            self.reconnect()

        while retries > 0:
            try:
                recs = self.sf.query(query)
                break
            except (SalesforceExpiredSession,
                    ConnectionResetError) as err:  # Whoopsie, our connection timed out, restart and go again
                Plog.error('SF Query Error: ' + str(err))
                self.reconnect()
                retries -= 1

        # Turn the records into an array of dicts
        result = []
        if recs is not None:
            for r in recs['records']:
                row = {}
                for k in r:
                    if k != 'attributes':
                        v = r[k]
                        if type(v).__name__ == 'OrderedDict':
                            for k2 in v:
                                if k2 != 'attributes':
                                    row[f'{k}.{k2}'] = v[k2]
                        else:
                            row[k] = r[k]
                result.append(row)
        else:
            Plog.error(f'do_query received None on query({query})')

        Plog.info('RESULT: ' + str(result))
        Plog.info(f'SF Query: A total of {len(result)} rows returned.')
        # context = { 'table': table, 'results': result}
        # return context
        return result

    def ask_database(self, query):
        Plog.debug('SF Query: ' + query)
        try:
            results = str(self.do_query(query))
        except Exception as e:
            results = f"query failed with error: {e}"
            Plog.error(results)
        return results

    def call_function(self, name, data):
        if name == 'ask_database':
            return self.ask_database(data)

        return f'Function {name} not supported.'

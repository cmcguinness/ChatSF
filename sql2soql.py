"""
    sql2soql

    Salesforce's SOQL is based upon SQL, but has just enough differences
    to break things all the damn time.

    This is a function that takes a SQL Select statement and translates it
    into SOQL.  It cannot translate every SQL statement, but it can handle
    a meaningful subset of them.  There is a lot more that needs to be done
    to make it truly resilient, but since this is a technology demonstration
    it is good enough for that purpose.

    The design of this is driven mostly by observing what breaks and then
    putting in a heuristic to fix it. It is not a general purpose SQL to
    SOQL translator, but it is a good start.

"""

#   If you don't like regular expressions, you're in for a, um, "treat"
import re


#
#   sql2soql
#
#   The one and only function that does it all.
#
#   Arguments:
#
#   sql:                (required) The SQL Select statement to translate
#   table2columns:      (optional) A Dictionary where we can get a list of columns for a given
#                       table (SObject), used to expand SELECT *.  If a table is not
#                       in the dictionary (or it is not present), we use FIELDS(ALL).
#   add_id:             (optional) Ensure that Id is always in the list of columns (Except for aggregates)
#   hard_limit:         (optional) If a LIMIT is not set, set one
#
#   Basic execution path:
#
#       1. Break apart the SQL SELECT Statement
#       2. Fix up the parts of it that need to be rewritten from SQL to SOQL
#       3. Reassemble the SOQL SELECT statement
#
def sql2soql(sql: str, table2columns: dict = {}, add_id=True, hard_limit: int = 0) -> str:
    #   #######################################################
    #   Part 1: Parse the SQL statement
    #   #######################################################

    # SELECT clause

    # The SELECT itself
    selector = re.compile(r"select\s+", re.IGNORECASE)
    match = selector.search(sql)
    # Don't need to save the select part
    sql = sql[match.end(0):]

    # The fieldlist
    selector = re.compile(r"(.*?)from\s+", re.IGNORECASE)
    match = selector.search(sql)
    fields = sql[:match.end(1)].strip()
    sql = sql[match.end(1):]

    # Generate a list of fields (Not great for SELECT A b, C d FROM ... but it works)
    match = re.findall('(?i)\\s*(\*|[\\w\\d_\.]+(\\(.*?\\)){0,1})\\s*', fields)
    list_fields = [m[0] for m in match]

    #   Determine if this is an aggregate query
    selector = re.compile(r"(count\(|avg\(|count_distinct\(|min\(|max\(|sum\()", re.IGNORECASE)
    match = selector.search(fields)
    aggregate = match is not None

    #   FROM
    selector = re.compile(r"(FROM\s+)(\w+)\s*", re.IGNORECASE)
    match = selector.search(sql)
    table = match.group(2)
    sql = sql[match.end(0):]

    #   where
    where = None
    selector = re.compile(r"WHERE\s+", re.IGNORECASE)
    match = selector.search(sql)
    if match is not None:
        sql = sql[match.end(0):]
        selector = re.compile(r"(.*?)(GROUP|ORDER|LIMIT|$)", re.IGNORECASE)
        match = selector.search(sql)
        where = sql[:match.end(1)].strip()
        sql = sql[match.end(1):]

    #   GROUP BY
    groupby = None
    selector = re.compile(r"GROUP\s+BY\s+", re.IGNORECASE)
    match = selector.search(sql)
    if match is not None:
        sql = sql[match.end(0):]
        selector = re.compile(r"(.*?)(ORDER|LIMIT|$)", re.IGNORECASE)
        match = selector.search(sql)
        groupby = sql[:match.end(1)].strip()
        sql = sql[match.end(1):]

    #   ORDER BY
    orderby = None
    selector = re.compile(r"ORDER\s+BY\s+", re.IGNORECASE)
    match = selector.search(sql)
    if match is not None:
        sql = sql[match.end(0):]
        selector = re.compile(r"(.*?)(LIMIT|$)", re.IGNORECASE)
        match = selector.search(sql)
        orderby = sql[:match.end(1)].strip()
        sql = sql[match.end(1):]

    #   LIMIT
    limit = None
    selector = re.compile(r"(LIMIT\s+)(\d+)\s*", re.IGNORECASE)
    match = selector.search(sql)
    if match is not None:
        limit = match.group(2)
    elif not aggregate and hard_limit > 0:
        limit = str(hard_limit)

    #   #######################################################
    #   Part 2: Apply heuristics to fix up the SQL
    #   #######################################################

    #   SELECT * is not supported in SQL
    if fields == '*':
        if table2columns is not None and table in table2columns:
            fields = ', '.join(table2columns[table])
            list_fields = table2columns[table]
        else:
            fields = 'FIELDS(ALL)'
            list_fields.append('Id')  # So we won't try to add Id and get an error

    #   SELECT count(*) -> Select count()
    if fields.lower() == 'count(*)':
        fields = 'count()'

    #   Make sure 'Id' is in the list of fields,
    #   If not an aggregate and we were told to do it
    if 'Id' not in list_fields and not aggregate and add_id:
        fields += ", Id"
        list_fields.append('Id')

    # Add in any field that ends in Id (Yes, Id does too, but you can comment this code out and get just Id)
    if (not aggregate) and add_id and table in table2columns:
        for f in table2columns[table]:
            if f.endswith('Id') or f.endswith('Id__c'):
                if f not in list_fields:
                    fields += f', {f}'
                    list_fields.append(f)

    # Remove any columns we don't know about (hallucinated columns)
    if table in table2columns:
        cols = table2columns[table]
        for c in list_fields:
            if '(' not in c:
                if c not in cols:
                    list_fields.remove(c)

        # Regenerate the fields
        fields = ', '.join(list_fields)

    # Dealing with Dates has two problems:
    if where is not None:

        # 1. Use of Date psuedo-constants that don't exist in SOQL (e.g., THIS_MONTH_END)
        where = where.replace('TODAY()', 'TODAY')
        where = re.sub(r"<=\s*THIS_MONTH_END", "< NEXT_MONTH", where)
        where = re.sub(r">=\s*THIS_MONTH_START", "> LAST_MONTH", where)
        where = re.sub(r"<=\s*LAST_DAY\(TODAY(\(\))?\)", "< NEXT_MONTH", where)
        where = re.sub(r">=\s*FIRST_DAY\(TODAY(\(\))?\)", "> LAST_MONTH", where)

        #   2. Date Literals: in SQL they are 'YYYY-MM-DD', but SOQL drops the '
        pat_date1 = r"'\d\d\d\d-\d{1,2}-\d{1,2}'"
        while m := re.search(pat_date1, where):
            first = m.start(0)
            last = m.end(0)
            where = where[:last - 1] + where[last:]
            where = where[:first] + where[first + 1:]


    #   #######################################################
    #   Part 3: Rebuild the SELECT statement as SOQL
    #   #######################################################

    result = 'SELECT ' + fields + ' '
    result += 'FROM ' + table + ' '
    if where is not None:
        result += 'WHERE ' + where + ' '
    if groupby is not None:
        result += 'GROUP BY ' + groupby + ' '
    if orderby is not None:
        result += 'ORDER BY ' + orderby + ' '
    if limit is not None:
        result += 'LIMIT ' + limit

    return result, table


#   #######################################################
#   Test Code: run this file directly to execute
#   #######################################################
if __name__ == '__main__':

    #   Two tables we use in the test (except for Task, which is purposely
    #   not included
    tables = {
        'Account': ['Id', 'a1', 'a2', 'a3', 'OwnerId'],
        'Contact': ['Id', 'c1', 'c2', 'c3', 'OwnerId', 'AccountId'],
        'Opportunity': ['AccountId', 'Amount', 'CloseDate', 'ContactId', 'Description',
                        'ExpectedRevenue', 'ForecastCategoryName', 'Id', 'IsClosed', 'IsDeleted',
                        'IsWon', 'Name', 'NextStep', 'OwnerId', 'Owner.Name', 'Probability', 'StageName',
                        'TotalOpportunityQuantity', 'Type']
    }

    tests_with_tables = [
        "SELECT a1, a2 FROM Account WHERE Name='Fred' GROUP BY lastname ORDER BY firstname",
        "SELECT Max(a1) cmax FROM Account WHERE Name='Fred' Group BY firstname order by cmax DESC",
        "SELECT * FROM Contact WHERE FirstName='Fred' and LastName='Smith' ORDER BY email DESC limit 20",
        "SELECT a1, a2, a3 FROM Account WHERE ModDate > '2023-01-01' GROUP BY industry ORDER BY revenue",
        "SELECT a1, a2 FROM Account WHERE ModDate > '2023-1-01' GROUP BY industry ORDER BY revenue",
        "SELECT * from Task",
        "SELECT a1 FROM Account WHERE Name = 'Edge Communications'",
        "SELECT Id, c1 FROM Contact WHERE Name = 'Fred Fickle'",
        "SELECT * FROM Opportunity WHERE Id = '006Hu00001WBrTrIAL"
    ]

    tests_without_tables = [
        "SELECT a1, a2 FROM Account WHERE ModDate > '2023-1-01' GROUP BY industry ORDER BY revenue",
        "SELECT * from Task",
    ]

    print('With all options:\n')

    for test in tests_with_tables:
        print("In : ", test)
        o = sql2soql(test, tables, add_id=True, hard_limit=4)
        print("Out: ", o)
        print()

    print('\nWith no options:\n')

    for test in tests_without_tables:
        print("In : ", test)
        o = sql2soql(test, add_id=False)
        print("Out: ", o)
        print('', flush=True)

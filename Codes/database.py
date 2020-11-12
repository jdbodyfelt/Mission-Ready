import sqlite3
from sqlite3 import Error
from movies import *


class movieDatabase:

    # /*******************************************/
    def __init__(
        self, pklName: str = config["saveFile"], dbName: str = config["dbName"]
    ):
        """Constructor
        :param pklName: pandas dataframe pickle file
        :param dbName: SQLite3 database file or login
        """
        try:
            self.df = pd.read_pickle(pklName)
            self._final_data_prep()
            self.conn = sqlite3.connect(dbName)
        except Error as e:
            print(e)

    # /*******************************************/
    def _final_data_prep(self):
        """
        1. Throw out unreleased movies. 
        2. SQLite does NOT support array cells. Explode to fix.
        3. Fix some " and ' appearances in strings. 
        """
        self.df.dropna(subset=['release_date'], inplace=True)
        self.df = self.df.explode("genres")
        self.df = self.df.explode("production_companies")
        self.df.reset_index(inplace=True)
        newCols = {"production_companies": "company", "genres": "genre", "index": "movie_id"}
        self.df.rename(columns=newCols, inplace=True)
        for col in ["company", "title", "director"]:
            self.df[col] = self.df[col].apply(
                lambda x: x.replace('"', "").replace("'", "")
            )

        self.df['release_year'] = self.df['release_date'].apply(lambda x: int(x.year))
        self.df['release_month'] = self.df['release_date'].apply(lambda x: int(x.month))
        self.df.drop(columns='release_date', inplace=True)
        
    # /*******************************************/
    def _create_table(self, ddl: str = None):
        """DDL Table Creation
        :param ddl: CREATE TABLE DDL
        """
        if not ddl:
            raise Error("Missing DDL!")
        create = "CREATE TABLE IF NOT EXISTS"
        if create not in ddl.upper():
            raise Error(f"DDL doesn't contain '{create}'!")
        try:
            cursor = self.conn.cursor()
            cursor.execute(ddl)
            # Note: DDLs have implicit commits!
        except Error as e:
            print(e)

    # /*******************************************/
    def _clear_table(self, table: str = None):
        """SQL Table Truncation
        :param table: table to drop all rows
        """
        if not table:
            raise Error("Missing table name!")
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"DELETE FROM {table}")
            self.conn.commit()
        except Error as e:
            print(e)

    # /*******************************************/
    def createTables(self):
        """ Driver routine to make ALL tables in SQL """
        DDLs = {
            "director": "id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT",
            "genre": "id INTEGER PRIMARY KEY, name TEXT",
            "company": "id INTEGER PRIMARY KEY, name TEXT",
            "movie": """
            id INTEGER PRIMARY KEY, 
            movie_id INTEGER, 
            title TEXT, 
            release_year INTEGER, 
            release_month INTEGER, 
            budget INTEGER, 
            revenue INTEGER, 
            popularity REAL, 
            runtime INTEGER,
            director INTEGER, 
            genre INTEGER, 
            company INTEGER,
            FOREIGN KEY(director) REFERENCES director(id),
            FOREIGN KEY(genre) REFERENCES genre(id),
            FOREIGN KEY(company) REFERENCES company(id)
            """,
        }
        for table, declStr in DDLs.items():
            ddl = f"CREATE TABLE IF NOT EXISTS {table} ({declStr});"
            self._create_table(ddl)

    # /*******************************************/
    def _get_sql_insert(self, table: str = None, record={}):
        """
        :param table: table name to insert record
        :param record: record dictionary
        :return sql: SQL string
        """
        sql = f"INSERT INTO {table}("
        for key in record.keys():
            sql += f"{key}, "
        sql = f"{sql[:-2]}) VALUES ("
        for val in record.values():
            if isinstance(val, str):
                sql += f"'{val}', "
            else:
                sql += f"{val}, "
        sql = f"{sql[:-2]});"
        return sql

    # /*******************************************/
    def _split_names(self):
        """
        This internal function cleans name string into first/last.
        """
        df = pd.DataFrame(columns=["full", "first", "last"])
        self.df["director"] = self.df["director"].apply(
            lambda x: x.replace(" Jr.", "").replace(" II", "").replace(" III", "")
        )
        df["full"] = self.df["director"].unique()
        for k, name in enumerate(df["full"]):
            names = name.split(" ")
            if len(names) == 1:
                df.iloc[k]["first"] = ""
                df.iloc[k]["last"] = names[0]
            elif len(names) == 2:
                df.iloc[k]["first"] = names[0]
                df.iloc[k]["last"] = names[1]
            elif len(names) == 3:
                if names[-1] == "":
                    df.iloc[k]["first"] = names[0]
                    df.iloc[k]["last"] = names[1]
                elif names[1][-1] == ".":
                    df.iloc[k]["first"] = names[0]
                    df.iloc[k]["last"] = names[-1]
                elif names[1].lower() in ["del", "de", "von", "van", "el"]:
                    df.iloc[k]["first"] = names[0]
                    df.iloc[k]["last"] = " ".join(names[1:])
                else:
                    df.iloc[k]["first"] = names[0]
                    df.iloc[k]["last"] = names[-1]
            else:
                df.iloc[k]["first"] = names[0]
                df.iloc[k]["last"] = " ".join(names[1:])
        df.sort_values(["last", "first"], inplace=True)
        return df

    # /*******************************************/
    def populateTables(self):
        """
        This external call injects all data from pandas dataframe
        into the SQL database. 
        """
        cursor = self.conn.cursor()
        for tab in ["genre", "company", "director"]:
            print(f"Filling {tab.upper()} table...")
            try:
                self._clear_table(tab)
                uniq = (
                    sorted(self.df[tab].unique())
                    if tab != "director"
                    else self._split_names()
                )
                for uid, name in enumerate(uniq if tab != 'director' else uniq['full']):
                    rec = {"id": uid, "name": name} 
                    if(tab == "director"):
                        del rec["name"]
                        rec.update({'first_name': uniq.iloc[uid]['first'],'last_name': uniq.iloc[uid]['last']})
                    sql = self._get_sql_insert(tab, rec)
                    cursor.execute(sql)
                self.conn.commit()
                uniq = uniq if tab != 'director' else uniq['full'].tolist()
                self.df[tab] = self.df[tab].apply(lambda x: int(uniq.index(x)))
            except Error as e:
                print(e)
        # Finally, we'll inject the movie table!
        print(f"Filling MOVIE table...")
        try:
            self._clear_table('movie')
            for uid, row in self.df.iterrows():
                sql = self._get_sql_insert('movie', dict(row))
                cursor.execute(sql)
            self.conn.commit()
        except Error as e:
                print(e)
        finally:
            print('Finished! Dataframe loaded into SQLite database.')

#####################################################

if __name__ == "__main__":
    db = movieDatabase()
    db.createTables()
    db.populateTables()
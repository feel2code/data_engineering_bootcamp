from logging import Logger
 
from lib.pg import PgConnect


class DdsRepository:
    def __init__(self, db: PgConnect, logger: Logger) -> None:
        self._db = db
        self._logger = logger

    def _execute_query(self, query, params=None):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
        return cur

    def _get_columns(self, table: str, except_col: str = None) -> list:
        additional_exception = f' EXCEPT {except_col} ' if except_col else ''
        query = f"SELECT * {additional_exception} FROM dds.{table} LIMIT 0;"
        with self._execute_query(query) as cur:
            return [desc[0] for desc in cur.description] 

    def _insert_records(self, table: str, colnames: list, pk_cols: str, values_args) -> str:
        values_set = ", ".join(['%(' + colname + ')s' for colname in colnames])
        colnames_set = ", ".join([colname + '=EXCLUDED.' + colname for colname in colnames[1:]])
        params={col: val for col, val in zip(colnames, values_args)}
        query = f"""
                    INSERT INTO {table}({", ".join(colnames)}) 
                    VALUES ({values_set})
                    ON CONFLICT ({pk_cols}) DO UPDATE
                    SET {colnames_set};
                """
        self._execute_query(query, params)
        return query

    def hubs_insert(self, hub: str, *args: list) -> None:
        table, pk_cols = f'h_{hub}', f'h_{hub}_pk'
        values_args=list(args)
        colnames = self._get_columns(table)
        query = self._insert_records(table, colnames, pk_cols, values_args) 
        self._logger.info(f"query: {query}")

    def links_insert(self, link: str, *args: list) -> None:
        table, pk_cols = f'l_{link}', f'hk_{link}_pk'
        values_args=list(args)
        colnames = self._get_columns(table)
        query = self._insert_records(table, colnames, pk_cols, values_args) 
        self._logger.info(f"query: {query}")


    def sattelite_insert(self, sattelite: str, *args: list) -> None:
        except_col = 'load_dt'
        table, pk_cols = f's_{sattelite}', f'h_{(sattelite.split("_"))[0]}_pk, {except_col}'
        values_args=list(args)
        colnames = self._get_columns(table, except_col)
        query = self._insert_records(table, colnames, pk_cols, values_args) 
        self._logger.info(f"query: {query}")

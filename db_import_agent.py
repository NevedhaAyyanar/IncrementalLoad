import MySQLdb
import MySQLdb.cursors
import csv
import datetime
import dp_utils
import glob
import logging.config
import os
import pandas as pd
import pathlib
import signal
import subprocess
import sys
import time
import traceback
from TCLIService.ttypes import TOperationState
from contextlib import closing
from optparse import OptionParser
from pyhive import hive
from csv import writer
from datetime import timedelta
fileExist = False
Job_status = ''
err_desc = ''

hive_user = ''
hive_passwd = ''
hive_host = ''
hive_port = ''
##########################################Added for incremental logic#######################
log_file_path = '/opt/data-lake-bigb-incremental/DEV-pipeline-incremental-load/config/incremental_logs.csv' #path in putty (EC2 Instance)
config_file_name = '/opt/data-lake-bigb-incremental/DEV-pipeline-incremental-load/config/incremental_config.csv'
df_incremental_log = pd.read_csv(log_file_path)
df_incremental_config = pd.read_csv(config_file_name)


##########################################Added for incremental logic#######################

class db_import_agent:

        def __init__(self, options, hive_db_name, hive_table, logger):
                self.logger = logger
                self.email_client = dp_utils.email_client()
                self.batch_size = options.batch_size
                self.db_type = options.db_type
                self.db_host = options.db_host
                self.db_port = options.db_port
                self.db_user = options.db_user
                self.db_passwd = options.db_passwd
                self.db_name = options.db_name
                self.db_table = options.db_table
                self.db_where = options.db_where
                self.db_columns = options.db_columns
                self.db_extract_query = options.db_extract_query
                self.hive_db_name = hive_db_name
                self.hive_table = hive_table
                self.part_spec = options.part_spec
		if 'bigb' in self.db_host:
                	self.tmp_dir = "/opt/data-lake-bigb-incremental/DEV-pipeline-incremental-load/sqoop/" + time.strftime("%Y%m%d") + "/bb/" + self.db_name + "/" + self.db_table + "/"
		elif 'easyday' in self.db_host:
			self.tmp_dir = "/opt/data-lake-bigb-incremental/DEV-pipeline-incremental-load/sqoop/" + time.strftime("%Y%m%d") + "/ed/" + self.db_name + "/" + self.db_table + "/"
                self.s3Path = options.s3Path
                if 'bigb' in self.db_host:
                        org = ''
                elif 'easyday' in self.db_host:
                        org = '/ED'
                tmp = self.s3Path.rstrip("/") + "/database=" + self.db_name + "/table=" + self.db_table
                tmp = self.s3Path.rstrip("/") + org + "/database=" + self.db_name + "/table=" + self.db_table
                self.logger.info(tmp)
                if (options.part_spec is not None):
                        self.s3CurrentPath = tmp.rstrip("/") + "/" + options.part_spec.rstrip("/") + "/"
                        self.logger.info(self.s3CurrentPath)
                else:
                        self.s3CurrentPath = tmp.rstrip("/") + "/"
                        self.logger.info(self.s3CurrentPath)
                self.logger.info("Temp Directory is: {0}".format(str(self.tmp_dir)))
                try:
                        pathlib.Path(self.tmp_dir).mkdir(parents=True)
                except OSError:
                        self.logger.info("Creation of the directory {0} failed".format(str(self.tmp_dir)))
                else:
                        self.logger.info("Successfully created the directory: {0} ".format(str(self.tmp_dir)))
                self.tmp_path = self.tmp_dir + self.hive_table + ".csv"
                self.logger.info("Temp File is: {0}".format(str(self.tmp_path)))
                #sys.exit()
        def execute(self, cmd):
                self.logger.info("Command : '{0}'".format(str(cmd)))
                process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stdin=subprocess.PIPE, shell=True)
                out, err = process.communicate()
                self.logger.info("Output of above Command: {0}".format(out.decode("utf-8")))

        def getConnection(self, dbType):
                if dbType == 'hive':
                        return hive.Connection(host=hive_host, port=hive_port, username=hive_user, password=hive_passwd)
                elif dbType == 'mysql':
                        #print(self.db_host,self.db_user, self.db_passwd,self.db_name)
                        return MySQLdb.connect(host=self.db_host, user=self.db_user, passwd=self.db_passwd, db=self.db_name, charset='utf8')

        def toHiveType(self, argument):
                if argument.startswith("int") or argument.startswith("smallint") or argument.startswith("mediumint"):
                        return "int"
                elif argument.startswith("tinyint"):
                        return "tinyint"
                elif argument.startswith("bigint"):
                        return "bigint"
                elif argument.startswith("float") or argument.startswith("decimal") or argument.startswith("double"):
                        return "double"
                elif argument.startswith("bit"):
                        return "boolean"
                else:
                        return "string"

        def executeMysqlCommandFetchAll(self, query):
                self.logger.info("Mysql Query: {0}".format(query))
                with closing(self.getConnection("mysql")) as con:
                        with closing(con.cursor()) as cur:
                                cur.execute(query)
                                return cur.fetchall()

        def executeMysqlCommandFetchAllPD(self, query):
                self.logger.info("Mysql Query: {0}".format(query))
                with closing(self.getConnection("mysql")) as con:
                        self.logger.info("mouli running fetchall function")
                        self.logger.info(query)
                        df = pd.read_sql(query, con)
                        self.logger.info("mouli completed fetch all 1/3")
                        df = df.replace(r'\r', '', regex=True)
                        self.logger.info("mouli completed fetch all 2/3")
                        df = df.replace(r'\n', '', regex=True)
                        self.logger.info("mouli completed fetch all 3/3")
                        return df

        def executeMysqlCommandFetchOne(self, query):
                self.logger.info("Mysql Query: {0}".format(query))
                with closing(self.getConnection("mysql")) as con:
                        self.logger.info("mouli running fetchone function")
                        with closing(con.cursor()) as cur:
                                self.logger.info("mouli closing conn fetch one")
                                cur.execute(query)
                                self.logger.info("mouli completed fetch one")
                                return cur.fetchone()[0]

##########################################Added for incremental logic#######################

    #write the incremental log to csv file
        def incremental_log(self,latest_max_value,now_time):
                self.logger.info("mouli now inside incremental log")
                append_to_incremental_log_list=[self.db_name,self.db_table,str(latest_max_value),now_time,self.db_host]
                self.logger.info(append_to_incremental_log_list)
                self.logger.info("outside open with csv writer")
                with open("/opt/data-lake-bigb-incremental/DEV-pipeline-incremental-load/config/incremental_logs.csv",'a') as incremental_logs:
                    self.logger.info("inside open with csv writer")
                    writer_object = writer(incremental_logs)
                    writer_object.writerow(append_to_incremental_log_list)
                    incremental_logs.close()                   
                #self.logger.info("mouli now inside incremental log")
                #append_to_incremental_log_list=[self.db_name,self.db_table,str(latest_max_value),now_time,self.db_host]
                #self.logger.info(str(now_time))
                #self.logger.info("mouli loaded append_to_incremental_log_list")
                #self.logger.info(log_file_path)
                #df_len = len(df_incremental_log)
                #self.logger.info(df_len)
                #self.logger.info(append_to_incremental_log_list)
                #df_incremental_log.drop(df_incremental_log.columns[[0]], axis = 1, inplace = True)
                #self.logger.info("mouli dropped 1 column from source")
                #self.logger.info("mouli before append")
                #self.logger.info(df_incremental_log)
		#self.logger.info(df_incremental_log.columns)
		#sys.exit()
                #df_incremental_log.loc[df_len] = append_to_incremental_log_list
                #self.logger.info(df_incremental_log)
                #self.logger.info("mouli dropping 1 column after appending")
                #df_incremental_log.drop(df_incremental_log.columns[[0]], axis = 1, inplace = True)
                #self.logger.info("mouli write to csv")
                #df_incremental_log.to_csv(log_file_path,index=False)

##########################################Added for incremental logic#######################

        def getMySQLSchema(self):
                column_Name = 0
                column_DataType = 1
                column_Default = 2
                mysqlColDataType = []
                index = 0
                with closing(self.getConnection("mysql")) as con:
                        with closing(con.cursor()) as cur:
                                select_stm = ("describe " + self.db_table)
                                cur.execute(select_stm)
                                rows = cur.fetchall()
                                for row in rows:
                                        mysqlValue = {}
                                        mysqlValue[column_Name] = row[0]
                                        mysqlValue[column_DataType] = row[1]
                                        mysqlValue[column_Default] = row[4]
                                        mysqlColDataType.insert(index, mysqlValue)
                                        index = index + 1
                return mysqlColDataType

        def executeHiveCommand(self, query):
                self.logger.info("Mysql Query: {0}".format(query))
                with closing(self.getConnection("hive")) as con:
                        with closing(con.cursor()) as HiveCommand:
                                HiveCommand.execute(query, async_=True)
                                status = HiveCommand.poll().operationState
                                while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
                                        logs = HiveCommand.fetch_logs()
                                        for message in logs:
                                                self.logger.info("{0}".format(message))
                                        status = HiveCommand.poll().operationState
                                return HiveCommand.fetchall()

        def getHiveSchema(self):
                hiveColDataType = []
                column_Name = 0
                column_DataType = 1
                index = 0

                query = 'use  ' + str(self.hive_db_name) + ';describe formatted ' + str(self.hive_table);
                rows = self.executeHiveCommand(query)
                firstLine = True
                for row in rows:
                        row = row.strip()
                        if firstLine or len(row) == 1:
                                firstLine = False
                                continue
                        if row.startswith('# '):
                                break
                        else:
                                hiveVal = {}
                                col = row.split('\t')
                                if len(col) > 1:
                                        hiveVal[column_Name] = col[0].strip()
                                        hiveVal[column_DataType] = col[1].strip()
                                        hiveColDataType.insert(index, hiveVal)
                                        index = index + 1
                return hiveColDataType

        def updateColumnsHive(self, hiveDBName, hiveTableName):
                global Job_status
                global err_desc
                hiveSchema = self.getHiveSchema()
                mysqlSchema = self.getMySQLSchema()
                mysqlLen = len(mysqlSchema)
                hiveLen = len(hiveSchema)
                self.logger.info("Source Schema : {0}".format(str(mysqlSchema)))
                self.logger.info("Hive Schema : {0}".format(str(hiveSchema)))
                if hiveLen == 0:
                        self.logger.info("Table Does Not Exist")
                        return
                elif mysqlLen == hiveLen:
                        self.logger.info("No changes in schema from source to destination")
                        return
                elif mysqlLen - 1 < hiveLen:
                        msgs = "Hive schema has more column than mysql,Hive Schema" + str(mysqlSchema) + " MySql Schema :" + str(hiveSchema)
                        self.logger.info(msgs)
                        err_desc = err_desc + msgs
                        return
                else:
                        maxCount = mysqlLen

                # Matching the schema
                idx = 0
                index = -1
                addedColumns = []
                while idx < maxCount:
                        index = index + 1
                        data = {}
                        if idx >= hiveLen:
                                data = mysqlSchema[idx]
                                addedColumns.insert(index, data)
                                idx = idx + 1
                        elif hiveSchema[idx].get(0) == mysqlSchema[idx].get(0):
                                idx = idx + 1
                        else:
                                idx = idx + 1
                                err_desc = err_desc + "Source to Destination schema mismatch in between Hive Schema :" + str(hiveSchema) + " Mysql Schema :" + str(mysqlSchema)
                                self.logger.info(err_desc)
                                return

                columnDT = ''
                for data in addedColumns:
                        columnDT = columnDT + (str(data.get(0)) + ' ' + self.toHiveType(data.get(1))) + ' , '

                columnDT = columnDT.rstrip(' , ')
                if len(columnDT) > 1:
                        query = 'use  ' + hiveDBName + '; ALTER TABLE ' + hiveTableName + ' ADD COLUMNS (' + columnDT + ')'
                        self.logger.info("Alter Statement :" + query)
                        retVal = self.executeHiveCommand(query)
                        if retVal != 0:
                                self.logger.error("Alter statement Failed to execute :" + query)
                        else:
                                if not (data.get(2) == None):
                                        Job_status = 'SUSPECT'

        def cleanS3(self):
                cmd = ("s3cmd --config=/opt/data-lake-bigb/.s3cfg del --recursive " + self.s3CurrentPath)
                self.execute(cmd)

        def cleanLocal(self):
                files = glob.glob(self.tmp_dir + "*")
                for f in files:
                        self.logger.info("{0}".format(f))
                        os.remove(f)

        def wherequery(self):
                self.logger.info('running wherequery #################################')
                #get the log for the current table
                df_get_table_log = df_incremental_log.where((df_incremental_log['DB_TABLE'] == self.db_table) & (df_incremental_log['HOST'] == self.db_host))

                #get the max updated time from log
                where_max_date = df_get_table_log.MAX_DATE.max()# + timedelta(seconds = -5)

                #update the where max date to get the rolling 90 days data
                today_end_date = datetime.datetime.now() + datetime.timedelta(days=-90)
                #where_max_date = today_end_date.replace(hour=0, minute=0, second=0, microsecond=0)

                #get the incremental column name from config
                inc_column_array = df_incremental_config[(df_incremental_config['DB_TABLE'] == self.db_table) & (df_incremental_config['HOST']==self.db_host)]
                if len(inc_column_array) != 0:
                    inc_column = inc_column_array['INCREMENTAL_COLUMN_NAME'].iloc[0]
                    where_clause = "where "+ inc_column + " >= '" + str(where_max_date) + "'"
                else:
                    inc_column = ''
                    where_clause = ''
                self.logger.info('mouli completed wherequery #################################')
                self.logger.info(where_clause)
                return where_clause,inc_column

        def preparePullQuery(self):
                dbTableStr = xstr(self.db_name) + "." + xstr(self.db_table)
                batch_count = 0
                self.logger.info('mouli preparequerystarted mouli printed this statement #################################')
                where_clause,inc_column = self.wherequery()
                self.logger.info('mouli length of where clause and inc_column below')
                self.logger.info(len(inc_column))
                try:

##########################################Added for incremental logic#######################
                        if len(where_clause) != 0:
                                self.logger.info('mouli running if clause #################################')
                                try:
                                        recordCount = self.executeMysqlCommandFetchOne("SELECT count(1) FROM " + dbTableStr + " " + where_clause)
                                        print(recordCount)
                                except:
                                        print('Error is: ', sys.exc_info())
                                recordCount = self.executeMysqlCommandFetchOne("SELECT count(1) FROM " + dbTableStr + " " + where_clause)
                                self.logger.info("Number of Record for ({0}) : {1}".format(dbTableStr, recordCount))
                                batch_size = int(self.batch_size)
                                for offset in range(0, recordCount, batch_size):
                                        self.logger.info('mouli starting for offset #################################')
                                        csv_file = str(self.tmp_dir) + self.hive_table + "_" + str(batch_count) + ".csv"
                                        query = "SELECT * FROM " + dbTableStr+" " + xstr(where_clause) + " LIMIT " + str(batch_size) + " OFFSET " + str(offset)
                                        self.logger.info("Part Query: %s ", query)
                                        df = self.executeMysqlCommandFetchAllPD(query)
                                        self.logger.info(inc_column)
                                        self.logger.info('mouli completed FetchAllPD #################################?')
                                        self.logger.info(type(inc_column))
                                        #get the max value to make an entry in incremental_logs.csv
                                        self.logger.info(inc_column)
                                        latest_max_value = df[inc_column].max()
                                        now_time = datetime.datetime.now()

                                        df.to_csv(csv_file, sep=chr(1), encoding='utf-8', quoting=csv.QUOTE_MINIMAL, quotechar='"', header=False, index=False, line_terminator='\n')
                                        batch_count = batch_count + 1

                                        self.logger.info('mouli starting inc log #################################')
                                        #call incremental_log fun to make an entry in log file
                                        self.incremental_log(latest_max_value,now_time)
                                        self.logger.info('mouli completed inc log #################################')


                                self.cleanS3()
                                self.execute("s3cmd --config=/opt/data-lake-bigb/.s3cfg put " + str(self.tmp_dir) + "* " + self.s3CurrentPath)
                                self.cleanLocal()
                                return 0
                        else:
##########################################Added for incremental logic#######################
                                self.logger.info('mouli skipped if clause running else part  #################################')
                                recordCount = self.executeMysqlCommandFetchOne("SELECT count(*) FROM " + dbTableStr + " " + xstr(self.db_where))
                                self.logger.info("Number of Record for ({0}) : {1}".format(dbTableStr, recordCount))
                                batch_size = int(self.batch_size)
                                for offset in range(0, recordCount, batch_size):
                                    csv_file = str(self.tmp_dir) + self.hive_table + "_" + str(batch_count) + ".csv"
                                    query = "SELECT * FROM " + dbTableStr + " " + xstr(self.db_where) + " LIMIT " + str(batch_size) + " OFFSET " + str(offset)
                                    self.logger.info("Part Query: %s ", query)
                                    df = self.executeMysqlCommandFetchAllPD(query)
                                    df.to_csv(csv_file, sep=chr(1), encoding='utf-8', quoting=csv.QUOTE_MINIMAL, quotechar='"', header=False, index=False, line_terminator='\n')
                                    batch_count = batch_count + 1
                                self.cleanS3()
                                self.execute("s3cmd --config=/opt/data-lake-bigb/.s3cfg put " + str(self.tmp_dir) + "* " + self.s3CurrentPath)
                                self.cleanLocal()
                                return 0
                except Exception as e:
                        subject = "[FAILURE] :: DBImport DB:{0} TABLE:{1}".format(self.db_name, self.db_table)
                        msg = traceback.format_exc()
                        sys.exc_info()
                        self.email_client.send_mail(subject, msg)
                        raise Exception("Failed to Import table " + dbTableStr + "Exception" + str(e))


def xstr(s):
        return '' if s is None else str(s)


def main():
        global err_desc
        global hive_db_name
        global hive_table
        optparser = OptionParser()
        optparser.add_option("--db_type", dest="db_type", default=None, help="Type of database")
        optparser.add_option("--db_host", dest="db_host", default=None, help="Hostname of database")
        optparser.add_option("--db_port", dest="db_port", default=None, help="Port of database")
        optparser.add_option("--db_user", dest="db_user", default=None, help="Username of database")
        optparser.add_option("--db_passwd", dest="db_passwd", default=None, help="Password of database")
        optparser.add_option("--db_name", dest="db_name", default=None, help="Name of database")
        optparser.add_option("--db_table", dest="db_table", default=None, help="dbtable to use in simple mode")
        optparser.add_option("--db_where", dest="db_where", default=None, help="where clause to use in simple mode")
        optparser.add_option("--db_columns", dest="db_columns", default=None, help="columns to use in simple mode")
        optparser.add_option("--db_extract_query", dest="db_extract_query", default=None, help="extract query clause to use in advanced mode")
        optparser.add_option("--hive_table", dest="hive_table", default=None, help="hive_table to import data in")
        optparser.add_option("--part_spec", dest="part_spec", default=None, help="Partition to import data into")
        optparser.add_option("--batch_size", dest="batch_size", default=70000, help="Batch Size for bulk Load")
        optparser.add_option("--s3Path", dest="s3Path", default="s3://fg-cnd.dp.internal.workspace/home/pradipta.dalal@akira.co.in/incremental_test/")
        optparser.add_option("--log_path", dest="log_path", default="/opt/data-lake-bigb-incremental/DEV-pipeline-incremental-load/logs/")
        optparser.add_option("--log_config", dest="log_config", default="/opt/data-lake-bigb-incremental/DEV-pipeline-incremental-load/config/logging.json")
        (options, args) = optparser.parse_args()
        log_path = '/opt/data-lake-bigb-incremental/DEV-pipeline-incremental-load/logs/' + options.db_name.lower() + "_" + datetime.datetime.now().strftime("%Y-%m-%d") + ".log"
        log = dp_utils.logger_utils()
        log.configure(log_path)
        err_desc = err_desc + '' + str(options)
        logger.debug("options = %s", str(options))
        logger.debug("args = %s", str(args))
        val = str(options.hive_table).split('.')

        if len(val) == 1:
                hive_db_name = 'default'
                hive_table = val[0]
        else:
                hive_db_name = val[0]
                hive_table = val[1]

        import_agent = db_import_agent(options, hive_db_name, hive_table, logger)
        if options.part_spec is not None:
                import_agent.updateColumnsHive()

        logger.info('mouli triggered preparepullquery $$$$$$$$$$$$$$$$$$$$$$$$')
        ret_value = import_agent.preparePullQuery()

        def signal_handler(*args):
                logger.info("Waiting for JVM to terminate ...")

        signal.signal(signal.SIGINT, signal_handler)  # Or whatever signal
        return ret_value


if __name__ == '__main__':

        logger = logging.getLogger(__name__)
        err_desc = ''
        try:
                code = main()
                sys.exit(code)
        except Exception:
                traceback.print_exc(file=sys.stderr)
                logger.error(traceback.format_exc())
                raise RuntimeError(err_desc)
                sys.exit(1)


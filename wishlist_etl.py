""" ETL processes for santa's wishlist """
import os.path
import pymssql
import luigi
import luigi.notifications
from dpl_utils.luigi_utils import DplTask
from boto.s3.connection import S3Connection
from itertools import groupby


class FullRun(DplTask):
    """FullRun task covers a full run of the ETL"""
    date = luigi.Parameter()

    def requires(self):
        """FullRun depends on SendReportEmail and SqlServerInsert"""
        return [SendReportEmail(environment=self.environment, date=self.date),
                SqlServerInsert(environment=self.environment, date=self.date)]


class SendReportEmail(DplTask):
    """Sends an email summary of the flat file"""
    date = luigi.Parameter()

    def requires(self):
        """SendReportEmail requires S3BucketDownload"""
        return S3BucketDownload(environment=self.environment, date=self.date)

    def run(self):
        """SendReportEmail reads the flat file and reports stats"""
        # read the tab delimited flat file and extract the date and price columns from each row
        grid = [(r.split('\t')[0], r.split('\t')[3]) for r in open(self.loggingbasedir + '/flatfile_{0}'.format(self.date)).readlines()]
        results = []
        # group the rows by date. build a list of strings reporting the total and average price per date
        for date, group in groupby(grid, lambda x: x[0]):
            prices = [int(x[1]) for x in list(group)]
            results.append('{0}: total[{1}] avg[{2}]'.format(date, sum(prices), sum(prices)/len(prices)))
        # send an email
        email = '<br/>'.join(results) + '<hr/><img src="http://pre12.deviantart.net/f8c4/th/pre/i/2015/352/a/5/luigi_elf_by_purple_guy2-d9kjpil.png" />'
        luigi.notifications.send_email_smtp('elves@northpole.org', 'Santas wishlist report', email, ['peter.weissbrod@abilitynetwork.com'], None)


class SqlServerInsert(DplTask):
    """Inserts rows from the flat file into sql server. Except for Bob."""
    date = luigi.Parameter()
    dbserver = luigi.Parameter(significant=False)
    dbuser = luigi.Parameter(significant=False)
    dbpassword = luigi.Parameter(significant=False)
    dbdatabase = luigi.Parameter(significant=False)

    def requires(self):
        """SqlServerInsert requires S3BucketDownload"""
        return S3BucketDownload(environment=self.environment, date=self.date)

    def run(self):
        """Read the flat file and insert into the DB"""
        # read the lines from the flat file into a 2d array
        grid = [line.split('\t') for line in open(self.loggingbasedir + '/flatfile_{0}'.format(self.date)).readlines()]
        # build the values to insert into the DB. calculate the category column. filter out bob
        vals = [(x[0], x[1], x[2], x[3], 'LOW' if x[3] < '20' else 'MED' if x < '50' else 'HIGH') for x in grid if x[0] != 'bob']
        # bulk-load the values into the table
        with pymssql.connect(self.dbserver, self.dbuser, self.dbpassword, self.dbdatabase, autocommit=True).cursor() as cursor:
            cursor.executemany('insert into wishlist values (%s, %s, %s, %s, %s)', vals)

    def assert_success(self):
        """ensure rows exist for the specified date upon completion"""
        with pymssql.connect(self.dbserver, self.dbuser, self.dbpassword, self.dbdatabase).cursor() as cursor:
            cursor.execute('select count(*) from wishlist where [date] = %s', self.date)
            if cursor.fetchone()[0] == 0:
                raise AssertionError('no wishes loaded for the specified date!')


class S3BucketDownload(DplTask):
    """Downloads from S3 into a flat file"""
    date = luigi.Parameter()
    s3bucket = luigi.Parameter(significant=False)
    s3accesskey = luigi.Parameter(significant=False)
    s3secretkey = luigi.Parameter(significant=False)

    def run(self):
        """download from s3 and produce flat file"""
        # open the flat file for writing
        with open(self.loggingbasedir + '/flatfile_{0}'.format(self.date), 'w') as flatfile:
            conn = S3Connection(self.s3accesskey, self.s3secretkey)
            # open the s3 bucket and iterate through the list
            bucket = conn.get_bucket(self.s3bucket, validate=False)
            for item in bucket.list():
                filedate = item.name.split('_')[0]
                filename = item.name.split('_')[1]
                # if the date of the file is beyond our parameter then ignore the file
                if filedate > self.date:
                    continue
                # for each item in the bucket write to the flat file
                for line in item.get_contents_as_string().splitlines():
                    if not line.startswith('#'):
                        vals = line.split('\t')
                        flatfile.write('{0}\t{1}\t{2}\t{3}\n'.format(filedate, filename, vals[0], vals[1]))

    def assert_success(self):
        """ensure the flat file exists for the date after running"""
        if not os.path.isfile(self.loggingbasedir + '/flatfile_{0}'.format(self.date)):
            raise AssertionError('No flat file exists for the specified date')

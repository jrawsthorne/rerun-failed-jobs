# Query couchbase to get jobs that have result=FAILURE. Add them to the rerun queue
# Query couchbase to get jobs that have result=Aborted or unstable and failcount=totalcount. Add them to the rerun queue
# Query couchbase to get jobs that have result=Unstable. Fetch the failcount for these jobs from the last weekly build, and compare with the current ones. If there are
#    more failures this week, add them to the rerun queue


# Sort the re-run queue by components, and identify components and subcomponents
# Until all jobs are run do the following :
# Check if there are more than 10 available machines in the server pool
# Trigger jobs per component. Wait for (no. of jobs x 30s + 60s)
# Check if there are more than 10 available machines in the server pool
# Periodically check for any new jobs from the above criteria added to the queue

# Once a job in the queue is executed, create a record in a couchbase bucket to capture the job name, original results and url so that its failures can be analyzed later
import ast
import time
import sys
from datetime import datetime
from couchbase.n1ql import N1QLQuery
from couchbase.cluster import Cluster
from couchbase.bucket import Bucket
from couchbase.cluster import PasswordAuthenticator
from couchbase.exceptions import HTTPError
import requests
import json
import copy
from threading import Thread
import threading
from requests.models import Response
from typing import List, Any
import logging

MAX_RETRIES = 3
COOLDOWN = 3

MAX_RERUN = 3

GREENBOARD_DB_HOST = "172.23.98.63"
GREENBOARD_DB_USERNAME = "Administrator"
GREENBOARD_DB_PASSWORD = "password"
GREENBOARD_DB_BUCKETNAME = "server"
RERUN_JOBS_HISTORY_BUCKETNAME = "rerun_jobs"

SERVER_POOL_DB_HOST = "172.23.105.177"
SERVER_POOL_DB_USERNAME = "Administrator"
SERVER_POOL_DB_PASSWORD = "esabhcuoc"
SERVER_POOL_DB_BUCKETNAME = "QE-server-pool"
MAX_AVAILABLE_VMS_TO_RERUN = 10

current_build_num = sys.argv[1]
prev_stable_build_num = sys.argv[2]
run_infinite = sys.argv[3]
LOCKMODE_WAIT = 100
logger = logging.getLogger("rerun_failed_jobs")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)
timestamp = str(datetime.now().strftime('%Y%m%dT_%H%M%S'))
fh = logging.FileHandler("./rerun_failed_jobs-{0}.log".format(timestamp))
fh.setFormatter(formatter)
logger.addHandler(fh)

components = [{"name": "2i", "poolId": "regression", "addPoolId": "None"},
              {"name": "analytics", "poolId": "jre", "addPoolId": "None"},
              {"name": "backup_recovery", "poolId": "regression", "addPoolId": "None"},
             {"name": "cli", "poolId": "regression", "addPoolId": "None"},
             {"name": "cli_imex", "poolId": "regression", "addPoolId": "None"},
             {"name": "durability", "poolId": "regression", "addPoolId": "None"},
             {"name": "ephemeral", "poolId": "regression", "addPoolId": "None"},
             {"name": "eventing", "poolId": "regression", "addPoolId": "None"},
             {"name": "fts", "poolId": "regression", "addPoolId": "elastic-fts"},
            {"name": "ipv6", "poolId": "ipv6", "addPoolId": "None"},
             {"name": "logredaction", "poolId": "regression", "addPoolId": "None"},
             {"name": "nserv", "poolId": "regression", "addPoolId": "None"},
             {"name": "query", "poolId": "regression", "addPoolId": "None"},
             {"name": "rbac", "poolId": "regression", "addPoolId": "None"},
             {"name": "security", "poolId": "security", "addPoolId": "None"},
             {"name": "transaction", "poolId": "regression", "addPoolId": "None"},
             {"name": "tunable", "poolId": "regression", "addPoolId": "None"},
             {"name": "view", "poolId": "regression", "addPoolId": "None"},
             {"name": "xdcr", "poolId": "regression", "addPoolId": "elastic-xdcr"},
              {"name": "epeng", "poolId": "regression", "addPoolId": "elastic-xdcr"},
            {"name": "cli_tools", "poolId": "regression", "addPoolId": "None"},
             {"name": "durability", "poolId": "regression", "addPoolId": "None"},
             {"name": "transaction", "poolId": "regression", "addPoolId": "None"},
              {"name": "sanity", "poolId": "regression", "addPoolId": "None"},
              {"name": "subdoc", "poolId": "regression", "addPoolId": "None"}]


class GenericOps(object):
    def __init__(self, bucket):
        self.bucket = bucket

    def close_bucket(self):
        self.bucket._close()

    def run_query(self, query):

        # Add retries if the query fails due to intermittent network issues
        result = []
        CURR_RETRIES = 0
        while CURR_RETRIES < MAX_RETRIES:
            try:
                row_iter = self.bucket.n1ql_query(N1QLQuery(query))
                if row_iter:
                    for row in row_iter:
                        row = ast.literal_eval(json.dumps(row))
                        result.append(row)
                    break
                return result
            except HTTPError as e:
                logger.info(str(e))
                logger.info('Retrying query...')
                time.sleep(COOLDOWN)
                CURR_RETRIES += 1
                pass
            if CURR_RETRIES == MAX_RETRIES:
                raise Exception('Unable to query the Couchbase server...')
        return result


class GreenBoardCluster(GenericOps):

    def __init__(self):
        self.greenboard_cluster = Cluster('couchbase://%s:%s' % (GREENBOARD_DB_HOST, "8091"))
        self.greenboard_authenticator = PasswordAuthenticator(GREENBOARD_DB_USERNAME, GREENBOARD_DB_PASSWORD)
        self.greenboard_cluster.authenticate(self.greenboard_authenticator)

    def get_greenboard_cluster(self):
        return self.get_greenboard_cluster()


class GreenBoardBucket(GreenBoardCluster):

    def __init__(self):
        GreenBoardCluster.__init__(self)

    def get_greenboard_bucket(self):
        self.bucket = self.greenboard_cluster.open_bucket(GREENBOARD_DB_BUCKETNAME, lockmode=LOCKMODE_WAIT)
        return self.bucket

    def run_query(self, query):
        self.get_greenboard_bucket()
        results = super(GreenBoardBucket, self).run_query(query)
        self.close_bucket()
        return results


class GreenBoardHistoryBucket(GreenBoardCluster):

    def __init__(self):
        GreenBoardCluster.__init__(self)

    def get_reun_job_db(self):
        self.bucket = self.greenboard_cluster.open_bucket(RERUN_JOBS_HISTORY_BUCKETNAME, lockmode=LOCKMODE_WAIT)
        return self.bucket

    def run_query(self, query):
        self.get_reun_job_db()
        results = super(GreenBoardHistoryBucket, self).run_query(query)
        self.close_bucket()
        return results


class ServerPoolCluster(GenericOps):

    def __init__(self):
        pass

    def get_server_pool_db(self):
        self.bucket = Bucket('couchbase://' + SERVER_POOL_DB_HOST + '/QE-Test-Suites?operation_timeout=60', lockmode=LOCKMODE_WAIT)
        return self.bucket

    def run_query(self, query):
        self.get_server_pool_db()
        results = super(ServerPoolCluster, self).run_query(query)
        self.close_bucket()
        return results


class RerunFailedJobs:

    rerun_jobs_queue = None  # type: List[Any]

    def __init__(self):

        self.green_board_bucket = GreenBoardBucket()
        self.server_pool_cluster = ServerPoolCluster()
        self.green_board_history_bucket = GreenBoardHistoryBucket()

        # Initialize the job run queue
        self.rerun_jobs_queue = []
        self._lock_queue = threading.Lock()

    def find_jobs_to_rerun(self):
        jobs_to_rerun = []
        all_results = []
        # Query couchbase to get jobs that have result=FAILURE. Add them to the rerun queue
        query = "select `build`, name,component,failCount,totalCount,build_id,url||tostring(build_id) as full_url, \
                'job failed' as reason from {0} where `build`='{1}'\
                and lower(os)='centos' and result='FAILURE' and ( url like '%test_suite_executor-jython/%' or url like \
                '%test_suite_executor-TAF/%' or url like '%test_suite_executor/%') \
                and name not like 'centos-rqg%' order by name;".format(GREENBOARD_DB_BUCKETNAME, current_build_num)

        logger.info("Running query : %s" % query)
        results = self.green_board_bucket.run_query(query)
        all_results.extend(results)

        # Query couchbase to get jobs that have result=Aborted or unstable and failcount=totalcount. Add them to the rerun queue
        query = "select `build`,name,component,failCount,totalCount,build_id,url||tostring(build_id) as full_url, \
                '0 tests passed' as reason from {0} where `build`='{1}'\
                and lower(os)='centos' and result in ['UNSTABLE','ABORTED'] and failCount=totalCount\
                and result='FAILURE' and ( url like '%test_suite_executor-jython/%' or url like '%test_suite_executor-TAF/%' or url like '%test_suite_executor/%') order by name;".format(
            GREENBOARD_DB_BUCKETNAME, current_build_num)
        logger.info("Running query : %s" % query)
        results = self.green_board_bucket.run_query(query)
        all_results.extend(results)

        # Query couchbase to get jobs that have result=Unstable. Fetch the failcount for these jobs from the last weekly build, and compare with the current ones. If there are
        #    more failures this week, add them to the rerun queue
        query = "select s1.`build`,s1.name, s1.component, s1.failCount, s1.totalCount, s1.build_id, s1.url || tostring(s1.build_id) \
                as full_url, 'more failures than {2}' as reason from {0} s1 left outer join {0} s2 on s1.name = s2.name\
                and s2. `build` = '{2}' \
                and lower(s2.os) = 'centos' and s2.result = 'UNSTABLE' and ( s2.url like '%test_suite_executor-jython/%' or s2.url like '%test_suite_executor-TAF/%' or s2.url like '%test_suite_executor/%') \
                and s2.name not like 'centos-rqg%' where s1.`build` = '{1}' and lower(s1.os) = 'centos' \
                and s1.result = 'UNSTABLE' and ( s1.url like '%test_suite_executor-jython/%' or s1.url like '%test_suite_executor-TAF/%' or s1.url like '%test_suite_executor/%') and s1.name not like 'centos-rqg%'\
                and (s1.failCount - s2.failCount) > 0 order by s1.name".format(GREENBOARD_DB_BUCKETNAME,
                                                                               current_build_num, prev_stable_build_num)

        logger.info("Running query : %s" % query)
        results = self.green_board_bucket.run_query(query)
        all_results.extend(results)

        for job in all_results:
            if not self.to_be_filtered(job):
                jobs_to_rerun.append(job)

        return jobs_to_rerun

    def to_be_filtered(self, job):
        # Remove jobs from the list if :

        component, subcomponent = self.process_job_name(job)

        # 1) Same job (unique build_id, job name) is already queued up
        if len([i for i in self.rerun_jobs_queue if
                (i['component'] == component and i['subcomponent'] == subcomponent)]) != 0:
            return True

        # 2) Same job (unique build_id, job name) was already re-run (might be still running)
        query = "select raw count(*) from {0} where `build`='{1}' and build_id={2} and name='{3}'".format(
            RERUN_JOBS_HISTORY_BUCKETNAME, current_build_num, job['build_id'], job['name'])
        count = int(self.green_board_history_bucket.run_query(query)[0])
        if count > 0:
            return True

        # 3) Same job (job name) has been re-run MAX_RERUN times.
        query = "select raw count(*) from {0} where `build`='{1}' and name='{2}'".format(
            RERUN_JOBS_HISTORY_BUCKETNAME, current_build_num, job['name'])
        count = int(self.green_board_history_bucket.run_query(query)[0])
        if count > MAX_RERUN:
            return True

        return False

    def process_job_name(self, job):
        comp_subcomp = job['name'][7:]
        tokens = comp_subcomp.split("_", 2)
        if tokens[0] in ["backup", "cli"]:
            if (tokens[0] == "backup" and tokens[1] == "recovery") or (
                    tokens[0] == "cli" and tokens[1] == "imex") or (
                    tokens[0] == "cli" and tokens[1] == "tools"):
                component = tokens[0] + "_" + tokens[1]
                subcomponent = "_".join(tokens[2:])
            else:
                component = tokens[0]
                subcomponent = "_".join(tokens[1:])
        else:
            component = tokens[0]
            subcomponent = "_".join(tokens[1:])

        return component, subcomponent

    def save_rerun_job_history(self, job):
        iteration = self.get_iteration(job)
        doc_name = job['name'] + "_" + job['build'] + "_" + str(iteration)
        job["timestamp"] = datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
        self.green_board_history_bucket.get_reun_job_db().upsert(doc_name, job)
        self.green_board_history_bucket.close_bucket()

    def get_iteration(self, job):
        query = "select raw count(*) from {0} where `build`='{1}' and name='{2}'".format(
            RERUN_JOBS_HISTORY_BUCKETNAME, current_build_num, job['name'])
        count = int(self.green_board_history_bucket.run_query(query)[0])
        return count+1

    def manage_rerun_jobs_queue(self, jobs_to_rerun):
        rerun_job_details = []
        for job in jobs_to_rerun:
            component, subcomponent = self.process_job_name(job)
            job["component"] = component
            job["subcomponent"] = subcomponent
            rerun_job_details.append(job)

        with self._lock_queue:
            self.rerun_jobs_queue.extend(rerun_job_details)
            logger.info("current rerun job queue")
            self.print_current_queue()

    def print_current_queue(self):
        logger.info("========================================")
        for job in self.rerun_jobs_queue:
            logger.info("||" + job["component"] + " " + job["subcomponent"] + "||")
        logger.info("========================================")

    def get_available_serverpool_machines(self, poolId):
        query = "SELECT raw count(*) FROM `{0}` where state ='available' and os = 'centos' and \
                (poolId = '{1}' or '{1}' in poolId)".format(
            SERVER_POOL_DB_BUCKETNAME, poolId)
        logger.info("Running query : %s" % query)
        available_vms = self.server_pool_cluster.run_query(query)[0]
        logger.info("number of available machines : {0}".format(available_vms))
        return available_vms

    def form_rerun_job_matrix(self):
        rerun_job_matrix = {}
        for job in self.rerun_jobs_queue:
            comp = job["component"]
            if comp in rerun_job_matrix.keys():
                comp_details = rerun_job_matrix[comp]
                sub_component_list = comp_details["subcomponent"]
                if job["subcomponent"] not in sub_component_list:
                    sub_component_list = sub_component_list + "," + job["subcomponent"]
                    count = comp_details["count"] + 1
            else:
                rerun_job_matrix[comp] = {}
                sub_component_list = job["subcomponent"]
                count = 1
            rerun_job_matrix[comp]["subcomponent"] = sub_component_list
            rerun_job_matrix[comp]["poolId"] = next(item for item in components if item["name"] == comp)["poolId"]
            rerun_job_matrix[comp]["addPoolId"] = next(item for item in components if item["name"] == comp)["addPoolId"]
            rerun_job_matrix[comp]["count"] = count

        logger.info("current rerun_job_matrix")
        for comp in rerun_job_matrix:
            logger.info(comp + " || " + str(rerun_job_matrix[comp]))

        return rerun_job_matrix

    def trigger_jobs(self):
        # 1. Get the number of available machines in the regression queue
        # 2. If available number > 10, then start with one component in the queue.
        # 3. Call test_suite_dispatcher job for that component.
        # 4. Wait for (no. of jobs to be rerun * 40s) + 1m
        # Repeat #1-4

        # wget "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=centos&version_number=$version_number&suite=12hour&component=subdoc,xdcr,rbac,query,nserv,2i,eventing,backup_recovery,sanity,ephemeral,epeng&url=$url&serverPoolId=regression&branch=$branch"

        # Segregate subcomponents by component in the queue
        logger.info("current job queue")
        self.print_current_queue()
        if self.rerun_jobs_queue:
            rerun_job_matrix = self.form_rerun_job_matrix()
            for comp in rerun_job_matrix:
                sleep_time = 60
                comp_rerun_details = rerun_job_matrix.get(comp)
                logger.info("processing : {0} : {1}".format(comp, comp_rerun_details))
                pool_id = comp_rerun_details["poolId"]
                available_vms = self.get_available_serverpool_machines(pool_id)
                if available_vms >= MAX_AVAILABLE_VMS_TO_RERUN:

                    url = "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?" \
                          "token={0}&OS={1}&version_number={2}&suite={3}&component={4}&subcomponent={5}&serverPoolId={6}&branch={7}&addPoolId={8}". \
                        format("extended_sanity", "centos", current_build_num, "12hr_weekly", comp, comp_rerun_details["subcomponent"], comp_rerun_details["poolId"],
                               "master", comp_rerun_details["addPoolId"])
                    logger.info("Triggering job with URL " + str(url))
                    response = requests.get(url, verify=True)
                    if not response.ok:
                        logger.error("Error in triggering job")
                        logger.error(str(response))
                    else:
                        # Save history for these jobs
                        for job in self.rerun_jobs_queue:
                            if job["component"] == comp:
                                self.save_rerun_job_history(job)

                        # Remove component from run queue
                        with self._lock_queue:
                            self.rerun_jobs_queue[:] = [job for job in self.rerun_jobs_queue if job.get('component') != comp]
                        sleep_time = (comp_rerun_details["count"] * 40) + 60
                    logger.info("sleeping for {0} before triggering again".format(sleep_time))
                time.sleep(sleep_time)

    def trigger_jobs_constantly(self):
        while run_infinite == "True" or self.rerun_jobs_queue:
            time.sleep(40)
            self.trigger_jobs()

        logger.info("stopping triggering jobs as job queue is empty")

    def find_and_manage_rerun_jobs(self):
        jobs_to_rerun = self.find_jobs_to_rerun()
        self.manage_rerun_jobs_queue(jobs_to_rerun)
        while run_infinite == "True" or self.rerun_jobs_queue:
            jobs_to_rerun = self.find_jobs_to_rerun()
            self.manage_rerun_jobs_queue(jobs_to_rerun)
            logger.info("sleeping for 1200 secs before triggering again")
            time.sleep(1200)

        logger.info("stopping monitoring")


if __name__ == '__main__':

    # Spawn 2 never ending threads - 1 to populate the re-run queue, other to dispatch jobs
    # whenever there are free machines

    rerun_inst = RerunFailedJobs()

    get_rerun_job_thread = Thread(target=rerun_inst.find_and_manage_rerun_jobs)
    get_rerun_job_thread.start()

    time.sleep(10)

    trigger_jobs_thread = Thread(target=rerun_inst.trigger_jobs_constantly)
    trigger_jobs_thread.start()

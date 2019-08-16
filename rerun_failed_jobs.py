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

current_build_num = sys.argv[1]
prev_stable_build_num = sys.argv[2]
run_infinite = sys.argv[3]
LOCKMODE_WAIT = 100

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
             {"name": "transaction", "poolId": "regression", "addPoolId": "None"}]



class Rerun_Failed_Jobs():

    def __init__(self):

        self.cb = Bucket('couchbase://' + SERVER_POOL_DB_HOST + '/QE-Test-Suites?operation_timeout=60', lockmode=LOCKMODE_WAIT)

        # Authenticate to the Greenboard Couchbase server
        self.greenboard_cluster = Cluster('couchbase://%s:%s' % (GREENBOARD_DB_HOST, "8091"))
        self.greenboard_authenticator = PasswordAuthenticator(GREENBOARD_DB_USERNAME, GREENBOARD_DB_PASSWORD)
        self.greenboard_cluster.authenticate(self.greenboard_authenticator)

        # Open the Greenboard DB bucket
        self.greenboard_cb = self.greenboard_cluster.open_bucket(GREENBOARD_DB_BUCKETNAME, lockmode=LOCKMODE_WAIT)

        # Open the Re-run Jobs History bucket
        self.rerun_history_cb = self.greenboard_cluster.open_bucket(RERUN_JOBS_HISTORY_BUCKETNAME, lockmode=LOCKMODE_WAIT)

        # Initialize the job run queue
        self.rerun_jobs_queue = []
        self._lock_queue = threading.Lock()
        self._lock_query = threading.Lock()

    def find_jobs_to_rerun(self):
        jobs_to_rerun = []
        all_results = []
        # Query couchbase to get jobs that have result=FAILURE. Add them to the rerun queue
        query = "select `build`, name,component,failCount,totalCount,build_id,url||tostring(build_id) as full_url, \
                'job failed' as reason from {0} where `build`='{1}'\
                and lower(os)='centos' and result='FAILURE' and url like '%test_suite_executor/%'\
                and name not like 'centos-rqg%' order by name;".format(GREENBOARD_DB_BUCKETNAME, current_build_num)

        print ("Running query : %s" % query)
        results = self.run_query(query)
        all_results.extend(results)

        # Query couchbase to get jobs that have result=Aborted or unstable and failcount=totalcount. Add them to the rerun queue
        query = "select `build`,name,component,failCount,totalCount,build_id,url||tostring(build_id) as full_url, \
                '0 tests passed' as reason from {0} where `build`='{1}'\
                and lower(os)='centos' and result in ['UNSTABLE','ABORTED'] and failCount=totalCount\
                and result='FAILURE' and url like '%test_suite_executor/%' order by name;".format(
            GREENBOARD_DB_BUCKETNAME, current_build_num)
        print ("Running query : %s" % query)
        results = self.run_query(query)
        all_results.extend(results)

        # Query couchbase to get jobs that have result=Unstable. Fetch the failcount for these jobs from the last weekly build, and compare with the current ones. If there are
        #    more failures this week, add them to the rerun queue
        query = "select s1.`build`,s1.name, s1.component, s1.failCount, s1.totalCount, s1.build_id, s1.url || tostring(s1.build_id) \
                as full_url, 'more failures than {2}' as reason from {0} s1 left outer join {0} s2 on s1.name = s2.name\
                and s2. `build` = '{2}' \
                and lower(s2.os) = 'centos' and s2.result = 'UNSTABLE' and s2.url like '%test_suite_executor/%' \
                and s2.name not like 'centos-rqg%' where s1.`build` = '{1}' and lower(s1.os) = 'centos' \
                and s1.result = 'UNSTABLE' and s1.url like '%test_suite_executor/%' and s1.name not like 'centos-rqg%'\
                and (s1.failCount - s2.failCount) > 0 order by s1.name".format(GREENBOARD_DB_BUCKETNAME,
                                                                               current_build_num, prev_stable_build_num)

        print ("Running query : %s" % query)
        results = self.run_query(query)
        all_results.extend(results)
        for job in all_results:
            if not self.check_if_exists_in_queue(job) and not self.to_be_filtered(job):
                jobs_to_rerun.append(job)
        #jobs_to_rerun.sort()

        print jobs_to_rerun

        return jobs_to_rerun

    def check_if_exists_in_queue(self, job):
        component, subcomponent = self.process_job_name(job)
        for job_queue in self.rerun_jobs_queue:
            job_queue_component, job_queue_subcomponent = self.process_job_name(job_queue)
            if component == job_queue_component and subcomponent == job_queue_subcomponent:
                return True
        return False

    def to_be_filtered(self, job):
        # Remove jobs from the list if :
        # 1) Same job (unique build_id, job name) is already queued up
        component, subcomponent = self.process_job_name(job)
        job_dict = job
        if len([i for i in self.rerun_jobs_queue if
                (i['component'] == component and i['subcomponent'] == subcomponent)]) != 0:
            return True
        else:
            # 2) Same job (unique build_id, job name) was already re-run (might be still running)
            query = "select raw count(*) from {0} where `build`='{1}' and build_id={2} and name='{3}'".format(
                RERUN_JOBS_HISTORY_BUCKETNAME, current_build_num, job_dict['build_id'], job_dict['name'])
            count = int(self.run_query(query, bucket=self.rerun_history_cb)[0])
            if count > 0:
                return True
            # 3) Same job (job name) has been re-run MAX_RERUN times.
            else:
                query = "select raw count(*) from {0} where `build`='{1}' and name='{2}'".format(
                    RERUN_JOBS_HISTORY_BUCKETNAME, current_build_num, job_dict['name'])
                count = int(self.run_query(query, bucket=self.rerun_history_cb)[0])
                if count > MAX_RERUN:
                    return True

    def process_job_name(self, job):
#        job_dict = ast.literal_eval(job)
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
        self.rerun_history_cb.upsert(doc_name, job)

    def get_iteration(self, job):
        query = "select raw count(*) from {0} where `build`='{1}' and name='{2}'".format(
            RERUN_JOBS_HISTORY_BUCKETNAME, current_build_num, job['name'])
        count = int(self.run_query(query, bucket=self.rerun_history_cb)[0])
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
            #self.rerun_jobs_queue.sort(key=lambda i: i['component'])

            print "========================================"
            for job in self.rerun_jobs_queue:
                print "||" + job["component"] + " " + job["subcomponent"] + "||"
            print "========================================"

    def trigger_jobs(self):
        # 1. Get the number of available machines in the regression queue
        # 2. If available number > 10, then start with one component in the queue.
        # 3. Call test_suite_dispatcher job for that component.
        # 4. Wait for (no. of jobs to be rerun * 40s) + 1m
        # Repeat #1-4

        # wget "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=centos&version_number=$version_number&suite=12hour&component=subdoc,xdcr,rbac,query,nserv,2i,eventing,backup_recovery,sanity,ephemeral,epeng&url=$url&serverPoolId=regression&branch=$branch"

        # Segregate subcomponents by component in the queue
        print self.rerun_jobs_queue
        if self.rerun_jobs_queue:
            component = self.rerun_jobs_queue[0]["component"]
            subcomponent = self.rerun_jobs_queue[0]["subcomponent"]
            i = 1
            for job in self.rerun_jobs_queue:
                if job["component"] == component and job["subcomponent"] not in subcomponent:
                    subcomponent = subcomponent + "," + job["subcomponent"]
                    i = i + 1

            #sleeptime = (i*40)+60

            poolId = next(item for item in components if item["name"] == component)["poolId"]
            addPoolId = next(item for item in components if item["name"] == component)["addPoolId"]

            query = "SELECT raw count(*) FROM `{0}` where state ='available' and os = 'centos' and (poolId = '{1}' or '{1}' in poolId)".format(
                SERVER_POOL_DB_BUCKETNAME, poolId)
            print ("Running query : %s" % query)
            availableVMs = self.run_query(query, self.cb)[0]

            if availableVMs >= 10:

                url = "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?" \
                      "token={0}&OS={1}&version_number={2}&suite={3}&component={4}&subcomponent={5}&serverPoolId={6}&branch={7}&addPoolId={8}". \
                    format("extended_sanity", "centos", current_build_num, "12hr_weekly", component, subcomponent, poolId,
                           "master", addPoolId)
                print url
                response = requests.get(url, verify=True)
                if not response.ok:
                    print "Error in triggering job"
                    print response
                else:
                    # Save history for these jobs

                    # Remove component from run queue
                    with self._lock_queue:
                        for job in self.rerun_jobs_queue:
                            if job["component"] == component:
                                self.save_rerun_job_history(job)
                        self.rerun_jobs_queue[:] = [job for job in self.rerun_jobs_queue if job.get('component') != component]


    def run_query(self, query, bucket=None):
        with self._lock_query:
            if not bucket:
                bucket = self.greenboard_cb

            # Add retries if the query fails due to intermittent network issues
            result = []
            CURR_RETRIES = 0
            while CURR_RETRIES < MAX_RETRIES:
                try:
                    row_iter = bucket.n1ql_query(N1QLQuery(query))
                    if row_iter:
                        for row in row_iter:
                            row = ast.literal_eval(json.dumps(row))
                            result.append(row)
                        break
                    return result
                except HTTPError as e:
                    print e
                    print('Retrying query...')
                    time.sleep(COOLDOWN)
                    CURR_RETRIES += 1
                    pass
                if CURR_RETRIES == MAX_RETRIES:
                    raise Exception('Unable to query the Couchbase server...')
            return result

    def trigger_jobs_constantly(self):
        while run_infinite == "True" or self.rerun_jobs_queue:
            time.sleep(20)
            self.trigger_jobs()

        print "stopping triggering jobs as job queue is empty"

    def find_and_manage_rerun_jobs(self):
        jobs_to_rerun = self.find_jobs_to_rerun()
        self.manage_rerun_jobs_queue(jobs_to_rerun)
        time.sleep(10)
        if run_infinite == "False" and not self.rerun_jobs_queue:
            print "stopping monitoring"
            return
        else:
            self.find_and_manage_rerun_jobs()


if __name__ == '__main__':
    # Spawn 2 never ending threads - 1 to populate the re-run queue, other to dispatch jobs whenever there are free machines
    rerun_inst = Rerun_Failed_Jobs()
    jobs_to_rerun = rerun_inst.find_jobs_to_rerun()

    get_rerun_job_thread = Thread(target=rerun_inst.find_and_manage_rerun_jobs)
    get_rerun_job_thread.start()

    time.sleep(10)

    trigger_jobs_thread = Thread(target=rerun_inst.trigger_jobs_constantly)
    trigger_jobs_thread.start()

    #rerun_inst.save_rerun_jobs_history(jobs_to_rerun, 1)
    #rerun_inst.manage_rerun_jobs_queue(jobs_to_rerun)
    #rerun_inst.trigger_jobs_constantly()

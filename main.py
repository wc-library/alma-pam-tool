import json
import asyncio
import aiohttp
import math
import time
import datetime
import codecs
from wakepy import keepawake

# --------------------
# Constants
# --------------------

# fill in and modify as needed
APIKEY = ""
BASEURL = "https://api-na.hosted.exlibrisgroup.com/almaws/v1/electronic"
HEADERS = {"accept": "application/json", "Content-Type": "application/json"}
START = time.monotonic()
MAX_API_CALLS_PER_DAY = 10000  # check your institution limit and account for other systems that might also use API calls.

# ---------------------
# Per Run Configuration
# ---------------------

# Mode
# accepted values are 'review', 'update', "clear_cache_all", and "clear_cache_collection", "clear_cache_portfolios"
mode = "review"
# Collection ID and Service ID
collectionid = ""
serviceid = ""
# Public Access Model
# set to the values that updated portfolios should use
public_access_model_code = "UA"
public_access_model_description = (
    "- Please note that the platform supports unlimited access"
)

# ---------------------
# End of Configuration
# ---------------------

errors = []
update_log_data = {
    "updated_portfolios": [],
    "update_failed_portfolios": [],
    "unchanged_portfolios": [],
    "total_in_collection": 0,
    "api_limit_reached": False,
}

review_log_data = {
    "reviewed_portfolios": [],
    "pam_types": set(),
    "total_in_collection": 0,
    "api_limit_reached": False,
}


class Cache:
    CACHE_TTL = 60 * 60 * 24 * 7
    EMPTY_CACHE = {
        "collection_overviews": [
            {
                "collection_id": "",
                "retrieved": 0.0,
                "data": {},
            },
        ],
        "portfolios_retrieved": [
            {
                "collection_id": "",
                "retrieved": 0.0,
                "data": [],
            },
        ],
        "portfolios_updated": [
            {
                "collection_id": "",
                "updated": 0.0,
                "data": [],
            },
        ],
        "portfolios_ready_to_update": [
            {
                "collection_id": "",
                "saved": 0.0,
                "data": [],
            },
        ],
        "portfolios_not_updating": [
            {
                "collection_id": "",
                "saved": 0.0,
                "data": [],
            },
        ],
        "api_calls_logged": [
            {
                "count": 0,
                "time": datetime.datetime.now().astimezone().isoformat(),
            },
        ],
        "total_api_calls_past_24_hrs": 0,
    }

    def __init__(self, loadedcache=None):
        if loadedcache is None:
            loadedcache = self.EMPTY_CACHE

        self.collection_overviews = loadedcache["collection_overviews"]
        self.portfolios_retrieved = loadedcache["portfolios_retrieved"]
        self.portfolios_updated = loadedcache["portfolios_updated"]
        self.portfolios_ready_to_update = loadedcache["portfolios_ready_to_update"]
        self.portfolios_not_updating = loadedcache["portfolios_not_updating"]
        self.api_calls_logged = loadedcache["api_calls_logged"]
        self.total_api_calls_past_24_hrs = loadedcache["total_api_calls_past_24_hrs"]
        self.expire_all()
        self.sum_api_calls()

    # methods to return portfolio objects for the current collectionid
    # could be empty lists
    def get_overview_port_ids(self):
        # check self.collection_overviews for collection_id
        # return the ids of each portfolio in the object
        portfolio_ids = []
        for collection in self.collection_overviews:
            if collection["collection_id"] == collectionid:
                for portfolio in collection["data"]:
                    portfolio_ids.append(portfolio["id"])
        return portfolio_ids

    def get_overview_port_first_retrieved_timestamp(self):
        retrieval_dates = []
        for collection in self.collection_overviews:
            if collection["collection_id"] == collectionid:
                retrieval_dates.append(collection["retrieved"])

        if retrieval_dates == []:
            return 0
        else:
            retrieval_dates.sort()
            return retrieval_dates[0]

    def get_retrieved_port_ids(self):
        portfolio_ids = []
        for portfolio in self.portfolios_retrieved:
            if portfolio["collection_id"] == collectionid:
                for port in portfolio["data"]:
                    portfolio_ids.append(port["id"])
        return portfolio_ids

    def get_retrieved_portfolios(self):
        portfolios = []
        for portfolio in self.portfolios_retrieved:
            if portfolio["collection_id"] == collectionid:
                portfolios = portfolios + portfolio["data"]
        return portfolios

    def get_portfolios_first_retrieved(self):
        retrieval_dates = []
        for portfolio in self.portfolios_retrieved:
            if portfolio["collection_id"] == collectionid:
                retrieval_dates.append(portfolio["retrieved"])

        if retrieval_dates == []:
            return 0
        else:
            retrieval_dates.sort()
            return retrieval_dates[0]

    def get_updated_portfolios(self):
        portfolios = []
        for portfolio in self.portfolios_updated:
            if portfolio["collection_id"] == collectionid:
                portfolios = portfolios + portfolio["data"]

        return portfolios

    def get_ready_to_update_portfolios(self):
        portfolios_to_update = []
        for portfolio in self.portfolios_ready_to_update:
            if portfolio["collection_id"] == collectionid:
                portfolios_to_update = portfolios_to_update + portfolio["data"]
        return portfolios_to_update

    def get_not_updating_portfolios(self):
        portfolios_not_to_update = []
        for portfolio in self.portfolios_not_updating:
            if portfolio["collection_id"] == collectionid:
                portfolios_not_to_update = portfolios_not_to_update + portfolio["data"]
        return portfolios_not_to_update

    def get_remaining_api_calls(self):
        return MAX_API_CALLS_PER_DAY - self.total_api_calls_past_24_hrs

    def expire_all(self):
        self.expire_overviews()
        self.expire_portfolios_retrieved()
        self.expire_portfolios_updated()
        self.expire_portfolios_ready_to_update()
        self.expire_portfolios_not_updating()
        self.expire_api_calls()

    def expire_overviews(self):
        overviews_to_keep = []
        for overview in self.collection_overviews:
            if overview["retrieved"] > (time.time() - self.CACHE_TTL):
                overviews_to_keep.append(overview)
        self.collection_overviews = overviews_to_keep

    def expire_portfolios_retrieved(self):
        portfolios_to_keep = []
        for portfolio in self.portfolios_retrieved:
            if portfolio["retrieved"] > (time.time() - self.CACHE_TTL):
                portfolios_to_keep.append(portfolio)
        self.portfolios_retrieved = portfolios_to_keep

    def expire_portfolios_updated(self):
        portfolios_to_keep = []
        for portfolio in self.portfolios_updated:
            if portfolio["updated"] > (time.time() - self.CACHE_TTL):
                portfolios_to_keep.append(portfolio)
        self.portfolios_updated = portfolios_to_keep

    def expire_portfolios_ready_to_update(self):
        portfolios_to_keep = []
        for portfolio in self.portfolios_ready_to_update:
            if portfolio["saved"] > (time.time() - self.CACHE_TTL):
                portfolios_to_keep.append(portfolio)
        self.portfolios_ready_to_update = portfolios_to_keep

    def expire_portfolios_not_updating(self):
        portfolios_to_keep = []
        for portfolio in self.portfolios_not_updating:
            if portfolio["saved"] > (time.time() - self.CACHE_TTL):
                portfolios_to_keep.append(portfolio)
        self.portfolios_not_updating = portfolios_to_keep

    def expire_api_calls(self):
        utc_now = datetime.datetime.now(datetime.timezone.utc)
        utc_midnight = datetime.datetime.combine(
            utc_now, datetime.datetime.min.time(), tzinfo=datetime.timezone.utc
        )
        api_calls_to_keep = []
        for session in self.api_calls_logged:
            if datetime.datetime.fromisoformat(session["time"]) > utc_midnight:
                api_calls_to_keep.append(session)
        self.api_calls_logged = api_calls_to_keep

    def sum_api_calls(self):
        total = 0
        self.expire_api_calls()

        for api_call_set in self.api_calls_logged:
            total = total + api_call_set["count"]

        self.total_api_calls_past_24_hrs = total

    def add_collection_overview(self, overview):
        # get collection id, time, and build overview wrapper,
        # then append it to the list
        newoverview = {
            "collection_id": collectionid,
            "retrieved": time.time(),
            "data": overview,
        }
        self.collection_overviews.append(newoverview)

    def add_portfolios_retrieved(self, portfolios):
        # get collection id, time, and build wrapper,
        # then append it to the list
        newportfoliolist = {
            "collection_id": collectionid,
            "retrieved": time.time(),
            "data": portfolios,
        }
        self.portfolios_retrieved.append(newportfoliolist)

    def add_portfolios_updated(self, portfolios):
        # get collection id, time, and build wrapper,
        # then append it to the list
        newportfoliolist = {
            "collection_id": collectionid,
            "updated": time.time(),
            "data": portfolios,
        }
        self.portfolios_updated.append(newportfoliolist)

    def add_portfolios_ready_to_update(self, portfolios):
        # get collection id, time, and build wrapper,
        # then append it to the list
        newportfoliolist = {
            "collection_id": collectionid,
            "saved": time.time(),
            "data": portfolios,
        }
        self.portfolios_ready_to_update.append(newportfoliolist)

    def add_portfolios_not_updating(self, portfolios):
        # get collection id, time, and build wrapper,
        # then append it to the list
        newportfoliolist = {
            "collection_id": collectionid,
            "saved": time.time(),
            "data": portfolios,
        }
        self.portfolios_not_updating.append(newportfoliolist)

    def add_api_call_set(self, count):
        # get time and build wrapper,
        # then append it to the list
        newapicallset = {
            "count": count,
            "time": datetime.datetime.now().astimezone().isoformat(),
        }
        self.api_calls_logged.append(newapicallset)
        self.sum_api_calls()

    def remove_collection_overview(self):
        overviews_to_keep = []
        for overview in self.collection_overviews:
            if overview["collection_id"] != collectionid:
                overviews_to_keep.append(overview)
        self.collection_overviews = overviews_to_keep

    def remove_all_portfolios_retrieved_by_collection(self):
        portset_to_keep = []
        for portset in self.portfolios_retrieved:
            if portset["collection_id"] != collectionid:
                portset_to_keep.append(portset)

        self.portfolios_retrieved = portset_to_keep

    def remove_portfolio_from_portfolios_retrieved(self, portfolio):
        # find the portfolio in the cache and remove it
        portset_to_keep = []
        for portset in self.portfolios_retrieved:
            ports_to_keep = []
            if portset["collection_id"] == collectionid:
                for port in portset["data"]:
                    if port != portfolio:
                        ports_to_keep.append(port)
                portset["data"] = ports_to_keep
                portset_to_keep.append(portset)
            else:
                ports_to_keep.append(portset)
        self.portfolios_retrieved = portset_to_keep

    def remove_all_portfolios_updated_by_collection(self):
        portset_to_keep = []
        for portset in self.portfolios_updated:
            if portset["collection_id"] != collectionid:
                portset_to_keep.append(portset)

        self.portfolios_updated = portset_to_keep

    def remove_portfolio_from_portfolios_updated(self, portfolio):
        # find the portfolio in the cache and remove it
        portset_to_keep = []
        for portset in self.portfolios_updated:
            ports_to_keep = []
            if portset["collection_id"] == collectionid:
                for port in portset["data"]:
                    if port != portfolio:
                        ports_to_keep.append(port)
                portset["data"] = ports_to_keep
                portset_to_keep.append(portset)
            else:
                portset_to_keep.append(portset)
        self.portfolios_updated = portset_to_keep

    def remove_portfolio_from_portfolios_not_updating(self, portfolio):
        # find the portfolio in the cache and remove it
        portset_to_keep = []
        for portset in self.portfolios_not_updating:
            ports_to_keep = []
            if portset["collection_id"] == collectionid:
                for port in portset["data"]:
                    if port != portfolio:
                        ports_to_keep.append(port)
                portset["data"] = ports_to_keep
                portset_to_keep.append(portset)
            else:
                portset_to_keep.append(portset)
        self.portfolios_not_updating = portset_to_keep

    def remove_all_portfolios_not_updating_by_collection(self):
        portset_to_keep = []
        for portset in self.portfolios_not_updating:
            if portset["collection_id"] != collectionid:
                portset_to_keep.append(portset)

        self.portfolios_not_updating = portset_to_keep

    def remove_all_portfolios_ready_to_update_by_collection(self):
        portset_to_keep = []
        for portset in self.portfolios_ready_to_update:
            if portset["collection_id"] != collectionid:
                portset_to_keep.append(portset)

        self.portfolios_ready_to_update = portset_to_keep

    def remove_portfolio_from_portfolios_ready_to_update(self, portfolio):
        # find the portfolio in the cache and remove it
        ports_to_keep = []
        portset_to_keep = []
        for portset in self.portfolios_ready_to_update:
            if portset["collection_id"] == collectionid:
                for port in portset["data"]:
                    if port != portfolio:
                        ports_to_keep.append(port)
                portset["data"] = ports_to_keep
                portset_to_keep.append(portset)
            else:
                ports_to_keep.append(portset)
        self.portfolios_ready_to_update = portset_to_keep

    def remove_all_but_api(self):
        self.collection_overviews = self.EMPTY_CACHE["collection_overviews"]
        self.portfolios_retrieved = self.EMPTY_CACHE["portfolios_retrieved"]
        self.portfolios_updated = self.EMPTY_CACHE["portfolios_updated"]
        self.portfolios_ready_to_update = self.EMPTY_CACHE["portfolios_ready_to_update"]
        self.portfolios_not_updating = self.EMPTY_CACHE["portfolios_not_updating"]

    def json(self):
        # return json object (for saving to file)
        obj = {
            "collection_overviews": self.collection_overviews,
            "portfolios_retrieved": self.portfolios_retrieved,
            "portfolios_updated": self.portfolios_updated,
            "portfolios_ready_to_update": self.portfolios_ready_to_update,
            "portfolios_not_updating": self.portfolios_not_updating,
            "api_calls_logged": self.api_calls_logged,
            "total_api_calls_past_24_hrs": self.total_api_calls_past_24_hrs,
        }
        return json.dumps(obj)


class RateLimiter:
    RATE = 25
    MAX_TOKENS = 25

    def __init__(self, client):
        self.client = client
        self.tokens = self.MAX_TOKENS
        self.updated_at = time.monotonic()

    async def get(self, *args, **kwargs):
        await self.wait_for_token()
        return self.client.get(*args, **kwargs)

    async def put(self, *args, **kwargs):
        await self.wait_for_token()
        return self.client.put(*args, **kwargs)

    async def wait_for_token(self):
        while self.tokens < 1:
            self.add_new_tokens()
            await asyncio.sleep(0.1)
        self.tokens -= 1

    def add_new_tokens(self):
        now = time.monotonic()
        time_since_update = now - self.updated_at
        new_tokens = time_since_update * self.RATE
        if self.tokens + new_tokens >= 1:
            self.tokens = min(self.tokens + new_tokens, self.MAX_TOKENS)
            self.updated_at = now


def time_convert(sec):
    mins = sec // 60
    sec = sec % 60
    hours = mins // 60
    mins = mins % 60
    return " {:02d} hrs: {:02d} mins: {:02d} secs.".format(
        int(hours), int(mins), int(sec)
    )


def print_port_details(portfolio):
    print(portfolio["id"])
    print(portfolio["public_access_model"]["value"])
    print(portfolio["public_access_model"]["desc"])
    print("\n")


def clean_port_list(port):
    if port is None:
        return False
    else:
        return True


def add_to_error_log(custom_error_string, status_code, now):

    custom_error_string = (
        f"{custom_error_string} after{time_convert(now)} elapsed"
        if status_code == ""
        else f"{custom_error_string} with error code: {status_code} after{time_convert(now)} elapsed"
    )

    errors.append(custom_error_string)


def port_log_format(port, count, total):
    if "desc" in port["public_access_model"]:
        desc = str(port["public_access_model"]["desc"])
    else:
        desc = " "

    log_lines = (
        str(count)
        + "/"
        + str(total)
        + "\n"
        + "Title: "
        + port["resource_metadata"]["title"]
        + "\n"
        + "MMS ID: "
        + port["resource_metadata"]["mms_id"]["value"]
        + "\n"
        + "Public Access Model: "
        + str(port["public_access_model"]["value"] + "; ")
        + " Description: "
        + desc
        + "\n"
        + "Portfolio ID: "
        + port["id"]
        + ("\n" * 2)
    )

    return log_lines


def load_cache():
    print("Loading cache...")
    global global_cache
    try:
        with open("cache.json", "r") as cache:
            rawcache = cache.read()
    except OSError:
        global_cache = Cache()

    try:
        cache = json.loads(rawcache)
        global_cache = Cache(cache)
    except Exception:
        global_cache = Cache()
    print("Cache loaded.")
    return


def save_cache():
    with codecs.open("cache.json", "w", "utf-8") as cache:
        cache.write(global_cache.json())
    return


def save_error_log():
    timestamp = time.strftime("%Y-%m-%d-%H_%M", time.localtime())

    name = f"{mode}_error_log-{timestamp}.txt"

    if errors == []:
        errors.append("No errors this time")

    with open(name, "w") as e:
        for error in errors:
            e.write(error + "\n")


def save_port_log():
    now = time.monotonic() - START
    timestamp = time.strftime("%Y-%m-%d-%H_%M", time.localtime())

    log = ""
    if mode == "review":
        name = f"review_port_log-{timestamp}.txt"
        num_portfolios_reviewed = len(review_log_data["reviewed_portfolios"])
        list_of_pam_types = list(review_log_data["pam_types"])
        detailed_log = ""
        if review_log_data["api_limit_reached"]:
            api_limit_reached = "\n API limit prevented finishing the review. \n"
        else:
            api_limit_reached = ""

        list_of_pams_log_header = "Following PAMS found in collected portfolios:\n"

        for pam in list_of_pam_types:
            temp_list = []

            detailed_log += f"\n{'-'*20} Portfolios with a PAM that is {'blank' if pam == '' else pam } {'-'*20}\n\n"
            for port in review_log_data["reviewed_portfolios"]:
                if port["public_access_model"]["value"] == pam:
                    temp_list.append(port)

            total = len(temp_list)

            for count, port in enumerate(temp_list, start=1):
                port_log = port_log_format(port, count, total)
                detailed_log += port_log

            list_of_pams_log_header += f"{'blank' if pam == '' else pam }: {total}\n"

        log = (
            (
                f"Number of Portfolios Reviewed: {num_portfolios_reviewed} out of {review_log_data['total_in_collection']} \n"
                f"Portfolio Review time: {time_convert(get_portfolios.time)} \n"
                f"Total time elapsed: {time_convert(now)} \n"
                f"\n {'-'*20} {num_portfolios_reviewed}/{review_log_data['total_in_collection']} Portfolios Reviewed {'-'*20} \n"
            )
            + list_of_pams_log_header
            + api_limit_reached
            + detailed_log
        )

    elif mode == "update":
        port_fetch_time = get_portfolios.time
        port_update_time = time_convert(update_portfolios_api.time - port_fetch_time)
        name = f"update_port_log-{timestamp}.txt"

        num_portfolios_updated = len(update_log_data["updated_portfolios"])
        total_num_portfolios_updated = len(global_cache.get_updated_portfolios())
        not_updating_portfolios = global_cache.get_not_updating_portfolios()
        num_of_unchanged_ports = len(not_updating_portfolios)
        num_update_failed_portfolios = len(update_log_data["update_failed_portfolios"])

        if update_log_data["api_limit_reached"]:
            api_limit_reached = "\n API limit prevented finishing the review. \n"
        else:
            api_limit_reached = ""

        detailed_log = f"\n{'-'*20}Portfolios That Failed to Update{'-'*20}\n\n"

        for num, port in enumerate(
            update_log_data["update_failed_portfolios"], start=1
        ):
            detailed_log += port_log_format(port, num, num_update_failed_portfolios)

        detailed_log += f"\n{'-'*20}Portfolios That Updated This Run{'-'*20}\n\n"

        for num, port in enumerate(update_log_data["updated_portfolios"], start=1):
            detailed_log += port_log_format(port, num, num_portfolios_updated)

        detailed_log += (
            f"\n{'-'*20}Portfolios That Were Set In Alma Already{'-'*20}\n\n"
        )

        for num, port in enumerate(not_updating_portfolios, start=1):
            detailed_log += port_log_format(port, num, num_of_unchanged_ports)

        log = (
            (
                f"Total Portfolios Updated Across All Runs (according to cache): {total_num_portfolios_updated} out of {total_num_portfolios_updated + len(global_cache.get_ready_to_update_portfolios())} \n"
                f"Portfolio Update time: {port_update_time} \n"
                f"Total time elapsed: {time_convert(now)} \n"
                f"{'-'*20} {num_portfolios_updated}/{num_portfolios_updated + num_update_failed_portfolios} Portfolios Updated This Run {'-'*20}\n"
            )
            + api_limit_reached
            + detailed_log
        )
    else:
        return

    with codecs.open(name, "w", "utf-8") as p:
        p.write(log)


# API Request Functions
async def get_port_api(sem, session, ID, counter, total):
    requesturl = (
        BASEURL
        + "/e-collections/"
        + collectionid
        + "/e-services/"
        + serviceid
        + "/portfolios/"
        + ID
        + "?apikey="
        + APIKEY
    )
    try:
        async with sem:
            async with await session.get(requesturl, headers=HEADERS) as response:
                now = time.monotonic() - START
                if response.status == 200:
                    print(
                        str(round((counter / total) * 100))
                        + "% "
                        + str(counter)
                        + "/"
                        + str(total)
                        + time_convert(now)
                    )
                    portfolio = json.loads(await response.text())
                    return portfolio

                else:
                    error_message = "Failed to retrieve porfolio: " + str(ID)
                    add_to_error_log(error_message, str(response.status), now)
                    return
    except (
        aiohttp.ServerDisconnectedError,
        aiohttp.ClientResponseError,
        aiohttp.ClientConnectorError,
    ) as error:
        error_message = f"The server connection was dropped on {requesturl} : {error}"
        add_to_error_log(error_message, "", now)
        print(error_message)


async def get_port_list_api(sem, session, offset, counter, total):
    requesturl = (
        BASEURL
        + "/e-collections/"
        + collectionid
        + "/e-services/"
        + serviceid
        + "/portfolios"
        + "?apikey="
        + APIKEY
        + "&limit=100"
        + "&offset="
        + str(offset)
    )

    try:
        async with sem:
            async with await session.get(requesturl, headers=HEADERS) as response:
                now = time.monotonic() - START
                if response.status == 200:
                    print(
                        f"{round((counter / total) * 100)}% {counter}/{total} {time_convert(now)} |"
                        f" portfolios {offset} to {offset + 100} retrieved from collection list"
                    )
                    partial_response = await response.json()

                    return partial_response["portfolio"]

                else:
                    error_message = "Failed to retrieve partial porfolio list"
                    add_to_error_log(error_message, str(response.status), now)
                    return
    except (
        aiohttp.ServerDisconnectedError,
        aiohttp.ClientResponseError,
        aiohttp.ClientConnectorError,
    ) as error:
        error_message = f"The server connection was dropped on {requesturl} : {error}"
        add_to_error_log(error_message, "", now)
        print(error_message)


async def get_all_collection_portfolio_overview_api(session, number_of_portfolios):
    semaphore = asyncio.Semaphore(30)
    now = time.monotonic() - START
    tasks = []
    counter = 0
    number_of_queries = range(0, (number_of_portfolios % 100) + 1)

    if number_of_portfolios > 100:
        number_of_queries = range(0, math.ceil(number_of_portfolios / 100))

    else:
        number_of_queries = [0]

    if global_cache.get_remaining_api_calls() < len(number_of_queries):
        review_log_data["api_limit_reached"] = True
        update_log_data["api_limit_reached"] = True

        add_to_error_log(
            "couldn't retrieve the overview", "api limit insufficient", now
        )
        return []

    for query in number_of_queries:
        counter += 1

        if query != 0:
            offset = query * 100
        else:
            offset = query

        task = asyncio.ensure_future(
            get_port_list_api(
                semaphore, session, offset, counter, len(number_of_queries)
            )
        )
        tasks.append(task)

    results = await asyncio.gather(*tasks)

    global_cache.add_api_call_set(len(number_of_queries))

    portfolios = []
    for partiallist in results:
        for port in partiallist:
            portfolios.append(port)
    if len(portfolios) == number_of_portfolios:
        return portfolios
    else:
        print("Failed to get all portfolios!")
        return []


async def get_all_portfolio_details_api(session, portfolio_ids):
    semaphore = asyncio.Semaphore(30)
    tasks = []
    counter = 0
    total_portfolios = len(portfolio_ids)
    if global_cache.get_remaining_api_calls() < total_portfolios:
        review_log_data["api_limit_reached"] = True
        update_log_data["api_limit_reached"] = True
        return []

    for id in portfolio_ids:
        counter += 1
        task = asyncio.ensure_future(
            get_port_api(semaphore, session, id, counter, total_portfolios)
        )
        tasks.append(task)
    results = await asyncio.gather(*tasks)

    global_cache.add_api_call_set(total_portfolios)

    results = list(filter(clean_port_list, results))

    return results


async def update_portfolios_api(session, portfolio_list):
    semaphore = asyncio.Semaphore(30)
    tasks = []
    counter = 0
    total_portfolios = len(portfolio_list)
    for port in portfolio_list:
        counter += 1
        if port["id"]:
            task = asyncio.ensure_future(
                update_port(semaphore, session, port, counter, total_portfolios)
            )
            tasks.append(task)
    results = await asyncio.gather(*tasks)
    global_cache.add_api_call_set(total_portfolios)
    update_portfolios_api.time = time.monotonic() - START

    return


async def get_collection_overview_api(session):
    now = time.monotonic() - START
    requesturl = BASEURL + "/e-collections/" + collectionid + "?apikey=" + APIKEY
    if global_cache.get_remaining_api_calls() < 1:
        add_to_error_log(
            "Ran out of API requests, couldn't check the number of portfolios in the collection",
            "API limit exceeded",
            now,
        )
        return

    global_cache.add_api_call_set(1)

    try:
        async with await session.get(requesturl, headers=HEADERS) as response:
            if response.status == 200:
                collection = json.loads(await response.text())
                number_of_portfolios = collection["portfolios"]["value"]
                return number_of_portfolios

            else:
                print(
                    "Something went wrong looking up number of portfolios in the collection! "
                    + "\n"
                    + "Error Code: "
                    + str(response.status)
                    + "\n"
                    + "Program ending early."
                )
                add_to_error_log(
                    "Something went wrong looking up number of portfolios in the collection! ",
                    str(response.status),
                    now,
                )
                return
    except (
        aiohttp.ServerDisconnectedError,
        aiohttp.ClientResponseError,
        aiohttp.ClientConnectorError,
    ) as error:
        error_message = f"The server connection was dropped on {requesturl} : {error}"
        add_to_error_log(error_message, "", now)
        print(error_message)


async def update_port(sem, session, portfolio, counter, total):
    requesturl = (
        BASEURL
        + "/e-collections/"
        + collectionid
        + "/e-services/"
        + serviceid
        + "/portfolios/"
        + portfolio["id"]
        + "?apikey="
        + APIKEY
    )
    try:
        async with sem:
            async with await session.put(
                requesturl, headers=HEADERS, json=portfolio
            ) as response:
                now = time.monotonic() - START
                if response.status == 200:
                    print(
                        str(round((counter / total) * 100))
                        + "% "
                        + str(counter)
                        + "/"
                        + str(total)
                        + time_convert(now)
                    )
                    update_log_data["updated_portfolios"].append(portfolio)
                else:
                    error_message = "Failed to update porfolio: " + str(portfolio["id"])
                    add_to_error_log(error_message, str(response.status), now)
                    update_log_data["update_failed_portfolios"].append(portfolio)
    except (
        aiohttp.ServerDisconnectedError,
        aiohttp.ClientResponseError,
        aiohttp.ClientConnectorError,
    ) as error:
        error_message = f"The server connection was dropped on {requesturl} : {error}"
        add_to_error_log(error_message, "", now)
        print(error_message)


async def get_collection_overview(session):
    # always get the number of portfolios and the last modified date from the api
    print("Getting collection overview...")
    collection_overview = await get_collection_overview_api(session)

    if type(collection_overview) != int or collection_overview == 0:
        print("Error! Problem getting portfolio count.")
        return None

    else:
        number_of_portfolios = collection_overview
        print("Collection overview complete.")
        return number_of_portfolios


async def get_port_ids(session, number_of_portfolios):
    # only get the portfolio overview list from the api if not in cache
    print("Getting portfolio IDs...")
    portfolio_ids = global_cache.get_overview_port_ids()

    if len(portfolio_ids) != number_of_portfolios:
        portfolio_list = await get_all_collection_portfolio_overview_api(
            session, number_of_portfolios
        )

        if portfolio_list == []:
            print("Error retrieving portfolios from API")
        else:
            global_cache.remove_collection_overview()
            global_cache.add_collection_overview(portfolio_list)
            portfolio_ids = global_cache.get_overview_port_ids()
    print("Portfolio IDs have been retrieved.")
    return portfolio_ids


async def get_portfolios(session, number_of_portfolios, portfolio_ids):
    # only get portfolio details from the api if not in cache
    print("Getting portfolios...")

    portfolios = global_cache.get_retrieved_portfolios()

    if len(portfolios) < number_of_portfolios:
        existing_ids = global_cache.get_retrieved_port_ids()
        # filter the already retrieved ids out of the portfolios_ids list
        filtered_ids = list(filter(lambda id: id not in existing_ids, portfolio_ids))

        # limit the number of ids to look up to less than the remaining api limit
        if global_cache.get_remaining_api_calls() >= len(filtered_ids):
            api_limited_ids = filtered_ids
        else:
            review_log_data["api_limit_reached"] = True
            update_log_data["api_limit_reached"] = True
            api_limited_ids = filtered_ids[: global_cache.get_remaining_api_calls()]
            print(
                f"Not enough API requests left, retrieving only {len(api_limited_ids)} portfolios."
            )

        new_portfolios = await get_all_portfolio_details_api(session, api_limited_ids)
        global_cache.add_portfolios_retrieved(new_portfolios)

        global_cache.remove_all_portfolios_updated_by_collection()
        global_cache.remove_all_portfolios_ready_to_update_by_collection()
        global_cache.remove_all_portfolios_not_updating_by_collection()

        portfolios = global_cache.get_retrieved_portfolios()

    get_portfolios.time = time.monotonic() - START
    print("Portfolios retrieved.")
    return portfolios


def all_prepared_portfolios_are_in_cache(number_of_portfolios):
    updated_ports = global_cache.get_updated_portfolios()
    ready_to_update_ports = global_cache.get_ready_to_update_portfolios()
    not_updating_ports = global_cache.get_not_updating_portfolios()
    total = updated_ports + ready_to_update_ports + not_updating_ports
    if len(total) == number_of_portfolios:
        return True
    else:
        return False


def all_retrieved_portfolios_are_in_cache(number_of_portfolios):
    return len(global_cache.get_retrieved_portfolios()) == number_of_portfolios


def prepare_portfolios_for_update(portfolios):
    portfolios_to_update = []
    not_updated_ports = []
    for portfolio in portfolios:

        if portfolio["public_access_model"]["value"] == "":
            # we updated the existing key value pairs rather than creating a new dictionary
            # because "public_access_model" might have other keys that we don't want to overwrite
            portfolio["public_access_model"]["value"] = public_access_model_code
            portfolio["public_access_model"]["desc"] = public_access_model_description
            portfolios_to_update.append(portfolio)

        elif portfolio["public_access_model"] is None:
            # we needed to add a new dictionary since it is currently a None object rather than
            # an existing dictionary with the necessary keys
            portfolio["public_access_model"] = {
                "value": public_access_model_code,
                "desc": public_access_model_description,
            }
            portfolios_to_update.append(portfolio)

        else:
            not_updated_ports.append(portfolio)

    global_cache.add_portfolios_not_updating(not_updated_ports)
    global_cache.add_portfolios_ready_to_update(portfolios_to_update)
    return portfolios_to_update


async def update_mode(session):
    portfolios_to_update = []

    number_of_portfolios = await get_collection_overview(session)
    if number_of_portfolios is None:
        return

    portfolio_ids = await get_port_ids(session, number_of_portfolios)
    if portfolio_ids == []:
        return

    portfolios = await get_portfolios(session, number_of_portfolios, portfolio_ids)
    if portfolios == []:
        return

    # Check if we have the prepared collection in cache or need to prepare it
    if all_prepared_portfolios_are_in_cache(number_of_portfolios):
        portfolios_to_update = global_cache.get_ready_to_update_portfolios()

    elif all_retrieved_portfolios_are_in_cache(number_of_portfolios):
        portfolios_to_update = prepare_portfolios_for_update(portfolios)

    else:
        print("Not all portfolios have been retrieved, can't start updating yet")
        return

    # Make update calls within the number of api calls left; update cache accordingly
    if global_cache.get_remaining_api_calls() >= len(portfolios_to_update):
        await update_portfolios_api(session, portfolios_to_update)

        global_cache.add_portfolios_updated(update_log_data["updated_portfolios"])

        for port in update_log_data["updated_portfolios"]:
            global_cache.remove_portfolio_from_portfolios_ready_to_update(port)

    else:
        remaining_calls = global_cache.get_remaining_api_calls()
        review_log_data["api_limit_reached"] = True
        update_log_data["api_limit_reached"] = True
        ready_to_update_now = portfolios_to_update[0:remaining_calls]

        await update_portfolios_api(session, ready_to_update_now)

        global_cache.add_portfolios_updated(update_log_data["updated_portfolios"])

        for port in update_log_data["updated_portfolios"]:
            global_cache.remove_portfolio_from_portfolios_ready_to_update(port)


async def review_mode(session):
    number_of_portfolios = await get_collection_overview(session)
    if number_of_portfolios is None:
        return

    portfolio_ids = await get_port_ids(session, number_of_portfolios)
    if portfolio_ids == []:
        return

    portfolios = await get_portfolios(session, number_of_portfolios, portfolio_ids)
    if portfolios == []:
        return

    print("Preparing log data...")
    for port in portfolios:
        review_log_data["pam_types"].add(port["public_access_model"]["value"])
        review_log_data["reviewed_portfolios"].append(port)

    review_log_data["total_in_collection"] = number_of_portfolios
    print("Log data complete.")


def checkAPIlimit():
    if global_cache.total_api_calls_past_24_hrs >= MAX_API_CALLS_PER_DAY:
        print("According to the cache record, API calls have hit the configured limit")
        print("Try again tomorrow or increase the set limit if it's safe to do so")
        return True
    else:
        return False


async def main():
    load_cache()

    async with aiohttp.ClientSession() as session:
        session = RateLimiter(session)

        if mode == "update":
            if checkAPIlimit():
                return

            await update_mode(session)
            print("Preparing logs. Please wait ...")
            save_port_log()
            save_error_log()
            print("Logs complete.")

        elif mode == "review":
            if checkAPIlimit():
                return

            await review_mode(session)
            print("Preparing logs. Please wait ...")
            save_port_log()
            save_error_log()
            print("Logs complete.")

        elif mode == "clear_cache_all":
            print("Clearing all cache...")
            global_cache.remove_all_but_api()
            print("Cache cleared.")

        elif mode == "clear_cache_collection":
            print("Clearing all cache for selected collection...")
            global_cache.remove_collection_overview()
            global_cache.remove_all_portfolios_ready_to_update_by_collection()
            global_cache.remove_all_portfolios_retrieved_by_collection()
            global_cache.remove_all_portfolios_updated_by_collection()
            print("Cache cleared.")

        elif mode == "clear_cache_portfolios":
            id_to_remove = input("Please provide portfolio ID to remove:\n")

            # get portfolio from portfolios retrieved
            print("Checking retrieved portfolios...")
            cache_ports = global_cache.get_retrieved_portfolios()
            filtered_ports = list(
                filter(lambda port: id_to_remove in port["id"], cache_ports)
            )
            if len(filtered_ports) < 1:
                print("Portfolio ID not found in Portfolios retrieved.")
            else:
                print("Found ID, removal in process...")
                filtered_ports = filtered_ports[0]

                global_cache.remove_portfolio_from_portfolios_retrieved(filtered_ports)

            # get portfolio from portfolios ready_to_update
            print("Checking portfolios Ready to update...")
            cache_ports = global_cache.get_ready_to_update_portfolios()
            filtered_ports = list(
                filter(lambda port: id_to_remove in port["id"], cache_ports)
            )
            if len(filtered_ports) < 1:
                print("Portfolio ID not found in Portfolios Ready to Update.")
            else:
                print("Found ID, removal in process...")
                filtered_ports = filtered_ports[0]

                global_cache.remove_portfolio_from_portfolios_ready_to_update(
                    filtered_ports
                )

            # get portfolio from portfolios updated
            print("Checking updated portfolios...")
            cache_ports = global_cache.get_updated_portfolios()
            filtered_ports = list(
                filter(lambda port: id_to_remove in port["id"], cache_ports)
            )
            if len(filtered_ports) < 1:
                print("Portfolio ID not found in Updated Portfolios.")
            else:
                print("Found ID, removal in process...")
                filtered_ports = filtered_ports[0]

                global_cache.remove_portfolio_from_portfolios_updated(filtered_ports)

            # get portfolio from portfolios updated
            print("Checking not updated portfolios...")
            cache_ports = global_cache.get_not_updating_portfolios()
            filtered_ports = list(
                filter(lambda port: id_to_remove in port["id"], cache_ports)
            )
            if len(filtered_ports) < 1:
                print("Portfolio ID not found in Portfolios Not Updated.")
            else:
                print("Found ID, removal in process...")
                filtered_ports = filtered_ports[0]

                global_cache.remove_portfolio_from_portfolios_not_updating(
                    filtered_ports
                )

            print("Selected portfolios removed from cache.")

        else:
            print("error: mode variable value not recognized")

    print("Saving cache...")
    save_cache()
    print("Cache saved.")


with keepawake(keep_screen_awake=False):
    asyncio.run(main())

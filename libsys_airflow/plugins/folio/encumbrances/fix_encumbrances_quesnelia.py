# type: ignore

import asyncio
import getpass
import json
import sys
import time
from datetime import datetime
from decimal import Decimal
from http import HTTPStatus

import httpx
import requests

import logging
from airflow.models import Variable

logger = logging.getLogger(__name__)

ITEM_MAX = 2147483647
MAX_BY_CHUNK = 1000

okapi_url = Variable.get("OKAPI_URL")
headers = {}
client = httpx.AsyncClient()
dryrun = False

# request timeout in seconds
ASYNC_CLIENT_TIMEOUT = 30

# limit the number of parallel threads.
# Try different values. Bigger values - for increasing performance, but could produce "Connection timeout exception"
MAX_ACTIVE_THREADS = 7


# ---------------------------------------------------
# Utility functions


def raise_exception_for_reply(r):
    raise Exception(f'Status code: {r.status_code}. Response: "{r.text}"')


def login(tenant, username, password):
    login_headers = {'x-okapi-tenant': tenant, 'Content-Type': 'application/json'}
    data = {'username': username, 'password': password}
    try:
        r = requests.post(okapi_url + 'authn/login', headers=login_headers, json=data)
        if r.status_code != 201:
            raise_exception_for_reply(r)
        logger.info('Logged in successfully.')
        okapi_token = r.json()['okapiToken']
        return {
            'x-okapi-tenant': tenant,
            'x-okapi-token': okapi_token,
            'Content-Type': 'application/json',
        }
    except Exception as err:
        print('Error during login:', err)
        logger.error('Error during login:', err)
        raise Exception("Exiting Fix Encumbrances script.")


async def get_request_without_query(url: str) -> dict:
    try:
        resp = await client.get(url, headers=headers, timeout=ASYNC_CLIENT_TIMEOUT)

        if resp.status_code == HTTPStatus.OK:
            return resp.json()
        else:
            print(f'Error getting record with url {url} : \n{resp.text} ')
            logger.error(f'Error getting record with url {url} : \n{resp.text} ')
            raise Exception("Exiting Fix Encumbrances script.")
    except Exception as err:
        print(f'Error getting record with url {url} : {err=}')
        logger.error(f'Error getting record with url {url} : {err=}')
        raise Exception("Exiting Fix Encumbrances script.")


async def get_request(url: str, query: str) -> dict:
    params = {'query': query, 'offset': '0', 'limit': ITEM_MAX}

    try:
        resp = await client.get(
            url, headers=headers, params=params, timeout=ASYNC_CLIENT_TIMEOUT
        )

        if resp.status_code == HTTPStatus.OK:
            return resp.json()
        else:
            print(f'Error getting records by {url} ?query= "{query}": \n{resp.text} ')
            logger.error(f'Error getting records by {url} ?query= "{query}": \n{resp.text} ')
            raise Exception("Exiting Fix Encumbrances script.")
    except Exception as err:
        print(f'Error getting records by {url}?query={query}: {err=}')
        logger.error(f'Error getting records by {url}?query={query}: {err=}')
        raise Exception("Exiting Fix Encumbrances script.")


async def post_request(url: str, data):
    if dryrun:
        return
    try:
        resp = await client.post(
            url, headers=headers, data=json.dumps(data), timeout=ASYNC_CLIENT_TIMEOUT
        )
        if (
            resp.status_code == HTTPStatus.CREATED
            or resp.status_code == HTTPStatus.NO_CONTENT
        ):
            return
        print(f'Error in POST request {url} "{data}": {resp.text}')
        logger.error(f'Error in POST request {url} "{data}": {resp.text}')
        raise Exception("Exiting Fix Encumbrances script.")

    except Exception as err:
        print(f'Error in POST request {url} "{data}": {err=}')
        logger.error(f'Error in POST request {url} "{data}": {err=}')
        raise Exception("Exiting Fix Encumbrances script.")


async def put_request(url: str, data):
    if dryrun:
        return
    try:
        resp = await client.put(
            url, headers=headers, data=json.dumps(data), timeout=ASYNC_CLIENT_TIMEOUT
        )
        if resp.status_code == HTTPStatus.NO_CONTENT:
            return
        print(f'Error updating record {url} "{data}": {resp.text}')
        logger.error(f'Error updating record {url} "{data}": {resp.text}')
        raise Exception("Exiting Fix Encumbrances script.")

    except Exception as err:
        print(f'Error updating record {url} "{data}": {err=}')
        logger.error(f'Error updating record {url} "{data}": {err=}')
        raise Exception("Exiting Fix Encumbrances script.")


def get_fiscal_years_by_query(query) -> dict:
    params = {'query': query, 'offset': '0', 'limit': ITEM_MAX}
    try:
        r = requests.get(
            okapi_url + 'finance-storage/fiscal-years', headers=headers, params=params
        )
        if r.status_code != 200:
            raise_exception_for_reply(r)
        return r.json()['fiscalYears']
    except Exception as err:
        print(f'Error getting fiscal years with query "{query}": {err}')
        logger.error(f'Error getting fiscal years with query "{query}": {err}')
        raise Exception("Exiting Fix Encumbrances script.")


def get_by_chunks(url, query, key) -> list:
    # See https://github.com/folio-org/raml-module-builder#implement-chunked-bulk-download
    records = []
    last_id = None
    while True:
        if last_id is None:
            modified_query = query + ' AND cql.allRecords=1 sortBy id'
        else:
            modified_query = query + f' AND id > {last_id} sortBy id'
        params = {'query': modified_query, 'offset': 0, 'limit': MAX_BY_CHUNK}
        r = requests.get(url, headers=headers, params=params)
        if r.status_code != 200:
            raise_exception_for_reply(r)
        j = r.json()
        if key not in j.keys():
            raise Exception(
                f'Could not find key when retrieving by chunks; url={url}, key={key}'
            )
        records_in_chunk = j[key]
        if len(records_in_chunk) == 0:
            break
        records.extend(records_in_chunk)
        last_id = records_in_chunk[-1]['id']
    return records


def get_order_ids_by_query(query) -> list:
    try:
        orders = get_by_chunks(
            okapi_url + 'orders-storage/purchase-orders', query, 'purchaseOrders'
        )
        ids = []
        for order in orders:
            ids.append(order['id'])
    except Exception as err:
        print(f'Error getting order ids with query "{query}": {err}')
        logger.error(f'Error getting order ids with query "{query}": {err}')
        raise Exception("Exiting Fix Encumbrances script.")
    return ids


async def get_encumbrances_by_query(query) -> list:
    url = okapi_url + 'finance-storage/transactions'
    response = await get_request(url, query)
    return response['transactions']


async def get_encumbrance_by_ids(encumbrance_ids) -> list:
    query = ''
    for idx, enc_id in enumerate(encumbrance_ids):
        if len(encumbrance_ids) != idx + 1:
            query = query + f"id=={enc_id} OR "
        else:
            query = query + f"id=={enc_id}"
    resp = await get_request(okapi_url + 'finance-storage/transactions', query)

    return resp['transactions']


def get_fiscal_year(fiscal_year_code) -> dict:
    query = f'code=="{fiscal_year_code}"'
    fiscal_years = get_fiscal_years_by_query(query)
    if len(fiscal_years) == 0:
        print(f'Could not find fiscal year "{fiscal_year_code}".')
        logger.error(f'Could not find fiscal year "{fiscal_year_code}".')
        raise Exception("Exiting Fix Encumbrances script.")
    return fiscal_years[0]


def test_fiscal_year_current(fiscal_year) -> bool:
    start = datetime.fromisoformat(fiscal_year['periodStart'])
    end = datetime.fromisoformat(fiscal_year['periodEnd'])
    now = datetime.now().astimezone()
    return start < now < end


def get_closed_orders_ids() -> list:
    print('Retrieving closed order ids...')
    query = 'workflowStatus=="Closed"'
    closed_orders_ids = get_order_ids_by_query(query)
    print('  Closed orders:', len(closed_orders_ids))
    return closed_orders_ids


def get_open_orders_ids() -> list:
    print('Retrieving open order ids...')
    query = 'workflowStatus=="Open"'
    open_orders_ids = get_order_ids_by_query(query)
    print('  Open orders:', len(open_orders_ids))
    return open_orders_ids


async def batch_update(transactions: list):
    batch = {'transactionsToUpdate': transactions}
    url = f'{okapi_url}finance-storage/transactions/batch-all-or-nothing'
    await post_request(url, batch)


async def batch_delete(transaction_ids: list):
    batch = {'idsOfTransactionsToDelete': transaction_ids}
    url = f'{okapi_url}finance-storage/transactions/batch-all-or-nothing'
    await post_request(url, batch)


async def get_budgets_by_query(query) -> list:
    budget_collection = await get_request(okapi_url + 'finance-storage/budgets', query)
    return budget_collection['budgets']


async def get_budget_by_fund_id(fund_id, fiscal_year_id) -> dict:
    query = f'fundId=={fund_id} AND fiscalYearId=={fiscal_year_id}'
    budgets = await get_budgets_by_query(query)
    if len(budgets) == 0:
        print(
            f'Could not find budget for fund "{fund_id}" and fiscal year "{fiscal_year_id}".'
        )
        logger.error(
            f'Could not find budget for fund "{fund_id}" and fiscal year "{fiscal_year_id}".'
        )
        raise Exception("Exiting Fix Encumbrances script.")
    return budgets[0]


async def get_budgets_by_fiscal_year(fiscal_year_id) -> list:
    query = f'fiscalYearId=={fiscal_year_id}'
    return await get_budgets_by_query(query)


async def get_order_encumbrances(order_id, fiscal_year_id, sem=None) -> list:
    url = okapi_url + 'finance-storage/transactions'
    query = f'encumbrance.sourcePurchaseOrderId=={order_id} AND fiscalYearId=={fiscal_year_id}'
    response = await get_request(url, query)
    if sem is not None:
        sem.release()
    return response['transactions']


def progress(index, total_elements, label=''):
    if total_elements == 0:
        return
    progress_length = 80
    current_progress_length = int(
        round(progress_length * index / float(total_elements))
    )

    percents_completed = round(100.0 * index / float(total_elements), 1)
    bar = '=' * current_progress_length + '-' * (
        progress_length - current_progress_length
    )

    sys.stdout.write('%s - [%s] %s%s \r' % (label, bar, percents_completed, '%'))
    sys.stdout.flush()

    if index == total_elements:
        print()


# ---------------------------------------------------
# Remove duplicate encumbrances


def find_encumbrances_to_remove(encumbrances) -> list:
    encumbrance_changes = []
    unreleased_encumbrances = []
    released_encumbrances = []
    for enc in encumbrances:
        if enc['encumbrance']['status'] == 'Unreleased':
            unreleased_encumbrances.append(enc)
        if enc['encumbrance']['status'] == 'Released':
            released_encumbrances.append(enc)
    for enc1 in unreleased_encumbrances:
        from_fund_id = enc1['fromFundId']
        source_po_line_id = enc1['encumbrance']['sourcePoLineId']
        if 'expenseClassId' in enc1:
            expense_class_id = enc1['expenseClassId']
        else:
            expense_class_id = None
        fiscal_year_id = enc1['fiscalYearId']
        for enc2 in released_encumbrances:
            ec_test = (expense_class_id is None and 'expenseClassId' not in enc2) or (
                expense_class_id is not None
                and 'expenseClassId' in enc2
                and enc2['expenseClassId'] == expense_class_id
            )
            if (
                enc2['fromFundId'] == from_fund_id
                and enc2['encumbrance']['sourcePoLineId'] == source_po_line_id
                and ec_test
                and enc2['fiscalYearId'] == fiscal_year_id
            ):
                encumbrance_changes.append({'remove': enc1, 'replace_by': enc2})
                break
    return encumbrance_changes


async def update_poline_encumbrance(encumbrance_to_remove, replace_by, poline=None):
    url = (
        okapi_url
        + f"orders-storage/po-lines/{encumbrance_to_remove['encumbrance']['sourcePoLineId']}"
    )
    if poline is None:
        poline = await get_request_without_query(url)
    for fd in poline['fundDistribution']:
        if 'encumbrance' in fd and fd['encumbrance'] == encumbrance_to_remove['id']:
            fd['encumbrance'] = replace_by['id']
            await put_request(url, poline)
            break


async def remove_encumbrances_and_update_polines(encumbrance_changes):
    futures = []
    ids_to_delete = []
    for change in encumbrance_changes:
        encumbrance_to_remove = change['remove']
        replace_by = change['replace_by']
        futures.append(
            asyncio.ensure_future(
                update_poline_encumbrance(encumbrance_to_remove, replace_by)
            )
        )
        ids_to_delete.append(encumbrance_to_remove['id'])
    await asyncio.gather(*futures)
    await batch_delete(ids_to_delete)


async def remove_duplicate_encumbrances_in_order(order_id, fiscal_year_id, sem) -> int:
    order_encumbrances = await get_order_encumbrances(order_id, fiscal_year_id)
    if len(order_encumbrances) == 0:
        sem.release()
        return 0
    encumbrance_changes = find_encumbrances_to_remove(order_encumbrances)
    if len(encumbrance_changes) == 0:
        sem.release()
        return 0
    print(f"  Removing the following encumbrances for order {order_id}:")
    for change in encumbrance_changes:
        print(f"    {change['remove']['id']}")
    await remove_encumbrances_and_update_polines(encumbrance_changes)
    sem.release()
    return len(encumbrance_changes)


async def remove_duplicate_encumbrances(open_and_closed_orders_ids, fiscal_year_id):
    print('Removing duplicate encumbrances...')
    futures = []
    sem = asyncio.Semaphore(MAX_ACTIVE_THREADS)
    for idx, order_id in enumerate(open_and_closed_orders_ids):
        await sem.acquire()
        progress(idx, len(open_and_closed_orders_ids))
        futures.append(
            asyncio.ensure_future(
                remove_duplicate_encumbrances_in_order(order_id, fiscal_year_id, sem)
            )
        )

    nb_removed_encumbrances = await asyncio.gather(*futures)
    progress(len(open_and_closed_orders_ids), len(open_and_closed_orders_ids))
    print(f'  Removed {sum(nb_removed_encumbrances)} encumbrance(s).')


# ---------------------------------------------------
# Fix poline-encumbrance relations


async def get_polines_by_order_id(order_id) -> list:
    query = f'purchaseOrderId=={order_id}'
    resp = await get_request(okapi_url + 'orders-storage/po-lines', query)
    po_lines = resp['poLines']
    return po_lines


async def update_encumbrance_fund_id(encumbrance, new_fund_id, poline):
    encumbrance['fromFundId'] = new_fund_id
    encumbrance_id = encumbrance['id']
    print(
        f"  Fixing fromFundId for po line {poline['id']} ({poline['poLineNumber']}) encumbrance {encumbrance_id}"
    )
    await batch_update([encumbrance])


# Remove a duplicate encumbrance if it has a wrong fromFundId, and update the poline fd if needed
async def fix_fund_id_with_duplicate_encumbrances(encumbrances, fd_fund_id, poline):
    encumbrances_with_right_fund = []
    encumbrances_with_bad_fund = []
    for encumbrance in encumbrances:
        if encumbrance['fromFundId'] == fd_fund_id:
            encumbrances_with_right_fund.append(encumbrance)
        else:
            encumbrances_with_bad_fund.append(encumbrance)
    if len(encumbrances_with_bad_fund) == 0:
        print(
            f"  Warning: there is a remaining duplicate encumbrance for poline {poline['id']} "
            f"({poline['poLineNumber']})."
        )
        return
    if len(encumbrances_with_right_fund) != 1:
        print(
            f"  Problem fixing encumbrances for poline {poline['id']} ({poline['poLineNumber']}), "
            "please fix by hand."
        )
        return
    replace_by = encumbrances_with_right_fund[0]
    ids_to_delete = []
    for encumbrance_to_remove in encumbrances_with_bad_fund:
        print(
            f"  Removing encumbrance {encumbrance_to_remove['id']} for po line {poline['id']} "
            f"({poline['poLineNumber']})"
        )
        await update_poline_encumbrance(encumbrance_to_remove, replace_by, poline)
        ids_to_delete.append(encumbrance_to_remove['id'])
    await batch_delete(ids_to_delete)


# Fix encumbrance fromFundId if it doesn't match the po line fund distribution (see MODFISTO-384, MODFISTO-385)
async def fix_poline_encumbrance_fund_id(poline, order_encumbrances):
    fds = poline['fundDistribution']
    # we can't fix the fundId if there is more than 1 fund distribution in the po line
    if len(fds) != 1:
        return

    fd_fund_id = fds[0]['fundId']
    encumbrances = []
    for enc in order_encumbrances:
        if enc['encumbrance']['sourcePoLineId'] == poline['id']:
            encumbrances.append(enc)
    if len(encumbrances) == 0:
        return
    if len(encumbrances) == 1:
        if encumbrances[0]['fromFundId'] == fd_fund_id:
            return
        await update_encumbrance_fund_id(encumbrances[0], fd_fund_id, poline)
        return
    await fix_fund_id_with_duplicate_encumbrances(encumbrances, fd_fund_id, poline)


def check_if_fd_needs_updates_and_update_fd(poline, order_encumbrances, fd) -> bool:
    poline_id = poline['id']
    fd_fund_id = fd['fundId']
    for enc in order_encumbrances:
        # if (
        #     enc['encumbrance']['sourcePoLineId'] == poline_id
        #     and float(enc['amount']) != 0.0
        #     and enc['fromFundId'] == fd_fund_id
        # ):
        if (
            enc['encumbrance']['sourcePoLineId'] == poline_id
            and enc['fromFundId'] == fd_fund_id
        ):
            fd_encumbrance_id = fd['encumbrance']
            if enc['id'] == fd_encumbrance_id:
                return False
            # print(
            #     f"  Updating poline {poline_id} ({poline['poLineNumber']}) encumbrance {fd_encumbrance_id} "
            #     f"with new value {enc['id']}"
            # )
            fd['encumbrance'] = enc['id']
            return True
    return False


# for each fund distribution check encumbrance relationship and modify if needed -
#   in case if encumbrance id specified in fund distribution:
#   get encumbrance by poline id and current FY<>transaction.FY and amount <> 0
#   if fd.encumbrance != transaction.id --> set new encumbrance reference
#   update poline if modified
# (feature added with MODFISTO-350)
async def fix_poline_encumbrance_link(poline, order_encumbrances):
    poline_needs_updates = False
    for fd in poline['fundDistribution']:
        if 'encumbrance' in fd:
            if check_if_fd_needs_updates_and_update_fd(poline, order_encumbrances, fd):
                poline_needs_updates = True

    # update poline if one or more fund distributions modified
    if poline_needs_updates:
        url = f"{okapi_url}orders-storage/po-lines/{poline['id']}"
        await put_request(url, poline)


async def process_poline_encumbrances_relations(poline, order_encumbrances):
    await fix_poline_encumbrance_fund_id(poline, order_encumbrances)
    await fix_poline_encumbrance_link(poline, order_encumbrances)


# Get encumbrances for the fiscal year and call process_po_line_encumbrances_relations() for each po line
async def process_order_encumbrances_relations(order_id, fiscal_year_id, order_sem):
    po_lines = await get_polines_by_order_id(order_id)
    if len(po_lines) == 0:
        order_sem.release()
        return
    order_encumbrances = await get_order_encumbrances(order_id, fiscal_year_id)
    if len(order_encumbrances) == 0:
        order_sem.release()
        return

    for po_line in po_lines:
        await process_poline_encumbrances_relations(po_line, order_encumbrances)

    order_sem.release()


# Call process_order_encumbrances_relations() for each order
async def fix_poline_encumbrances_relations(
    open_orders_ids, fiscal_year_id, fy_is_current
):
    print('Fixing poline-encumbrance links...')
    if len(open_orders_ids) == 0:
        print('  Found no open orders.')
        return
    if not fy_is_current:
        print(
            '  Fiscal year is not current, the step to fix po line encumbrance relations will be skipped.'
        )
        return
    orders_futures = []
    order_sem = asyncio.Semaphore(MAX_ACTIVE_THREADS)
    for idx, order_id in enumerate(open_orders_ids):
        await order_sem.acquire()
        progress(idx, len(open_orders_ids))
        orders_futures.append(
            asyncio.ensure_future(
                process_order_encumbrances_relations(
                    order_id, fiscal_year_id, order_sem
                )
            )
        )
    await asyncio.gather(*orders_futures)
    progress(len(open_orders_ids), len(open_orders_ids))


# ---------------------------------------------------
# Fix encumbrance orderStatus for closed orders


async def get_order_encumbrances_to_fix(order_id, fiscal_year_id) -> dict:
    query = (
        f'encumbrance.orderStatus<>"Closed" AND encumbrance.sourcePurchaseOrderId=={order_id} AND '
        f'fiscalYearId=={fiscal_year_id}'
    )
    url = okapi_url + 'finance-storage/transactions'

    return await get_request(url, query)


async def unrelease_order_encumbrances(encumbrances) -> list:
    for encumbrance in encumbrances:
        encumbrance['encumbrance']['status'] = 'Unreleased'
    await batch_update(encumbrances)

    # the encumbrance amounts get modified (and the version in MG), so we need to get a fresh version
    modified_encumbrance_futures = []

    # split retrieving encumbrances by small id lists
    # reasons:
    # - retrieving one by one will slow down performance
    # - retrieving all at once will generate too long query and fail the request due to RMB restrictions
    enc_ids = build_ids_2d_array(encumbrances)
    for enc_ids_row in enc_ids:
        modified_encumbrance_futures.append(get_encumbrance_by_ids(enc_ids_row))
    modified_encumbrances = await asyncio.gather(*modified_encumbrance_futures)

    flatten_list_of_modified_encumbrances = sum(modified_encumbrances, [])
    return flatten_list_of_modified_encumbrances


def build_ids_2d_array(entities) -> list:
    # prepare two-dimensional array of ids for requesting the records by ids in bulks
    ids_2d_array = []
    index = 0
    for row in range(ITEM_MAX):
        inner_list = []
        for col in range(20):
            if index == len(entities):
                break
            inner_list.append(entities[index]['id'])
            index = index + 1
        ids_2d_array.append(inner_list)
        if index == len(entities):
            break
    return ids_2d_array


async def fix_order_status_and_release_encumbrances(order_id, encumbrances):
    try:
        for encumbrance in encumbrances:
            encumbrance['encumbrance']['status'] = 'Released'
            encumbrance['encumbrance']['orderStatus'] = 'Closed'
        await batch_update(encumbrances)

    except Exception as err:
        print(
            f'Error when fixing order status in encumbrances for order {order_id}:', err
        )
        logger.error(
            f'Error when fixing order status in encumbrances for order {order_id}:', err
        )
        raise Exception("Exiting Fix Encumbrances script.")


async def fix_order_encumbrances_order_status(order_id, encumbrances):
    # We can't just PUT the encumbrance with a modified orderStatus, because
    # mod-finance-storage's EncumbranceService ignores changes to released encumbrances
    # unless it's to unrelease them. So we have to unrelease the encumbrances first.
    # Eventually we could rely on the post-MODFISTO-328 implementation to change orderStatus directly
    # (Morning Glory bug fix).
    try:
        # print(f'\n  Fixing the following encumbrance(s) for order {order_id} :')
        for encumbrance in encumbrances:
            print(f"    {encumbrance['id']}")
        modified_encumbrances = await unrelease_order_encumbrances(encumbrances)
        if len(modified_encumbrances) != 0:
            await fix_order_status_and_release_encumbrances(
                order_id, modified_encumbrances
            )
    except Exception as err:
        print(
            f'Error when fixing order status in encumbrances for order {order_id}:', err
        )
        logger.error(
            f'Error when fixing order status in encumbrances for order {order_id}:', err
        )
        raise Exception("Exiting Fix Encumbrances script.")


async def fix_encumbrance_order_status_for_closed_order(
    order_id, fiscal_year_id, sem
) -> int:
    encumbrances = await get_order_encumbrances_to_fix(order_id, fiscal_year_id)
    if len(encumbrances['transactions']) != 0:
        await fix_order_encumbrances_order_status(
            order_id, encumbrances['transactions']
        )
    sem.release()
    return len(encumbrances['transactions'])


async def fix_encumbrance_order_status_for_closed_orders(
    closed_orders_ids, fiscal_year_id
):
    print('Fixing encumbrance order status for closed orders...')
    if len(closed_orders_ids) == 0:
        print('  Found no closed orders.')
        return
    fix_encumbrance_futures = []
    max_active_order_threads = 5
    sem = asyncio.Semaphore(max_active_order_threads)
    for idx, order_id in enumerate(closed_orders_ids):
        await sem.acquire()
        # progress(idx, len(closed_orders_ids))
        fixed_encumbrance_future = asyncio.ensure_future(
            fix_encumbrance_order_status_for_closed_order(order_id, fiscal_year_id, sem)
        )
        fix_encumbrance_futures.append(fixed_encumbrance_future)
    nb_fixed_encumbrances = await asyncio.gather(*fix_encumbrance_futures)
    # progress(len(closed_orders_ids), len(closed_orders_ids))

    print(f'  Fixed order status for {sum(nb_fixed_encumbrances)} encumbrance(s).')


# ---------------------------------------------------
# Unrelease open orders encumbrances with non-zero amounts


async def unrelease_encumbrances(order_id, encumbrances):
    # print(f'\n  Unreleasing the following encumbrance(s) for order {order_id} :')
    for encumbrance in encumbrances:
        print(f"    {encumbrance['id']}")
        encumbrance['encumbrance']['status'] = 'Unreleased'
    await batch_update(encumbrances)


async def unrelease_encumbrances_with_non_zero_amounts(
    order_id, fiscal_year_id, sem
) -> int:
    query = (
        f'amount<>0.0 AND encumbrance.status=="Released" AND encumbrance.sourcePurchaseOrderId=={order_id} AND '
        f'fiscalYearId=={fiscal_year_id}'
    )
    transactions_response = await get_request(
        okapi_url + 'finance-storage/transactions', query
    )

    order_encumbrances = transactions_response['transactions']
    # unrelease encumbrances by order id
    if len(order_encumbrances) != 0:
        await unrelease_encumbrances(order_id, order_encumbrances)

    sem.release()
    return len(order_encumbrances)


async def unrelease_open_orders_encumbrances_with_nonzero_amounts(
    fiscal_year_id, open_orders_ids
):
    print('Unreleasing open orders encumbrances with non-zero amounts...')
    if len(open_orders_ids) == 0:
        print('  Found no open orders.')
        return
    enc_futures = []
    sem = asyncio.Semaphore(MAX_ACTIVE_THREADS)
    for idx, order_id in enumerate(open_orders_ids):
        await sem.acquire()
        # progress(idx, len(open_orders_ids))
        enc_futures.append(
            asyncio.ensure_future(
                unrelease_encumbrances_with_non_zero_amounts(
                    order_id, fiscal_year_id, sem
                )
            )
        )
    unreleased_encumbrances_amounts = await asyncio.gather(*enc_futures)
    # progress(len(open_orders_ids), len(open_orders_ids))

    print(
        f'  Unreleased {sum(unreleased_encumbrances_amounts)} open order encumbrance(s) with non-zero amounts.'
    )


# ---------------------------------------------------
# Release open orders encumbrances with negative amounts (see MODFISTO-368)


async def release_encumbrances(order_id, encumbrances):
    # print(f'\n  Releasing the following encumbrances for order {order_id} :')
    for encumbrance in encumbrances:
        print(f"    {encumbrance['id']}")
        encumbrance['encumbrance']['status'] = 'Released'
    await batch_update(encumbrances)


async def release_encumbrances_with_negative_amounts(
    order_id, fiscal_year_id, sem
) -> int:
    query = (
        'amount </number 0 AND encumbrance.status=="Unreleased" AND '
        f'(encumbrance.amountAwaitingPayment >/number 0 OR encumbrance.amountExpended >/number 0) AND '
        f'encumbrance.sourcePurchaseOrderId=={order_id} AND fiscalYearId=={fiscal_year_id}'
    )
    transactions_response = await get_request(
        okapi_url + 'finance-storage/transactions', query
    )

    order_encumbrances = transactions_response['transactions']
    # release encumbrances by order id
    if len(order_encumbrances) != 0:
        await release_encumbrances(order_id, order_encumbrances)

    sem.release()
    return len(order_encumbrances)


async def release_open_orders_encumbrances_with_negative_amounts(
    fiscal_year_id, open_orders_ids
):
    print('Releasing open orders encumbrances with negative amounts...')
    if len(open_orders_ids) == 0:
        print('  Found no open orders.')
        return
    enc_futures = []
    sem = asyncio.Semaphore(MAX_ACTIVE_THREADS)
    for idx, order_id in enumerate(open_orders_ids):
        await sem.acquire()
        # progress(idx, len(open_orders_ids))
        enc_futures.append(
            asyncio.ensure_future(
                release_encumbrances_with_negative_amounts(
                    order_id, fiscal_year_id, sem
                )
            )
        )
    released_encumbrances_amounts = await asyncio.gather(*enc_futures)
    # progress(len(open_orders_ids), len(open_orders_ids))

    print(
        f'  Released {sum(released_encumbrances_amounts)} open order encumbrance(s) with negative amounts.'
    )


# ---------------------------------------------------
# Release cancelled order line encumbrances (see MODFISTO-383)


def find_encumbrances_to_release(po_lines, order_encumbrances) -> list:
    encumbrances_to_release = []
    for pol in po_lines:
        if pol['paymentStatus'] == 'Cancelled':
            for enc in order_encumbrances:
                if (
                    enc['encumbrance']['sourcePoLineId'] == pol['id']
                    and enc['encumbrance']['status'] == 'Unreleased'
                ):
                    encumbrances_to_release.append(enc)
    return encumbrances_to_release


async def release_cancelled_pol_encumbrances(order_id, fiscal_year_id, sem) -> int:
    po_lines = await get_polines_by_order_id(order_id)
    if len(po_lines) == 0:
        sem.release()
        return 0
    order_encumbrances = await get_order_encumbrances(order_id, fiscal_year_id)
    if len(order_encumbrances) == 0:
        sem.release()
        return 0
    encumbrances_to_release = find_encumbrances_to_release(po_lines, order_encumbrances)
    if len(encumbrances_to_release) == 0:
        sem.release()
        return 0
    await release_encumbrances(order_id, encumbrances_to_release)
    sem.release()
    return len(encumbrances_to_release)


async def release_cancelled_order_line_encumbrances(fiscal_year_id, open_orders_ids):
    print('Releasing cancelled order line encumbrances...')
    if len(open_orders_ids) == 0:
        print('  Found no open orders.')
        return
    enc_futures = []
    sem = asyncio.Semaphore(MAX_ACTIVE_THREADS)
    for idx, order_id in enumerate(open_orders_ids):
        await sem.acquire()
        # progress(idx, len(open_orders_ids))
        enc_futures.append(
            asyncio.ensure_future(
                release_cancelled_pol_encumbrances(order_id, fiscal_year_id, sem)
            )
        )
    released_encumbrances_amounts = await asyncio.gather(*enc_futures)
    # progress(len(open_orders_ids), len(open_orders_ids))

    print(
        f'  Released {sum(released_encumbrances_amounts)} cancelled order line encumbrance(s).'
    )


# ---------------------------------------------------
# Recalculate budget encumbered


async def update_budgets(encumbered, fund_id, fiscal_year_id, sem) -> int:
    nb_modified = 0
    budget = await get_budget_by_fund_id(fund_id, fiscal_year_id)

    # Cast into decimal values, so 0 == 0.0 == 0.00 will return true
    if Decimal(str(budget['encumbered'])) != Decimal(encumbered):
        # print(
        #     f"    Budget \"{budget['name']}\": changing encumbered from {budget['encumbered']} to {encumbered}"
        # )
        budget['encumbered'] = encumbered

        url = f"{okapi_url}finance-storage/budgets/{budget['id']}"
        await put_request(url, budget)
        nb_modified = 1
    sem.release()
    return nb_modified


async def recalculate_budget_encumbered(open_and_closed_orders_ids, fiscal_year_id):
    # Recalculate the encumbered property for all the budgets related to these encumbrances
    # Take closed orders into account because we might have to set a budget encumbered to 0
    print(
        f'Recalculating budget encumbered for {len(open_and_closed_orders_ids)} orders ...'
    )
    enc_future = []
    sem = asyncio.Semaphore(MAX_ACTIVE_THREADS)
    for idx, order_id in enumerate(open_and_closed_orders_ids):
        await sem.acquire()
        # progress(idx, len(open_and_closed_orders_ids))
        enc_future.append(
            asyncio.ensure_future(get_order_encumbrances(order_id, fiscal_year_id, sem))
        )

    encumbrances = sum(await asyncio.gather(*enc_future), [])
    # progress(len(open_and_closed_orders_ids), len(open_and_closed_orders_ids))

    encumbered_for_fund = {}
    budgets = await get_budgets_by_fiscal_year(fiscal_year_id)
    for budget in budgets:
        fund_id = budget['fundId']
        if fund_id not in encumbered_for_fund:
            encumbered_for_fund[fund_id] = 0

    for encumbrance in encumbrances:
        fund_id = encumbrance['fromFundId']
        if fund_id in encumbered_for_fund:
            encumbered_for_fund[fund_id] += Decimal(str(encumbrance['amount']))

    print('  Updating budgets...')

    update_budget_futures = []
    for fund_id, encumbered in encumbered_for_fund.items():
        await sem.acquire()
        update_budget_futures.append(
            asyncio.ensure_future(
                update_budgets(str(encumbered), fund_id, fiscal_year_id, sem)
            )
        )
    nb_modified = sum(await asyncio.gather(*update_budget_futures))

    print(f'  Edited {nb_modified} budget(s).')
    print('  Done recalculating budget encumbered.')


# ---------------------------------------------------
# Release unreleased encumbrances for closed orders


async def get_order_encumbrances_to_release(order_id, fiscal_year_id) -> list:
    query = (
        f'encumbrance.status=="Unreleased" AND encumbrance.sourcePurchaseOrderId=={order_id} AND '
        f'fiscalYearId=={fiscal_year_id}'
    )
    return await get_encumbrances_by_query(query)


async def release_order_encumbrances(order_id, fiscal_year_id, sem) -> int:
    encumbrances = await get_order_encumbrances_to_release(order_id, fiscal_year_id)
    if len(encumbrances) != 0:
        await release_encumbrances(order_id, encumbrances)
    sem.release()
    return len(encumbrances)


async def release_unreleased_encumbrances_for_closed_orders(
    closed_orders_ids, fiscal_year_id
):
    print('Releasing unreleased encumbrances for closed orders...')
    if len(closed_orders_ids) == 0:
        print('  Found no closed orders.')
        return
    nb_released_encumbrance_futures = []
    sem = asyncio.Semaphore(MAX_ACTIVE_THREADS)

    for idx, order_id in enumerate(closed_orders_ids):
        await sem.acquire()
        # progress(idx, len(closed_orders_ids))
        nb_released_encumbrance_futures.append(
            asyncio.ensure_future(
                release_order_encumbrances(order_id, fiscal_year_id, sem)
            )
        )
    nb_released_encumbrances = await asyncio.gather(*nb_released_encumbrance_futures)
    # progress(len(closed_orders_ids), len(closed_orders_ids))

    print(f'  Released {sum(nb_released_encumbrances)} encumbrance(s).')


# ---------------------------------------------------
# All operations


async def all_operations(
    closed_orders_ids,
    open_orders_ids,
    open_and_closed_orders_ids,
    fiscal_year_id,
    fy_is_current,
):
    await remove_duplicate_encumbrances(open_and_closed_orders_ids, fiscal_year_id)
    await fix_poline_encumbrances_relations(
        open_orders_ids, fiscal_year_id, fy_is_current
    )
    if fy_is_current:
        await fix_encumbrance_order_status_for_closed_orders(
            closed_orders_ids, fiscal_year_id
        )
    await unrelease_open_orders_encumbrances_with_nonzero_amounts(
        fiscal_year_id, open_orders_ids
    )
    await release_open_orders_encumbrances_with_negative_amounts(
        fiscal_year_id, open_orders_ids
    )
    await release_cancelled_order_line_encumbrances(fiscal_year_id, open_orders_ids)
    await recalculate_budget_encumbered(open_and_closed_orders_ids, fiscal_year_id)
    await release_unreleased_encumbrances_for_closed_orders(
        closed_orders_ids, fiscal_year_id
    )


# ---------------------------------------------------
# Dry-run mode selection


def dryrun_mode_selection():
    global dryrun

    choice_i = 0
    while choice_i < 1 or choice_i > 2:
        print('1) Dry-run mode (read-only, will not apply fixes)')
        print('2) Normal mode (will apply fixes)')
        choice_s = input('Choose an option: ')
        try:
            choice_i = int(choice_s)
            if choice_i < 1 or choice_i > 2:
                print('Invalid option.')
        except ValueError:
            print('Invalid option.')
    if choice_i == 1:
        dryrun = True
        print("Dry-run mode enabled. Fixes *will not* actually be applied.")
    else:
        print("Normal mode. All fixes *will* be applied.")
    print()


# ---------------------------------------------------
# Menu and running operations


async def run_operation(choice, fiscal_year_code, tenant, username, password):
    global headers
    initial_time = time.time()
    headers = login(tenant, username, password)
    fiscal_year = get_fiscal_year(fiscal_year_code)
    fy_is_current = test_fiscal_year_current(fiscal_year)
    fiscal_year_id = fiscal_year['id']

    if choice == 1:
        closed_orders_ids = get_closed_orders_ids()
        open_orders_ids = get_open_orders_ids()
        open_and_closed_orders_ids = closed_orders_ids + open_orders_ids
        await all_operations(
            closed_orders_ids,
            open_orders_ids,
            open_and_closed_orders_ids,
            fiscal_year_id,
            fy_is_current,
        )
    elif choice == 2:
        closed_orders_ids = get_closed_orders_ids()
        open_orders_ids = get_open_orders_ids()
        open_and_closed_orders_ids = closed_orders_ids + open_orders_ids
        await remove_duplicate_encumbrances(open_and_closed_orders_ids, fiscal_year_id)
    elif choice == 3:
        open_orders_ids = get_open_orders_ids()
        await fix_poline_encumbrances_relations(
            open_orders_ids, fiscal_year_id, fy_is_current
        )
    elif choice == 4:
        if not fy_is_current:
            print(
                'Fiscal year is not current - fixing encumbrance order status is not needed.'
            )
        else:
            closed_orders_ids = get_closed_orders_ids()
            await fix_encumbrance_order_status_for_closed_orders(
                closed_orders_ids, fiscal_year_id
            )
    elif choice == 5:
        open_orders_ids = get_open_orders_ids()
        await unrelease_open_orders_encumbrances_with_nonzero_amounts(
            fiscal_year_id, open_orders_ids
        )
    elif choice == 6:
        open_orders_ids = get_open_orders_ids()
        await release_open_orders_encumbrances_with_negative_amounts(
            fiscal_year_id, open_orders_ids
        )
    elif choice == 7:
        open_orders_ids = get_open_orders_ids()
        await release_cancelled_order_line_encumbrances(fiscal_year_id, open_orders_ids)
    elif choice == 8:
        closed_orders_ids = get_closed_orders_ids()
        open_orders_ids = get_open_orders_ids()
        open_and_closed_orders_ids = closed_orders_ids + open_orders_ids
        await recalculate_budget_encumbered(open_and_closed_orders_ids, fiscal_year_id)
    elif choice == 9:
        closed_orders_ids = get_closed_orders_ids()
        await release_unreleased_encumbrances_for_closed_orders(
            closed_orders_ids, fiscal_year_id
        )
    delta = round(time.time() - initial_time)
    hours, remainder = divmod(delta, 3600)
    minutes, seconds = divmod(remainder, 60)
    print(
        f'Elapsed time: {hours} hour(s), {minutes} minute(s) and {seconds} second(s).'
    )


def menu(fiscal_year_code, tenant, username):
    print('1) Run all fixes (can be long)')
    print('2) Remove duplicate encumbrances')
    print('3) Fix order line - encumbrance relations')
    print(
        '4) Fix encumbrance order status for closed orders (current fiscal year only)'
    )
    print('5) Unrelease open order encumbrances with nonzero amounts')
    print('6) Release open order encumbrances with negative amounts')
    print('7) Release cancelled order line encumbrances')
    print(
        '8) Recalculate all budget encumbered amounts (avoid any transaction while this is running!)'
    )
    print('9) Release unreleased encumbrances for closed orders')
    print('10) Quit')
    choice_s = input('Choose an option: ')
    try:
        choice_i = int(choice_s)
    except ValueError:
        print('Invalid option.')
        return
    if choice_i < 1 or choice_i > 10:
        print('Invalid option.')
        return
    if choice_i == 10:
        return
    if choice_i == 1 and dryrun:
        print(
            "Note that, because dry-run mode is enabled, some operations will behave differently because they "
            "depend on the execution of previous ones, such as when recalculating the budget encumbrances."
        )
    password = getpass.getpass('Password:')
    asyncio.run(run_operation(choice_i, fiscal_year_code, tenant, username, password))


# ---------------------------------------------------
# Main


# def main():
#     global okapi_url
#     if len(sys.argv) != 5:
#         print(
#             "Syntax: ./fix_encumbrances.py 'fiscal_year_code' 'okapi_url' 'tenant' 'username'"
#         )
#         raise SystemExit(1)
#     fiscal_year_code = sys.argv[1]
#     okapi_url = sys.argv[2]
#     tenant = sys.argv[3]
#     username = sys.argv[4]
#     dryrun_mode_selection()
#     menu(fiscal_year_code, tenant, username)


# main()
